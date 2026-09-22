// Package mysql implements a provisional standard-schema Outbox.
// Legacy IAM/qs-server table mappings are separate M2/M4 integration work.
package mysql

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
)

// Schema is applied explicitly by the host. Constructors never execute DDL.
//
//go:embed schema.sql
var Schema string

type Appender struct{ tx *sql.Tx }

// Standard-schema scheduling columns store UTC clock digits. Passing time.Time
// to MySQL would let the host driver's loc transform them again. String values
// preserve this storage contract without changing the caller's transaction or
// session timezone (including business sessions using UTC+8).
const databaseTimeLayout = "2006-01-02 15:04:05.000000"

// Bind requires the existing host transaction; it never begins or commits one.
func Bind(tx *sql.Tx) (*Appender, error) {
	if tx == nil {
		return nil, errors.New("host SQL transaction required")
	}
	return &Appender{tx: tx}, nil
}

func (a *Appender) Append(ctx context.Context, m message.Message, due time.Time) error {
	if a == nil || a.tx == nil {
		return errors.New("host SQL transaction required")
	}
	if !m.Valid() || due.IsZero() {
		return errors.New("valid message and due time required")
	}
	in := m.Input()
	hash := m.Fingerprint()
	_, err := a.tx.ExecContext(ctx, `INSERT INTO rm_outbox
 (producer,message_id,destination,event_type,schema_version,scope,content_type,occurred_at,payload,fingerprint,next_attempt_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=id`, in.Producer, in.ID, in.Destination, in.EventType, in.SchemaVersion, in.Scope, in.ContentType, in.OccurredAt, in.Payload, hash[:], due.UTC().Format(databaseTimeLayout))
	if err != nil {
		return err
	}
	var existing []byte
	if err := a.tx.QueryRowContext(ctx, `SELECT fingerprint FROM rm_outbox WHERE producer=? AND message_id=? AND destination=? FOR UPDATE`, in.Producer, in.ID, in.Destination).Scan(&existing); err != nil {
		return err
	}
	if !bytes.Equal(existing, hash[:]) {
		return outbox.ErrConflict
	}
	return nil
}

type Store struct{ db *sql.DB }

var _ outbox.Store = (*Store)(nil)

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("host database required")
	}
	return &Store{db: db}, nil
}

func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]outbox.Claim, error) {
	if limit < 1 || limit > 1000 || lease < time.Microsecond || lease > 24*time.Hour {
		return nil, errors.New("limit must be 1..1000 and lease 1us..24h")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var nowText string
	if err := tx.QueryRowContext(ctx, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%d %H:%i:%s.%f')").Scan(&nowText); err != nil {
		return nil, err
	}
	now, err := time.Parse(databaseTimeLayout, nowText)
	if err != nil {
		return nil, fmt.Errorf("decode database UTC clock: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,producer,message_id,destination,event_type,schema_version,scope,content_type,occurred_at,payload,fingerprint,version,attempt_count FROM rm_outbox
 WHERE (state IN ('pending','retry_wait') AND next_attempt_at<=?) OR (state='publishing' AND lease_until<=?)
 ORDER BY next_attempt_at,id LIMIT ? FOR UPDATE SKIP LOCKED`, nowText, nowText, limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		claim       outbox.Claim
		input       message.Input
		fingerprint []byte
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		in := &c.input
		if err := rows.Scan(&c.claim.RecordID, &in.Producer, &in.ID, &in.Destination, &in.EventType, &in.SchemaVersion, &in.Scope, &in.ContentType, &in.OccurredAt, &in.Payload, &c.fingerprint, &c.claim.Version, &c.claim.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	claims := make([]outbox.Claim, 0, len(candidates))
	for _, c := range candidates {
		m, decodeErr := message.New(c.input)
		fingerprint := m.Fingerprint()
		if decodeErr != nil || !bytes.Equal(c.fingerprint, fingerprint[:]) {
			// This row is still locked. Corruption is preserved and made visible,
			// rather than starving every valid record behind it indefinitely.
			if _, err := tx.ExecContext(ctx, `UPDATE rm_outbox SET state='quarantined',last_error_code='invalid_immutable_content',claim_token=NULL,lease_until=NULL,version=version+1 WHERE id=? AND version=?`, c.claim.RecordID, c.claim.Version); err != nil {
				return nil, err
			}
			continue
		}
		var token [32]byte
		if _, err := rand.Read(token[:]); err != nil {
			return nil, err
		}
		c.claim.Token = hex.EncodeToString(token[:])
		c.claim.LeaseUntil = now.Add(lease).Truncate(time.Microsecond)
		c.claim.Message = m
		if _, err := tx.ExecContext(ctx, `UPDATE rm_outbox SET state='publishing',claim_token=?,lease_until=?,version=version+1,attempt_count=attempt_count+1 WHERE id=? AND version=?`, c.claim.Token, c.claim.LeaseUntil.Format(databaseTimeLayout), c.claim.RecordID, c.claim.Version); err != nil {
			return nil, err
		}
		c.claim.Version++
		c.claim.Attempts++
		claims = append(claims, c.claim)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claims, nil
}

func (s *Store) Confirm(ctx context.Context, c outbox.Claim) error {
	return s.update(ctx, c, `state='published',transport_confirmed_at=UTC_TIMESTAMP(6),last_error_code=''`)
}
func (s *Store) Retry(ctx context.Context, c outbox.Claim, delay time.Duration, code string) error {
	if delay <= 0 || code == "" || len(code) > 128 {
		return errors.New("retry requires positive delay and bounded error code")
	}
	micros := delay / time.Microsecond
	if delay%time.Microsecond != 0 {
		micros++
	}
	return s.update(ctx, c, `state='retry_wait',next_attempt_at=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)),last_error_code=?`, int64(micros), code)
}
func (s *Store) Quarantine(ctx context.Context, c outbox.Claim, code string) error {
	if code == "" || len(code) > 128 {
		return errors.New("bounded quarantine reason required")
	}
	return s.update(ctx, c, `state='quarantined',last_error_code=?`, code)
}
func (s *Store) update(ctx context.Context, c outbox.Claim, set string, args ...any) error {
	id, parseErr := strconv.ParseUint(c.RecordID, 10, 64)
	if parseErr != nil || id == 0 || strconv.FormatUint(id, 10) != c.RecordID || c.Token == "" || c.Version == 0 {
		return outbox.ErrStaleClaim
	}
	args = append(args, id, c.Token, c.Version)
	result, err := s.db.ExecContext(ctx, `UPDATE rm_outbox SET `+set+`,claim_token=NULL,lease_until=NULL,version=version+1
 WHERE id=? AND claim_token=? AND version=? AND state='publishing' AND lease_until>UTC_TIMESTAMP(6)`, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: affected %d rows", outbox.ErrStaleClaim, n)
	}
	return nil
}
