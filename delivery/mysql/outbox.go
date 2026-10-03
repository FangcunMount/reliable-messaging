// Package mysql implements distinct broker-published and business-confirmed states.
// It borrows original host transactions and never owns a pool, schema or scheduler.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
)

var ErrConflict = errors.New("original message identity or body mismatch")
var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

type Identity struct{ Producer, Destination, MessageID string }
type Record struct {
	Identity
	BodySHA256               string
	Body, Wire               []byte
	Topic, AggregateKey      string
	AggregateSequence        uint64
	Ordered, RequiresReceipt bool
	Stage                    string
	// Receipt-required records count unconfirmed delivery attempts. Final ACKs
	// count failed/uncertain PUB only; successful duplicate ACK PUB spends none.
	Attempts uint64
}
type Outbox struct{ table string }

// New validates an explicit host table name without touching storage.
func New(table string) (*Outbox, error) {
	if !identifier.MatchString(table) {
		return nil, errors.New("single explicit SQL table identifier required")
	}
	return &Outbox{table: "`" + table + "`"}, nil
}
func valid(tx *sql.Tx, id Identity) error {
	if tx == nil || id.Producer == "" || id.Destination == "" || id.MessageID == "" {
		return ErrConflict
	}
	return nil
}
func (o *Outbox) locked(ctx context.Context, tx *sql.Tx, id Identity, hash string) (Record, error) {
	var r Record
	if err := valid(tx, id); err != nil {
		return r, err
	}
	r.Identity = id
	err := tx.QueryRowContext(ctx, "SELECT body_sha256,requires_receipt,stage FROM "+o.table+" WHERE producer=? AND destination=? AND message_id=? FOR UPDATE", id.Producer, id.Destination, id.MessageID).Scan(&r.BodySHA256, &r.RequiresReceipt, &r.Stage)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrConflict
	}
	if err != nil {
		return r, err
	}
	if r.BodySHA256 != hash {
		return r, ErrConflict
	}
	return r, nil
}

// Pending enforces source aggregate order without waiting for model completion.
func (o *Outbox) Pending(ctx context.Context, tx *sql.Tx, limit int) ([]Record, error) {
	if tx == nil || limit < 1 || limit > 100 {
		return nil, errors.New("original transaction and bounded batch required")
	}
	query := fmt.Sprintf(`SELECT n.producer,n.destination,n.message_id,n.body_sha256,n.body,n.wire,n.topic,n.aggregate_key,n.aggregate_sequence,n.ordered,n.requires_receipt,n.stage,n.attempts
 FROM %s n WHERE n.stage IN ('staged','awaiting_receipt') AND n.available_at<=UTC_TIMESTAMP(6)
 AND (n.ordered=0 OR NOT EXISTS (SELECT 1 FROM %s older WHERE older.producer=n.producer AND older.destination=n.destination AND older.aggregate_key=n.aggregate_key AND older.ordered=1 AND older.stage<>'confirmed' AND older.aggregate_sequence<n.aggregate_sequence))
 ORDER BY n.available_at,n.message_id LIMIT ?`, o.table, o.table)
	rows, err := tx.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Record{}
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Producer, &r.Destination, &r.MessageID, &r.BodySHA256, &r.Body, &r.Wire, &r.Topic, &r.AggregateKey, &r.AggregateSequence, &r.Ordered, &r.RequiresReceipt, &r.Stage, &r.Attempts); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// Published never confirms a message that requires the receiver's durable receipt.
// A receipt-free final ACK ends at PUB OK: successful replays do not consume
// its persistent failure budget. Retry still counts every uncertain/failed PUB.
func (o *Outbox) Published(ctx context.Context, tx *sql.Tx, id Identity, hash string, waitSeconds int) error {
	if waitSeconds < 1 || waitSeconds > 60 {
		return errors.New("receipt wait must be 1..60 seconds")
	}
	r, err := o.locked(ctx, tx, id, hash)
	if err != nil {
		return err
	}
	if r.Stage == "confirmed" || r.Stage == "held" {
		return nil
	}
	stage := "awaiting_receipt"
	if !r.RequiresReceipt {
		stage = "confirmed"
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+o.table+" SET stage=?,attempts=IF(requires_receipt=1,attempts+1,attempts),published_at=UTC_TIMESTAMP(6),confirmed_at=IF(requires_receipt=0,UTC_TIMESTAMP(6),NULL),available_at=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)),error_code='' WHERE producer=? AND destination=? AND message_id=?", stage, waitSeconds, id.Producer, id.Destination, id.MessageID)
	return err
}

// Confirm requires prior host authentication of the exact original business receipt.
func (o *Outbox) Confirm(ctx context.Context, tx *sql.Tx, id Identity, hash string) error {
	r, err := o.locked(ctx, tx, id, hash)
	if err != nil {
		return err
	}
	if !r.RequiresReceipt {
		return ErrConflict
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+o.table+" SET stage='confirmed',confirmed_at=UTC_TIMESTAMP(6),error_code='' WHERE producer=? AND destination=? AND message_id=?", id.Producer, id.Destination, id.MessageID)
	return err
}

func (o *Outbox) Retry(ctx context.Context, tx *sql.Tx, id Identity, hash string, delaySeconds int, code string) error {
	if delaySeconds < 1 || delaySeconds > 60 {
		return errors.New("retry delay must be 1..60 seconds")
	}
	r, err := o.locked(ctx, tx, id, hash)
	if err != nil {
		return err
	}
	if r.Stage == "confirmed" || r.Stage == "held" {
		return nil
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+o.table+" SET attempts=attempts+1,available_at=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)),error_code=? WHERE producer=? AND destination=? AND message_id=?", delaySeconds, code, id.Producer, id.Destination, id.MessageID)
	return err
}
func (o *Outbox) Hold(ctx context.Context, tx *sql.Tx, id Identity, hash, code string) error {
	r, err := o.locked(ctx, tx, id, hash)
	if err != nil {
		return err
	}
	if r.Stage == "confirmed" {
		return nil
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+o.table+" SET stage='held',error_code=? WHERE producer=? AND destination=? AND message_id=?", code, id.Producer, id.Destination, id.MessageID)
	return err
}

// RearmAck never rearms business messages or grants permission to repeat execution.
func (o *Outbox) RearmAck(ctx context.Context, tx *sql.Tx, id Identity, hash string) error {
	r, err := o.locked(ctx, tx, id, hash)
	if err != nil {
		return err
	}
	if r.RequiresReceipt {
		return ErrConflict
	}
	if r.Stage == "held" {
		return nil // committed duplicates cannot revoke a persistent technical hold
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+o.table+" SET stage='staged',available_at=UTC_TIMESTAMP(6),confirmed_at=NULL,error_code='' WHERE producer=? AND destination=? AND message_id=?", id.Producer, id.Destination, id.MessageID)
	return err
}
