// Package mysql stores transaction-message evidence separately from Outbox.
// The host installs Schema and the binding row explicitly and owns its pools.
package mysql

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"errors"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	driver "github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var Schema string

const SchemaVersion = 1
const clockLayout = "2006-01-02 15:04:05.000000"

type Store struct {
	db      *sql.DB
	binding transaction.Binding
}

var _ transaction.AuthorizedStore = (*Store)(nil)

func New(db *sql.DB, b transaction.Binding) (*Store, error) {
	if db == nil || !b.Valid() || len(b.Owner) > 128 || len(b.StoreID) > 128 || len(b.WriterEpoch) > 128 {
		return nil, transaction.ErrBinding
	}
	return &Store{db, b}, nil
}
func (s *Store) bindingIn(ctx context.Context, tx *sql.Tx, writer bool) error {
	var id, epoch string
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT store_id,writer_epoch,schema_version FROM rm_tx_binding WHERE owner=? FOR SHARE`, []byte(s.binding.Owner)).Scan(&id, &epoch, &version); err != nil {
		return err
	}
	if id != s.binding.StoreID || (writer && epoch != s.binding.WriterEpoch) {
		return transaction.ErrBinding
	}
	if version != SchemaVersion {
		return transaction.ErrSchema
	}
	return nil
}
func key(e transaction.Execution) [32]byte {
	return transaction.ExecutionKey(e.Plan.Input().Owner, e.Plan.Input().OperationID, e.WriterEpoch, e.Fence)
}
func scanExecution(row interface{ Scan(...any) error }) (transaction.Execution, transaction.State, error) {
	var e transaction.Execution
	var data, token []byte
	var epoch, lease string
	var state transaction.State
	err := row.Scan(&data, &e.Fence, &epoch, &token, &lease, &state)
	if err != nil {
		return e, state, err
	}
	e.Plan, err = transaction.DecodePlan(data)
	if err != nil {
		return e, state, err
	}
	e.WriterEpoch = epoch
	copy(e.Token[:], token)
	e.LeaseUntil, err = time.ParseInLocation(clockLayout, lease, time.UTC)
	if err != nil || len(token) != 32 || !e.Valid() {
		return e, state, transaction.ErrInvalidPlan
	}
	return e, state, nil
}

const selectExecution = `SELECT plan,fence,writer_epoch,token,DATE_FORMAT(lease_until,'%Y-%m-%d %H:%i:%s.%f'),state FROM rm_tx_execution WHERE execution_key=?`

func (s *Store) Lookup(ctx context.Context, r transaction.Request) (transaction.Reservation, error) {
	if !r.Valid() || r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return transaction.Reservation{}, err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, false); err != nil {
		return transaction.Reservation{}, err
	}
	result, err := s.lookupIn(ctx, tx, r, false)
	if errors.Is(err, sql.ErrNoRows) {
		err = transaction.ErrNotFound
	}
	return result, err
}
func (s *Store) lookupIn(ctx context.Context, tx *sql.Tx, r transaction.Request, locked bool) (transaction.Reservation, error) {
	rk := r.KeyDigest()
	query := `SELECT owner,scope,idempotency_key,input_hash,active_execution FROM rm_tx_request WHERE request_key=?`
	if locked {
		query += ` FOR UPDATE`
	}
	var owner, scope, k string
	var hash, active []byte
	if err := tx.QueryRowContext(ctx, query, rk[:]).Scan(&owner, &scope, &k, &hash, &active); err != nil {
		return transaction.Reservation{}, err
	}
	if owner != r.Owner || scope != r.Scope || k != r.Key {
		return transaction.Reservation{}, transaction.ErrRequestIdentity
	}
	if !bytes.Equal(hash, r.InputHash[:]) {
		return transaction.Reservation{}, transaction.ErrInputConflict
	}
	e, state, err := scanExecution(tx.QueryRowContext(ctx, selectExecution, active))
	if err != nil {
		return transaction.Reservation{}, err
	}
	if err = r.Match(e.Plan); err != nil {
		return transaction.Reservation{}, err
	}
	e.Token = [32]byte{} // Lookup/duplicates never grant the winner's capability.
	return transaction.Reservation{Execution: e, State: state}, nil
}
func (s *Store) Reserve(ctx context.Context, r transaction.Request, p transaction.FrozenPlan, o transaction.ReserveOptions) (transaction.Reservation, error) {
	if !o.Valid() || !p.Valid() || r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	if err := r.Match(p); err != nil {
		return transaction.Reservation{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return transaction.Reservation{}, err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, true); err != nil {
		return transaction.Reservation{}, err
	}
	e, err := s.newExecution(ctx, tx, p, 1, o)
	if err != nil {
		return transaction.Reservation{}, err
	}
	rk, ek := r.KeyDigest(), key(e)
	_, err = tx.ExecContext(ctx, `INSERT INTO rm_tx_request(request_key,owner,scope,idempotency_key,input_hash,active_execution) VALUES(?,?,?,?,?,?)`, rk[:], []byte(r.Owner), []byte(r.Scope), []byte(r.Key), r.InputHash[:], ek[:])
	if err != nil {
		var d *driver.MySQLError
		if errors.As(err, &d) && d.Number == 1062 {
			if err = tx.Rollback(); err != nil {
				return transaction.Reservation{}, err
			}
			return s.Lookup(ctx, r)
		}
		return transaction.Reservation{}, err
	}
	if err = s.insertExecution(ctx, tx, r, e); err != nil {
		return transaction.Reservation{}, err
	}
	if err = tx.Commit(); err != nil {
		return transaction.Reservation{}, err
	}
	return transaction.Reservation{Execution: e, Acquired: true, State: transaction.Prepared}, nil
}
func (s *Store) newExecution(ctx context.Context, tx *sql.Tx, p transaction.FrozenPlan, fence uint64, o transaction.ReserveOptions) (transaction.Execution, error) {
	e := transaction.Execution{Plan: p, Fence: fence, WriterEpoch: s.binding.WriterEpoch}
	if _, err := rand.Read(e.Token[:]); err != nil {
		return e, err
	}
	var now string
	if err := tx.QueryRowContext(ctx, `SELECT DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%d %H:%i:%s.%f')`).Scan(&now); err != nil {
		return e, err
	}
	t, err := time.ParseInLocation(clockLayout, now, time.UTC)
	if err != nil {
		return e, err
	}
	e.LeaseUntil = t.Add(o.Lease).Truncate(time.Microsecond)
	return e, nil
}
func (s *Store) insertExecution(ctx context.Context, tx *sql.Tx, r transaction.Request, e transaction.Execution) error {
	data, err := transaction.EncodePlan(e.Plan)
	if err != nil {
		return err
	}
	ek, rk := key(e), r.KeyDigest()
	in := e.Plan.Input()
	if _, err = tx.ExecContext(ctx, `INSERT INTO rm_tx_execution(execution_key,request_key,owner,operation_id,writer_epoch,fence,token,plan,state,lease_until) VALUES(?,?,?,?,?,?,?,?,?,?)`, ek[:], rk[:], []byte(in.Owner), []byte(in.OperationID), []byte(e.WriterEpoch), e.Fence, e.Token[:], data, transaction.Prepared, e.LeaseUntil.Format(clockLayout)); err != nil {
		return err
	}
	for _, ref := range e.References() {
		data, err := transaction.EncodeReference(ref)
		if err != nil {
			return err
		}
		bk := transaction.BranchKey(ref.BranchID)
		if _, err = tx.ExecContext(ctx, `INSERT INTO rm_tx_witness(execution_key,branch_key,reference,state) VALUES(?,?,?,?)`, ek[:], bk[:], data, transaction.Prepared); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Evidence(ctx context.Context, ref transaction.Reference) (*transaction.Evidence, error) {
	if !ref.Valid() || ref.Owner != s.binding.Owner {
		return nil, transaction.ErrBinding
	}
	ek, bk := transaction.ExecutionKey(ref.Owner, ref.OperationID, ref.WriterEpoch, ref.Fence), transaction.BranchKey(ref.BranchID)
	var raw []byte
	var e transaction.Evidence
	// One autocommit statement, primary host pool, no borrowed ORM snapshot.
	err := s.db.QueryRowContext(ctx, `SELECT w.reference,e.state,w.state FROM rm_tx_execution e JOIN rm_tx_witness w ON w.execution_key=e.execution_key JOIN rm_tx_binding b ON b.owner=e.owner WHERE e.execution_key=? AND w.branch_key=? AND b.store_id=? AND b.schema_version=?`, ek[:], bk[:], []byte(s.binding.StoreID), SchemaVersion).Scan(&raw, &e.ExecutionState, &e.BranchState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.Reference, err = transaction.DecodeReference(raw)
	return &e, err
}

func (s *Store) Abort(ctx context.Context, e transaction.Execution) (bool, error) {
	return s.abort(ctx, e, false)
}
func (s *Store) abort(ctx context.Context, e transaction.Execution, expired bool) (bool, error) {
	if !e.Valid() || e.Plan.Input().Owner != s.binding.Owner {
		return false, transaction.ErrBinding
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, false); err != nil {
		return false, err
	}
	ek := key(e)
	query := `UPDATE rm_tx_execution SET state='aborted',guard_version=guard_version+1 WHERE execution_key=? AND state='prepared' AND token=?`
	if expired {
		query += ` AND lease_until<=UTC_TIMESTAMP(6)`
	}
	res, err := tx.ExecContext(ctx, query, ek[:], e.Token[:])
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	res, err = tx.ExecContext(ctx, `UPDATE rm_tx_witness SET state='aborted' WHERE execution_key=? AND state='prepared'`, ek[:])
	if err != nil {
		return false, err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n != int64(len(e.Plan.Input().Branches)) {
		return false, transaction.ErrStaleExecution
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ReconcileOnce(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, transaction.ErrInvalidPlan
	}
	rows, err := s.db.QueryContext(ctx, `SELECT plan,fence,writer_epoch,token,DATE_FORMAT(lease_until,'%Y-%m-%d %H:%i:%s.%f'),state FROM rm_tx_execution WHERE owner=? AND state='prepared' AND lease_until<=UTC_TIMESTAMP(6) ORDER BY lease_until LIMIT ?`, []byte(s.binding.Owner), limit)
	if err != nil {
		return 0, err
	}
	var executions []transaction.Execution
	for rows.Next() {
		e, _, err := scanExecution(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		executions = append(executions, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range executions {
		ok, err := s.abort(ctx, e, true)
		if err != nil {
			return n, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}

func (s *Store) Retry(ctx context.Context, a transaction.RetryAuthorization, o transaction.ReserveOptions) (transaction.Reservation, error) {
	return s.advance(ctx, a.Previous, a.Previous.Plan, o, false)
}
func (s *Store) Replan(ctx context.Context, a transaction.ReplanAuthorization, o transaction.ReserveOptions) (transaction.Reservation, error) {
	return s.advance(ctx, a.Previous, a.Candidate, o, true)
}
func (s *Store) advance(ctx context.Context, previous transaction.Execution, p transaction.FrozenPlan, o transaction.ReserveOptions, replan bool) (transaction.Reservation, error) {
	if !previous.Plan.Valid() || !p.Valid() || !o.Valid() || previous.Fence == 0 || previous.Fence >= transaction.MaxFence {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	r := transaction.RequestFor(previous.Plan)
	if r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	if err := r.Match(p); err != nil {
		return transaction.Reservation{}, err
	}
	if replan && p.Input().OperationID == previous.Plan.Input().OperationID {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return transaction.Reservation{}, err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, true); err != nil {
		return transaction.Reservation{}, err
	}
	current, err := s.lookupIn(ctx, tx, r, true)
	if err != nil {
		return transaction.Reservation{}, err
	}
	if current.State != transaction.Aborted || key(current.Execution) != key(previous) || current.Execution.Plan.Digest() != previous.Plan.Digest() {
		return transaction.Reservation{}, transaction.ErrStaleExecution
	}
	var oldEvidence []*transaction.Evidence
	oldKey := key(previous)
	for _, ref := range previous.References() {
		bk := transaction.BranchKey(ref.BranchID)
		var raw []byte
		var state transaction.State
		if err = tx.QueryRowContext(ctx, `SELECT reference,state FROM rm_tx_witness WHERE execution_key=? AND branch_key=? FOR UPDATE`, oldKey[:], bk[:]).Scan(&raw, &state); err != nil {
			return transaction.Reservation{}, err
		}
		actual, err := transaction.DecodeReference(raw)
		if err != nil {
			return transaction.Reservation{}, err
		}
		oldEvidence = append(oldEvidence, &transaction.Evidence{Reference: actual, ExecutionState: current.State, BranchState: state})
	}
	if err = transaction.ValidateAdvance(previous, p, oldEvidence, replan); err != nil {
		return transaction.Reservation{}, err
	}
	e, err := s.newExecution(ctx, tx, p, previous.Fence+1, o)
	if err != nil {
		return transaction.Reservation{}, err
	}
	if err = s.insertExecution(ctx, tx, r, e); err != nil {
		return transaction.Reservation{}, err
	}
	rk, ek := r.KeyDigest(), key(e)
	if _, err = tx.ExecContext(ctx, `UPDATE rm_tx_request SET active_execution=? WHERE request_key=?`, ek[:], rk[:]); err != nil {
		return transaction.Reservation{}, err
	}
	if err = tx.Commit(); err != nil {
		return transaction.Reservation{}, err
	}
	return transaction.Reservation{Execution: e, Acquired: true, State: transaction.Prepared}, nil
}
