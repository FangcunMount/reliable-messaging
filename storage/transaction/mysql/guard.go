package mysql

import (
	"context"
	"database/sql"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"gorm.io/gorm"
)

type Guard struct {
	tx        *sql.Tx
	execution transaction.Execution
	committed bool
}

// Bind must be the FIRST protocol operation in the host root transaction. The
// real conditional write fences the aborter until the host commits/rolls back.
func (s *Store) Bind(ctx context.Context, tx *sql.Tx, e transaction.Execution) (*Guard, error) {
	if tx == nil {
		return nil, transaction.ErrTransactionRequired
	}
	if !e.Valid() || e.Plan.Input().Owner != s.binding.Owner || e.WriterEpoch != s.binding.WriterEpoch {
		return nil, transaction.ErrBinding
	}
	if err := s.bindingIn(ctx, tx, true); err != nil {
		return nil, err
	}
	ek := key(e)
	res, err := tx.ExecContext(ctx, `UPDATE rm_tx_execution SET guard_version=guard_version+1 WHERE execution_key=? AND token=? AND state='prepared' AND lease_until>UTC_TIMESTAMP(6)`, ek[:], e.Token[:])
	if err != nil {
		return nil, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, transaction.ErrStaleExecution
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, `SELECT plan FROM rm_tx_execution WHERE execution_key=?`, ek[:]).Scan(&raw); err != nil {
		return nil, err
	}
	stored, err := transaction.DecodePlan(raw)
	if err != nil {
		return nil, err
	}
	if stored.Digest() != e.Plan.Digest() {
		return nil, transaction.ErrStaleExecution
	}
	for _, ref := range e.References() {
		bk := transaction.BranchKey(ref.BranchID)
		var state transaction.State
		if err = tx.QueryRowContext(ctx, `SELECT reference,state FROM rm_tx_witness WHERE execution_key=? AND branch_key=? FOR UPDATE`, ek[:], bk[:]).Scan(&raw, &state); err != nil {
			return nil, err
		}
		actual, decodeErr := transaction.DecodeReference(raw)
		if decodeErr != nil || actual != ref || state != transaction.Prepared {
			return nil, transaction.ErrStaleExecution
		}
	}
	return &Guard{tx: tx, execution: e}, nil
}
func (s *Store) BindGORM(ctx context.Context, tx *gorm.DB, e transaction.Execution) (*Guard, error) {
	if tx == nil || tx.Error != nil || tx.Statement == nil {
		return nil, transaction.ErrTransactionRequired
	}
	pool := tx.Statement.ConnPool
	if prepared, ok := pool.(*gorm.PreparedStmtTX); ok {
		pool = prepared.Tx
	}
	raw, ok := pool.(*sql.Tx)
	if !ok || raw == nil {
		return nil, transaction.ErrTransactionRequired
	}
	return s.Bind(ctx, raw, e)
}

// CommitWitness stages all witnesses and the execution terminal state in the
// borrowed root transaction. It never calls Commit/Rollback. Host errors require
// rollback of the WHOLE transaction; savepoint release is not root commitment.
func (g *Guard) CommitWitness(ctx context.Context) error {
	if g == nil || g.tx == nil || g.committed {
		return transaction.ErrTransactionRequired
	}
	ek := key(g.execution)
	res, err := g.tx.ExecContext(ctx, `UPDATE rm_tx_witness SET state='committed' WHERE execution_key=? AND state='prepared'`, ek[:])
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != int64(len(g.execution.Plan.Input().Branches)) {
		return transaction.ErrStaleExecution
	}
	res, err = g.tx.ExecContext(ctx, `UPDATE rm_tx_execution SET state='committed' WHERE execution_key=? AND token=? AND state='prepared'`, ek[:], g.execution.Token[:])
	if err != nil {
		return err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return transaction.ErrStaleExecution
	}
	g.committed = true
	return nil
}
