package mysql

import (
	"context"
	"database/sql"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

var _ transaction.PreparedStore = (*Store)(nil)

func (s *Store) Binding() transaction.Binding { return s.binding }
func (s *Store) Validate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, true); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.request_key,e.execution_key,w.reference,w.broker_message_id,w.half_outcome,w.final_outcome FROM rm_tx_request r,rm_tx_execution e,rm_tx_witness w WHERE FALSE`)
	if err != nil {
		return err
	}
	return rows.Close()
}
func (s *Store) receiptTx(ctx context.Context, e transaction.Execution, ref transaction.Reference, fn func(*sql.Tx, []byte, []byte, transaction.State) error) error {
	if !transaction.MatchExecution(e, ref) || ref.Owner != s.binding.Owner {
		return transaction.ErrBinding
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.bindingIn(ctx, tx, false); err != nil {
		return err
	}
	ek, bk := key(e), transaction.BranchKey(ref.BranchID)
	var state transaction.State
	var token, raw []byte
	if err = tx.QueryRowContext(ctx, `SELECT state,token FROM rm_tx_execution WHERE execution_key=? FOR UPDATE`, ek[:]).Scan(&state, &token); err != nil {
		return err
	}
	if string(token) != string(e.Token[:]) {
		return transaction.ErrStaleExecution
	}
	var branchState transaction.State
	if err = tx.QueryRowContext(ctx, `SELECT reference,state FROM rm_tx_witness WHERE execution_key=? AND branch_key=? FOR UPDATE`, ek[:], bk[:]).Scan(&raw, &branchState); err != nil {
		return err
	}
	actual, err := transaction.DecodeReference(raw)
	if err != nil {
		return err
	}
	if actual != ref || branchState != state {
		return transaction.ErrStaleExecution
	}
	if err = fn(tx, ek[:], bk[:], state); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) RecordHalf(ctx context.Context, e transaction.Execution, ref transaction.Reference, h transaction.HalfReceipt) error {
	if !h.Valid() {
		return transaction.ErrInvalidPlan
	}
	return s.receiptTx(ctx, e, ref, func(tx *sql.Tx, ek, bk []byte, state transaction.State) error {
		if state != transaction.Prepared {
			return transaction.ErrStaleExecution
		}
		var message, brokerTx []byte
		var previous transaction.HalfOutcome
		if err := tx.QueryRowContext(ctx, `SELECT broker_message_id,broker_transaction_id,half_outcome FROM rm_tx_witness WHERE execution_key=? AND branch_key=?`, ek, bk).Scan(&message, &brokerTx, &previous); err != nil {
			return err
		}
		if (len(message) > 0 && string(message) != h.BrokerMessageID) || (len(brokerTx) > 0 && string(brokerTx) != h.BrokerTransactionID) || (previous != transaction.HalfNotAttempted && previous != transaction.HalfUnknown && previous != h.Outcome) {
			return transaction.ErrStaleExecution
		}
		_, err := tx.ExecContext(ctx, `UPDATE rm_tx_witness SET half_outcome=?,broker_message_id=?,broker_transaction_id=?,half_error_code=?,half_recorded_at=UTC_TIMESTAMP(6) WHERE execution_key=? AND branch_key=?`, h.Outcome, []byte(h.BrokerMessageID), []byte(h.BrokerTransactionID), []byte(h.ErrorCode), ek, bk)
		return err
	})
}
func (s *Store) RecordFinal(ctx context.Context, e transaction.Execution, ref transaction.Reference, f transaction.FinalOutcome, code string) error {
	if !transaction.ValidFinal(f, code) {
		return transaction.ErrInvalidPlan
	}
	return s.receiptTx(ctx, e, ref, func(tx *sql.Tx, ek, bk []byte, state transaction.State) error {
		if state != transaction.Committed && state != transaction.Aborted {
			return transaction.ErrStaleExecution
		}
		if f == transaction.FinalCommitConfirmed && state != transaction.Committed || f == transaction.FinalRollbackConfirmed && state != transaction.Aborted {
			return transaction.ErrStaleExecution
		}
		var previous transaction.FinalOutcome
		if err := tx.QueryRowContext(ctx, `SELECT final_outcome FROM rm_tx_witness WHERE execution_key=? AND branch_key=?`, ek, bk).Scan(&previous); err != nil {
			return err
		}
		if previous != transaction.FinalNotAttempted && previous != transaction.FinalUnknown && previous != f {
			return transaction.ErrStaleExecution
		}
		_, err := tx.ExecContext(ctx, `UPDATE rm_tx_witness SET final_outcome=?,final_error_code=?,final_recorded_at=UTC_TIMESTAMP(6) WHERE execution_key=? AND branch_key=?`, f, []byte(code), ek, bk)
		return err
	})
}
