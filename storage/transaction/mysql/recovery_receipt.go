package mysql

import (
	"context"
	"database/sql"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

var _ transaction.RecoveryReceiptRecorder = (*Store)(nil)

// RecordRecoveryFinal is protocol audit, not business commitment authority.
// It deliberately does not load or reveal the original execution capability.
func (s *Store) RecordRecoveryFinal(ctx context.Context, ref transaction.Reference, outcome transaction.FinalOutcome) error {
	state, err := transaction.RecoveryFinalState(outcome)
	if err != nil {
		return err
	}
	if !ref.Valid() || ref.Owner != s.binding.Owner {
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
	ek, bk := transaction.ExecutionKey(ref.Owner, ref.OperationID, ref.WriterEpoch, ref.Fence), transaction.BranchKey(ref.BranchID)
	var executionState transaction.State
	var owner, operation, epoch []byte
	var fence uint64
	if err = tx.QueryRowContext(ctx, `SELECT owner,operation_id,writer_epoch,fence,state FROM rm_tx_execution WHERE execution_key=? FOR UPDATE`, ek[:]).Scan(&owner, &operation, &epoch, &fence, &executionState); err != nil {
		return err
	}
	if string(owner) != ref.Owner || string(operation) != ref.OperationID || string(epoch) != ref.WriterEpoch || fence != ref.Fence || executionState != state {
		return transaction.ErrStaleExecution
	}
	var raw []byte
	var branchState transaction.State
	var previous transaction.FinalOutcome
	if err = tx.QueryRowContext(ctx, `SELECT reference,state,final_outcome FROM rm_tx_witness WHERE execution_key=? AND branch_key=? FOR UPDATE`, ek[:], bk[:]).Scan(&raw, &branchState, &previous); err != nil {
		return err
	}
	actual, err := transaction.DecodeReference(raw)
	if err != nil {
		return err
	}
	if actual != ref || branchState != state || (previous != transaction.FinalNotAttempted && previous != transaction.FinalUnknown && previous != outcome) {
		return transaction.ErrStaleExecution
	}
	if previous == outcome {
		return tx.Commit()
	} // Preserve the first positive audit time.
	if _, err = tx.ExecContext(ctx, `UPDATE rm_tx_witness SET final_outcome=?,final_error_code='',final_recorded_at=UTC_TIMESTAMP(6) WHERE execution_key=? AND branch_key=?`, outcome, ek[:], bk[:]); err != nil {
		return err
	}
	return tx.Commit()
}
