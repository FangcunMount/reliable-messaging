package mongo

import (
	"context"
	"fmt"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
)

var _ transaction.RecoveryReceiptRecorder = (*Store)(nil)

func (s *Store) RecordRecoveryFinal(ctx context.Context, ref transaction.Reference, outcome transaction.FinalOutcome) error {
	state, err := transaction.RecoveryFinalState(outcome)
	if err != nil {
		return err
	}
	if !ref.Valid() || ref.Owner != s.binding.Owner {
		return transaction.ErrBinding
	}
	// A short protocol transaction reads binding and exact evidence in one snapshot;
	// the host's original business session is never borrowed or re-executed here.
	_, err = s.protocolTx(ctx, func(sc driver.SessionContext) (transaction.Reservation, error) {
		if err := s.bindingIn(sc, false); err != nil {
			return transaction.Reservation{}, err
		}
		ek := transaction.ExecutionKey(ref.Owner, ref.OperationID, ref.WriterEpoch, ref.Fence)
		var d executionDoc
		if err := s.db.Collection("rm_tx_execution").FindOne(sc, bson.M{"_id": ek[:]}).Decode(&d); err != nil {
			return transaction.Reservation{}, err
		}
		if d.Owner != ref.Owner || d.Epoch != ref.WriterEpoch || d.Fence != int64(ref.Fence) || d.State != state {
			return transaction.Reservation{}, transaction.ErrStaleExecution
		}
		for i, branch := range d.Branches {
			actual, err := transaction.DecodeReference(branch.Reference)
			if err != nil {
				return transaction.Reservation{}, err
			}
			if actual != ref {
				continue
			}
			if branch.State != state || (branch.Final != "" && branch.Final != transaction.FinalNotAttempted && branch.Final != transaction.FinalUnknown && branch.Final != outcome) {
				return transaction.Reservation{}, transaction.ErrStaleExecution
			}
			if branch.Final == outcome {
				return transaction.Reservation{}, nil
			}
			prefix := fmt.Sprintf("witnesses.%d.", i)
			filter := bson.M{"_id": ek[:], "state": state, prefix + "reference": branch.Reference, prefix + "state": state}
			result, err := s.db.Collection("rm_tx_execution").UpdateOne(sc, filter, bson.M{"$set": bson.M{prefix + "final_outcome": outcome, prefix + "final_error_code": ""}, "$currentDate": bson.M{prefix + "final_recorded_at": true}})
			if err != nil {
				return transaction.Reservation{}, err
			}
			if result.MatchedCount != 1 {
				return transaction.Reservation{}, transaction.ErrStaleExecution
			}
			return transaction.Reservation{}, nil
		}
		return transaction.Reservation{}, transaction.ErrStaleExecution
	})
	return err
}
