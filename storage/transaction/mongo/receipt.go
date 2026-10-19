package mongo

import (
	"context"
	"fmt"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
)

var _ transaction.PreparedStore = (*Store)(nil)

func (s *Store) Binding() transaction.Binding { return s.binding }
func (s *Store) Validate(ctx context.Context) error {
	if err := s.ValidateSchema(ctx); err != nil {
		return err
	}
	return s.bindingIn(ctx, true)
}
func (s *Store) receiptFilter(e transaction.Execution, ref transaction.Reference) (bson.M, string, error) {
	if !transaction.MatchExecution(e, ref) || ref.Owner != s.binding.Owner {
		return nil, "", transaction.ErrBinding
	}
	ek, digest := key(e), e.Plan.Digest()
	raw, err := transaction.EncodeReference(ref)
	if err != nil {
		return nil, "", err
	}
	for i, r := range e.References() {
		if r == ref {
			prefix := fmt.Sprintf("witnesses.%d.", i)
			return bson.M{"_id": ek[:], "token": e.Token[:], "plan_digest": digest[:], prefix + "reference": raw}, prefix, nil
		}
	}
	return nil, "", transaction.ErrInvalidPlan
}
func (s *Store) RecordHalf(ctx context.Context, e transaction.Execution, ref transaction.Reference, h transaction.HalfReceipt) error {
	if !h.Valid() {
		return transaction.ErrInvalidPlan
	}
	if err := s.bindingIn(ctx, false); err != nil {
		return err
	}
	filter, prefix, err := s.receiptFilter(e, ref)
	if err != nil {
		return err
	}
	filter["state"] = transaction.Prepared
	filter[prefix+"state"] = transaction.Prepared
	res, err := s.db.Collection("rm_tx_execution").UpdateOne(ctx, filter, bson.M{"$set": bson.M{prefix + "broker_message_id": h.BrokerMessageID, prefix + "broker_transaction_id": h.BrokerTransactionID, prefix + "half_outcome": h.Outcome, prefix + "half_error_code": h.ErrorCode}, "$currentDate": bson.M{prefix + "half_recorded_at": true}})
	if err != nil {
		return err
	}
	if res.MatchedCount != 1 {
		return transaction.ErrStaleExecution
	}
	return nil
}
func (s *Store) RecordFinal(ctx context.Context, e transaction.Execution, ref transaction.Reference, f transaction.FinalOutcome, code string) error {
	if !transaction.ValidFinal(f, code) {
		return transaction.ErrInvalidPlan
	}
	if err := s.bindingIn(ctx, false); err != nil {
		return err
	}
	filter, prefix, err := s.receiptFilter(e, ref)
	if err != nil {
		return err
	}
	switch f {
	case transaction.FinalCommitConfirmed:
		filter["state"] = transaction.Committed
		filter[prefix+"state"] = transaction.Committed
	case transaction.FinalRollbackConfirmed:
		filter["state"] = transaction.Aborted
		filter[prefix+"state"] = transaction.Aborted
	default:
		filter["$or"] = bson.A{bson.M{"state": transaction.Committed, prefix + "state": transaction.Committed}, bson.M{"state": transaction.Aborted, prefix + "state": transaction.Aborted}}
	}
	res, err := s.db.Collection("rm_tx_execution").UpdateOne(ctx, filter, bson.M{"$set": bson.M{prefix + "final_outcome": f, prefix + "final_error_code": code}, "$currentDate": bson.M{prefix + "final_recorded_at": true}})
	if err != nil {
		return err
	}
	if res.MatchedCount != 1 {
		return transaction.ErrStaleExecution
	}
	return nil
}
