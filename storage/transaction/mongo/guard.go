package mongo

import (
	"context"
	"errors"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Guard struct {
	store     *Store
	ctx       driver.SessionContext
	execution transaction.Execution
	version   int64
	finished  bool
}

func active(ctx driver.SessionContext, client *driver.Client) bool {
	if ctx == nil {
		return false
	}
	session := driver.SessionFromContext(ctx)
	if session == nil || session.Client() != client {
		return false
	}
	s, ok := session.(driver.XSession) //nolint:staticcheck // Pinned v1.17.6 active-transaction bridge, same as existing adapter.
	return ok && s.ClientSession() != nil && s.ClientSession().TransactionRunning()
}
func (s *Store) Bind(ctx driver.SessionContext, e transaction.Execution) (*Guard, error) {
	if !active(ctx, s.db.Client()) {
		return nil, transaction.ErrTransactionRequired
	}
	if !e.Valid() || e.Plan.Input().Owner != s.binding.Owner || e.WriterEpoch != s.binding.WriterEpoch {
		return nil, transaction.ErrBinding
	}
	if err := s.bindingIn(ctx, true); err != nil {
		return nil, err
	}
	ek, digest := key(e), e.Plan.Digest()
	var d executionDoc
	err := s.db.Collection("rm_tx_execution").FindOneAndUpdate(ctx, bson.M{"_id": ek[:], "state": transaction.Prepared, "token": e.Token[:], "plan_digest": digest[:], "$expr": bson.M{"$gt": bson.A{"$lease_until", "$$NOW"}}}, bson.M{"$inc": bson.M{"guard_version": 1}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&d)
	if errors.Is(err, driver.ErrNoDocuments) {
		return nil, transaction.ErrStaleExecution
	}
	if err != nil {
		return nil, err
	}
	stored, err := transaction.DecodePlan(d.Plan)
	if err != nil {
		return nil, err
	}
	if stored.Digest() != e.Plan.Digest() {
		return nil, transaction.ErrStaleExecution
	}
	if len(d.Branches) != len(e.References()) {
		return nil, transaction.ErrStaleExecution
	}
	for i, ref := range e.References() {
		actual, err := transaction.DecodeReference(d.Branches[i].Reference)
		if err != nil || actual != ref || d.Branches[i].State != transaction.Prepared {
			return nil, transaction.ErrStaleExecution
		}
	}
	return &Guard{store: s, ctx: ctx, execution: e, version: d.Version}, nil
}
func (g *Guard) CommitWitness(ctx context.Context) error {
	if g == nil || g.finished || !active(g.ctx, g.store.db.Client()) || driver.SessionFromContext(ctx) != driver.SessionFromContext(g.ctx) {
		return transaction.ErrTransactionRequired
	}
	ek := key(g.execution)
	res, err := g.store.db.Collection("rm_tx_execution").UpdateOne(g.ctx, bson.M{"_id": ek[:], "state": transaction.Prepared, "token": g.execution.Token[:], "guard_version": g.version}, bson.M{"$set": bson.M{"state": transaction.Committed, "witnesses.$[].state": transaction.Committed}})
	if err != nil {
		return err
	}
	if res.ModifiedCount != 1 {
		return transaction.ErrStaleExecution
	}
	g.finished = true
	return nil
}
