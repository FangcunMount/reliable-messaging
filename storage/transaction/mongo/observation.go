package mongo

import (
	"context"
	"time"

	"github.com/FangcunMount/reliable-messaging/internal/transactionobservation"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ transaction.ObservationReader = (*Store)(nil)

// ObserveProtocol uses an owned, read-only snapshot session on the borrowed pool.
// Projection excludes frozen plans, references and wire; cursor memory is bounded.
// No transaction, callback retry, schema install or business mutation occurs.
func (s *Store) ObserveProtocol(ctx context.Context) (transaction.ProtocolObservation, error) {
	ctx, cancel := transactionobservation.Budget(ctx)
	defer cancel()
	session, err := s.db.Client().StartSession(options.Session().SetSnapshot(true))
	if err != nil {
		return transaction.ProtocolObservation{}, err
	}
	defer session.EndSession(ctx)
	sc := driver.NewSessionContext(ctx, session)
	var a transactionobservation.Accumulator
	cursor, err := s.db.Collection("rm_tx_binding").Aggregate(sc, driver.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{"_id": s.binding.Owner}}},
		bson.D{{Key: "$project", Value: bson.M{"store_id": 1, "writer_epoch": 1, "schema_version": 1, "now": "$$NOW"}}},
	})
	if err != nil {
		return transaction.ProtocolObservation{}, err
	}
	var binding struct {
		Store   string    `bson:"store_id"`
		Epoch   string    `bson:"writer_epoch"`
		Version int       `bson:"schema_version"`
		Now     time.Time `bson:"now"`
	}
	if !cursor.Next(sc) {
		err = cursor.Err()
		_ = cursor.Close(sc)
		if err == nil {
			err = transaction.ErrBinding
		}
		return transaction.ProtocolObservation{}, err
	}
	err = cursor.Decode(&binding)
	_ = cursor.Close(sc)
	if err != nil {
		return transaction.ProtocolObservation{}, err
	}
	if binding.Store != s.binding.StoreID || binding.Epoch != s.binding.WriterEpoch {
		return transaction.ProtocolObservation{}, transaction.ErrBinding
	}
	if binding.Version != SchemaVersion {
		return transaction.ProtocolObservation{}, transaction.ErrSchema
	}
	if binding.Now.IsZero() {
		return transaction.ProtocolObservation{}, transactionobservation.ErrMetadata
	}
	a.Snapshot.AsOf = binding.Now
	cursor, err = s.db.Collection("rm_tx_execution").Find(sc, bson.M{"owner": s.binding.Owner}, options.Find().SetProjection(bson.M{
		"_id": 0, "state": 1, "prepare_started": 1, "lease_until": 1, "witnesses.state": 1,
		"witnesses.half_outcome": 1, "witnesses.final_outcome": 1, "witnesses.half_recorded_at": 1, "witnesses.final_recorded_at": 1,
	}).SetBatchSize(32))
	if err != nil {
		return transaction.ProtocolObservation{}, err
	}
	defer cursor.Close(sc)
	for cursor.Next(sc) {
		var row struct {
			State    transaction.State `bson:"state"`
			Started  bool              `bson:"prepare_started"`
			Lease    time.Time         `bson:"lease_until"`
			Branches []branchDoc       `bson:"witnesses"`
		}
		if err = cursor.Decode(&row); err != nil {
			return transaction.ProtocolObservation{}, err
		}
		if err = a.Execution(row.State, row.Started, row.Lease); err != nil {
			return transaction.ProtocolObservation{}, err
		}
		if len(row.Branches) == 0 {
			a.Snapshot.MetadataConflicts++
		}
		for _, b := range row.Branches {
			if err = a.Branch(row.State, row.Started, transactionobservation.Branch{State: b.State, Half: b.Half, Final: b.Final, HalfAt: b.HalfAt, FinalAt: b.FinalAt}); err != nil {
				return transaction.ProtocolObservation{}, err
			}
		}
	}
	if err = cursor.Err(); err != nil {
		return transaction.ProtocolObservation{}, err
	}
	return a.Snapshot, nil
}
