//go:build integration

package mongo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func fixture(t *testing.T) (*driver.Database, *Store) {
	t.Helper()
	uri := os.Getenv("RM_TX_MONGO_URI")
	if uri == "" || os.Getenv("RM_TX_DISPOSABLE") == "" {
		t.Fatal("required owned disposable Mongo replica set missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri).SetServerSelectionTimeout(2*time.Second).SetTimeout(3*time.Second).SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
	db := client.Database("rm_sdk_tx_owned")
	if err = InstallSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = db.CreateCollection(ctx, "tx_business"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Collection("rm_tx_binding").InsertOne(ctx, bson.M{"_id": "test.mongo", "store_id": "disposable-store", "writer_epoch": "epoch", "schema_version": SchemaVersion}); err != nil {
		t.Fatal(err)
	}
	s, err := New(db, transaction.Binding{Owner: "test.mongo", StoreID: "disposable-store", WriterEpoch: "epoch"})
	if err != nil {
		t.Fatal(err)
	}
	return db, s
}
func plan(t *testing.T, id, key string) transaction.FrozenPlan {
	t.Helper()
	m, err := message.New(message.Input{Producer: "fixture", ID: id, Destination: "logical", EventType: "created", SchemaVersion: "1", Scope: "scope", ContentType: "application/octet-stream", OccurredAt: "2026-10-19T00:00:00.000000123Z", Payload: []byte{0, 255, 1}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := transaction.NewPlan(transaction.PlanInput{Owner: "test.mongo", Scope: "scope", OperationID: "operation-" + id, IdempotencyKey: key, InputHash: sha256.Sum256([]byte("same business input")), Branches: []transaction.BranchInput{{ID: "branch", PhysicalTopic: "physical", RouteRevision: "v2", Message: m, Wire: []byte{0, 255, 5}}}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func reserve(t *testing.T, s *Store, id string) transaction.Execution {
	t.Helper()
	p := plan(t, id, id)
	r, err := s.Reserve(context.Background(), transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute})
	if err != nil || !r.Acquired {
		t.Fatal("reserve failed", err)
	}
	return r.Execution
}
func assertDecision(t *testing.T, s *Store, e transaction.Execution, want transaction.Decision) {
	t.Helper()
	for _, ref := range e.References() {
		if got := (transaction.StoreChecker{Store: s}).Check(context.Background(), ref); got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	}
}
func local(t *testing.T, db *driver.Database, s *Store, e transaction.Execution, id string, abort bool) {
	t.Helper()
	ctx := context.Background()
	session, err := db.Client().StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(ctx)
	err = session.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	sc := driver.NewSessionContext(ctx, session)
	guard, err := s.Bind(sc, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Collection("tx_business").InsertOne(sc, bson.M{"_id": id}); err != nil {
		t.Fatal(err)
	}
	if err = guard.CommitWitness(sc); err != nil {
		t.Fatal(err)
	}
	assertDecision(t, s, e, transaction.Unknown)
	if abort {
		err = session.AbortTransaction(sc)
	} else {
		err = session.CommitTransaction(sc)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Bind(sc, e); !errors.Is(err, transaction.ErrTransactionRequired) {
		t.Fatal("ended transaction bound", err)
	}
}

func TestOriginalMongoTransactionProofs(t *testing.T) {
	db, s := fixture(t)
	ctx := context.Background()
	t.Run("business and witness atomic root commit", func(t *testing.T) {
		e := reserve(t, s, "commit")
		local(t, db, s, e, "commit", false)
		assertDecision(t, s, e, transaction.Commit)
		if ok, err := s.Abort(ctx, e); err != nil || ok {
			t.Fatal("aborted committed execution", err)
		}
	})
	t.Run("root rollback has no business or committed witness", func(t *testing.T) {
		e := reserve(t, s, "rollback")
		local(t, db, s, e, "rollback", true)
		assertDecision(t, s, e, transaction.Unknown)
		n, err := db.Collection("tx_business").CountDocuments(ctx, bson.M{"_id": "rollback"})
		if err != nil || n != 0 {
			t.Fatal(n, err)
		}
		if ok, err := s.Abort(ctx, e); err != nil || !ok {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("abort fences late original transaction", func(t *testing.T) {
		e := reserve(t, s, "abort-first")
		if ok, err := s.Abort(ctx, e); err != nil || !ok {
			t.Fatal(err)
		}
		session, err := db.Client().StartSession()
		if err != nil {
			t.Fatal(err)
		}
		defer session.EndSession(ctx)
		if err = session.StartTransaction(); err != nil {
			t.Fatal(err)
		}
		sc := driver.NewSessionContext(ctx, session)
		if _, err = s.Bind(sc, e); !errors.Is(err, transaction.ErrStaleExecution) {
			t.Fatal(err)
		}
		session.AbortTransaction(sc)
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("abort competes with active execution write", func(t *testing.T) {
		e := reserve(t, s, "active")
		session, err := db.Client().StartSession()
		if err != nil {
			t.Fatal(err)
		}
		defer session.EndSession(ctx)
		if err = session.StartTransaction(); err != nil {
			t.Fatal(err)
		}
		sc := driver.NewSessionContext(ctx, session)
		guard, err := s.Bind(sc, e)
		if err != nil {
			t.Fatal(err)
		}
		timeout, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		if ok, err := s.Abort(timeout, e); err == nil || ok {
			t.Fatal("abort crossed original write lock", err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		if err = guard.CommitWitness(sc); err != nil {
			t.Fatal(err)
		}
		if err = session.CommitTransaction(sc); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Commit)
	})
	t.Run("concurrent first requests freeze one durable plan", func(t *testing.T) {
		const count = 10
		var wg sync.WaitGroup
		results := make(chan transaction.Reservation, count)
		errs := make(chan error, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				p := plan(t, fmt.Sprintf("parallel-%d", i), "parallel")
				r, err := s.Reserve(ctx, transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute})
				if err != nil {
					errs <- err
					return
				}
				results <- r
			}(i)
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		winners := 0
		var digest [32]byte
		for r := range results {
			if r.Acquired {
				winners++
			} else if r.Execution.Token != [32]byte{} {
				t.Fatal("duplicate stole capability")
			}
			if digest == [32]byte{} {
				digest = r.Execution.Plan.Digest()
			}
			if digest != r.Execution.Plan.Digest() {
				t.Fatal("different frozen winner")
			}
		}
		if winners != 1 {
			t.Fatal(winners)
		}
	})
	t.Run("driver callback retry keeps first identity and no external sends", func(t *testing.T) {
		e := reserve(t, s, "callback-retry")
		session, err := db.Client().StartSession()
		if err != nil {
			t.Fatal(err)
		}
		defer session.EndSession(ctx)
		if err = db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: bson.M{"failCommands": bson.A{"insert"}, "errorCode": 112, "errorLabels": bson.A{"TransientTransactionError"}}}}).Err(); err != nil {
			t.Fatal(err)
		}
		calls := 0
		_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (any, error) {
			calls++
			guard, err := s.Bind(sc, e)
			if err != nil {
				return nil, err
			}
			if _, err = db.Collection("tx_business").InsertOne(sc, bson.M{"_id": "callback-retry"}); err != nil {
				return nil, err
			}
			return nil, guard.CommitWitness(sc)
		})
		if err != nil || calls != 2 {
			t.Fatal(calls, err)
		}
		assertDecision(t, s, e, transaction.Commit)
		n, err := db.Collection("tx_business").CountDocuments(ctx, bson.M{"_id": "callback-retry"})
		if err != nil || n != 1 {
			t.Fatal(n, err)
		}
	})
	t.Run("commit retry does not replay local callback", func(t *testing.T) {
		e := reserve(t, s, "commit-retry")
		session, err := db.Client().StartSession()
		if err != nil {
			t.Fatal(err)
		}
		defer session.EndSession(ctx)
		if err = db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: bson.M{"failCommands": bson.A{"commitTransaction"}, "errorCode": 91, "errorLabels": bson.A{"UnknownTransactionCommitResult"}}}}).Err(); err != nil {
			t.Fatal(err)
		}
		calls := 0
		_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (any, error) {
			calls++
			guard, err := s.Bind(sc, e)
			if err != nil {
				return nil, err
			}
			if _, err = db.Collection("tx_business").InsertOne(sc, bson.M{"_id": "commit-retry"}); err != nil {
				return nil, err
			}
			return nil, guard.CommitWitness(sc)
		})
		if err != nil || calls != 1 {
			t.Fatal(calls, err)
		}
		assertDecision(t, s, e, transaction.Commit)
	})
	t.Run("explicit retry and replan preserve old terminal evidence", func(t *testing.T) {
		e := reserve(t, s, "retry")
		if _, err := s.Abort(ctx, e); err != nil {
			t.Fatal(err)
		}
		r, err := s.Retry(ctx, transaction.RetryAuthorization{Previous: e}, transaction.ReserveOptions{Lease: time.Minute})
		if err != nil || r.Execution.Fence != 2 {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Rollback)
		if _, err = s.Abort(ctx, r.Execution); err != nil {
			t.Fatal(err)
		}
		candidate := plan(t, "new-generation", "retry")
		next, err := s.Replan(ctx, transaction.ReplanAuthorization{Previous: r.Execution, Candidate: candidate}, transaction.ReserveOptions{Lease: time.Minute})
		if err != nil || next.Execution.Fence != 3 {
			t.Fatal(err)
		}
	})
	t.Run("expired prepared is unknown until durable reconciliation", func(t *testing.T) {
		e := reserve(t, s, "expiry")
		ek := key(e)
		if _, err := db.Collection("rm_tx_execution").UpdateOne(ctx, bson.M{"_id": ek[:]}, bson.M{"$set": bson.M{"lease_until": time.Now().Add(-time.Minute)}}); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		n, err := s.ReconcileOnce(ctx, 100)
		if err != nil || n != 1 {
			t.Fatal(n, err)
		}
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("missing original session and wrong store fail closed", func(t *testing.T) {
		e := reserve(t, s, "wrong")
		if _, err := s.Bind(nil, e); !errors.Is(err, transaction.ErrTransactionRequired) {
			t.Fatal(err)
		}
		wrong, err := New(db, transaction.Binding{Owner: "test.mongo", StoreID: "wrong", WriterEpoch: "epoch"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = wrong.Evidence(ctx, e.References()[0]); !errors.Is(err, transaction.ErrBinding) {
			t.Fatal(err)
		}
	})
}
