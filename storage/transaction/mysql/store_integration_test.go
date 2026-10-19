//go:build integration

package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func fixture(t *testing.T) (*sql.DB, *Store) {
	t.Helper()
	dsn := os.Getenv("RM_TX_MYSQL_DSN")
	if dsn == "" || os.Getenv("RM_TX_DISPOSABLE") == "" {
		t.Fatal("required owned disposable MySQL fixture missing")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(40)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	// The test harness owns a NEW database per binary invocation. No DROP/truncate.
	for _, stmt := range strings.Split(Schema, ";") {
		if strings.TrimSpace(stmt) != "" {
			if _, err = db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO rm_tx_binding VALUES('test.mysql','disposable-store','epoch',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE tx_business(id VARBINARY(128) PRIMARY KEY,value INT NOT NULL) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	s, err := New(db, transaction.Binding{Owner: "test.mysql", StoreID: "disposable-store", WriterEpoch: "epoch"})
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
	p, err := transaction.NewPlan(transaction.PlanInput{Owner: "test.mysql", Scope: "scope", OperationID: "operation-" + id, IdempotencyKey: key, InputHash: sha256.Sum256([]byte("same business input")), Branches: []transaction.BranchInput{{ID: "branch", PhysicalTopic: "physical", RouteRevision: "v2", Message: m, Wire: []byte{0, 255, 5}}}})
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
		t.Fatal(r, err)
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

func TestOriginalTransactionAndReservationProofs(t *testing.T) {
	db, s := fixture(t)
	ctx := context.Background()
	t.Run("business and witness commit in original sql transaction", func(t *testing.T) {
		e := reserve(t, s, "commit")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		guard, err := s.Bind(ctx, tx, e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO tx_business VALUES('commit',1)`); err != nil {
			t.Fatal(err)
		}
		if err = guard.CommitWitness(ctx); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Commit)
		if ok, err := s.Abort(ctx, e); err != nil || ok {
			t.Fatal("committed execution aborted", err)
		}
		if _, err = s.Bind(ctx, tx, e); err == nil {
			t.Fatal("ended transaction accepted")
		}
	})
	t.Run("rollback leaves no business or false committed witness", func(t *testing.T) {
		e := reserve(t, s, "rollback")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		guard, err := s.Bind(ctx, tx, e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO tx_business VALUES('rollback',1)`); err != nil {
			t.Fatal(err)
		}
		if err = guard.CommitWitness(ctx); err != nil {
			t.Fatal(err)
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		var n int
		if err = db.QueryRow(`SELECT COUNT(*) FROM tx_business WHERE id='rollback'`).Scan(&n); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		if ok, err := s.Abort(ctx, e); err != nil || !ok {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("abort fences late business and old terminal survives new fence", func(t *testing.T) {
		e := reserve(t, s, "abort-first")
		if ok, err := s.Abort(ctx, e); err != nil || !ok {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Bind(ctx, tx, e); !errors.Is(err, transaction.ErrStaleExecution) {
			t.Fatal(err)
		}
		tx.Rollback()
		next, err := s.Retry(ctx, transaction.RetryAuthorization{Previous: e}, transaction.ReserveOptions{Lease: time.Minute})
		if err != nil || !next.Acquired || next.Execution.Fence != e.Fence+1 || next.Execution.Plan.Digest() != e.Plan.Digest() {
			t.Fatal("retry lost frozen plan", err)
		}
		assertDecision(t, s, e, transaction.Rollback)
		assertDecision(t, s, next.Execution, transaction.Unknown)
		if _, err = s.Retry(ctx, transaction.RetryAuthorization{Previous: e}, transaction.ReserveOptions{Lease: time.Minute}); !errors.Is(err, transaction.ErrStaleExecution) {
			t.Fatal("stale retry accepted", err)
		}
	})
	t.Run("abort cannot race past active original transaction", func(t *testing.T) {
		e := reserve(t, s, "active")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		guard, err := s.Bind(ctx, tx, e)
		if err != nil {
			t.Fatal(err)
		}
		timeout, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			ok, err := s.Abort(timeout, e)
			if ok {
				done <- errors.New("abort crossed original transaction lock")
				return
			}
			done <- err
		}()
		if err = <-done; err == nil {
			t.Fatal("abort should time out behind real write lock")
		}
		assertDecision(t, s, e, transaction.Unknown)
		if err = guard.CommitWitness(ctx); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Commit)
	})
	t.Run("concurrent duplicate requests retain winner and hide capability", func(t *testing.T) {
		const count = 20
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
				if r.Execution.Token == [32]byte{} {
					t.Fatal("winner lacks capability")
				}
			} else if r.Execution.Token != [32]byte{} {
				t.Fatal("duplicate stole winner capability")
			}
			if digest == [32]byte{} {
				digest = r.Execution.Plan.Digest()
			}
			if r.Execution.Plan.Digest() != digest {
				t.Fatal("different first plan")
			}
		}
		if winners != 1 {
			t.Fatal(winners)
		}
		p := plan(t, "conflict", "parallel")
		in := p.Input()
		in.InputHash = sha256.Sum256([]byte("different"))
		p, err := transaction.NewPlan(in)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Reserve(ctx, transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute}); !errors.Is(err, transaction.ErrInputConflict) {
			t.Fatal(err)
		}
	})
	t.Run("gorm borrows root and ordinary handle rejected", func(t *testing.T) {
		gdb, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		e := reserve(t, s, "gorm")
		if _, err = s.BindGORM(ctx, gdb, e); !errors.Is(err, transaction.ErrTransactionRequired) {
			t.Fatal(err)
		}
		err = gdb.Transaction(func(tx *gorm.DB) error {
			guard, err := s.BindGORM(ctx, tx, e)
			if err != nil {
				return err
			}
			if err = tx.Exec(`INSERT INTO tx_business VALUES('gorm',1)`).Error; err != nil {
				return err
			}
			return guard.CommitWitness(ctx)
		})
		if err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Commit)
	})
	t.Run("expired prepared remains unknown until durable abort", func(t *testing.T) {
		e := reserve(t, s, "expired")
		ek := key(e)
		if _, err := db.Exec(`UPDATE rm_tx_execution SET lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) WHERE execution_key=?`, ek[:]); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		n, err := s.ReconcileOnce(ctx, 100)
		if err != nil || n != 1 {
			t.Fatal(n, err)
		}
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("explicit replan preserves old operation evidence", func(t *testing.T) {
		e := reserve(t, s, "replan")
		if _, err := s.Abort(ctx, e); err != nil {
			t.Fatal(err)
		}
		candidate := plan(t, "replanned", "replan")
		r, err := s.Replan(ctx, transaction.ReplanAuthorization{Previous: e, Candidate: candidate}, transaction.ReserveOptions{Lease: time.Minute})
		if err != nil || !r.Acquired || r.Execution.Plan.Digest() == e.Plan.Digest() {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Rollback)
	})
	t.Run("prepare admission consumes capability once before broker IO", func(t *testing.T) {
		e := reserve(t, s, "prepare-once")
		const count = 10
		var wg sync.WaitGroup
		results := make(chan error, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); results <- s.BeginPrepare(ctx, e) }()
		}
		wg.Wait()
		close(results)
		winners := 0
		for err := range results {
			if err == nil {
				winners++
			} else if !errors.Is(err, transaction.ErrStaleExecution) {
				t.Fatal(err)
			}
		}
		if winners != 1 {
			t.Fatal("prepare admission winners", winners)
		}
		observed, err := s.Lookup(ctx, transaction.RequestFor(e.Plan))
		if err != nil {
			t.Fatal(err)
		}
		if err = s.BeginPrepare(ctx, observed.Execution); !errors.Is(err, transaction.ErrBinding) {
			t.Fatal("observer gained send capability", err)
		}
	})
	t.Run("Broker receipt audit is separate from local commitment proof", func(t *testing.T) {
		e := reserve(t, s, "receipts")
		ref := e.References()[0]
		if err := s.Validate(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.BeginPrepare(ctx, e); err != nil {
			t.Fatal(err)
		}
		h := transaction.HalfReceipt{Outcome: transaction.HalfPrepared, BrokerMessageID: "synthetic-broker-id", BrokerTransactionID: "synthetic-broker-tx"}
		if err := s.RecordHalf(ctx, e, ref, h); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Unknown)
		if err := s.RecordFinal(ctx, e, ref, transaction.FinalCommitConfirmed, ""); !errors.Is(err, transaction.ErrStaleExecution) {
			t.Fatal("receipt fabricated local commit proof", err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		guard, err := s.Bind(ctx, tx, e)
		if err != nil {
			t.Fatal(err)
		}
		if err = guard.CommitWitness(ctx); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = s.RecordFinal(ctx, e, ref, transaction.FinalUnknown, "lost_ack"); err != nil {
			t.Fatal(err)
		}
		assertDecision(t, s, e, transaction.Commit)
		var brokerID, brokerTx, half, final string
		ek, bk := key(e), transaction.BranchKey(ref.BranchID)
		if err = db.QueryRow(`SELECT broker_message_id,broker_transaction_id,half_outcome,final_outcome FROM rm_tx_witness WHERE execution_key=? AND branch_key=?`, ek[:], bk[:]).Scan(&brokerID, &brokerTx, &half, &final); err != nil {
			t.Fatal(err)
		}
		if brokerID != h.BrokerMessageID || brokerTx != h.BrokerTransactionID || half != string(transaction.HalfPrepared) || final != string(transaction.FinalUnknown) {
			t.Fatal("receipt identity/outcome lost")
		}
	})
	t.Run("schema and store identity fail closed", func(t *testing.T) {
		wrong, err := New(db, transaction.Binding{Owner: "test.mysql", StoreID: "wrong", WriterEpoch: "epoch"})
		if err != nil {
			t.Fatal(err)
		}
		p := plan(t, "wrong", "wrong")
		if _, err = wrong.Reserve(ctx, transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute}); !errors.Is(err, transaction.ErrBinding) {
			t.Fatal(err)
		}
		if _, err = db.Exec(`UPDATE rm_tx_binding SET schema_version=2`); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Reserve(ctx, transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute}); !errors.Is(err, transaction.ErrSchema) {
			t.Fatal(err)
		}
	})
}
