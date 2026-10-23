package rocketmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
)

type recoveryAuditStore struct {
	*transactionMemoryStore
	writes           atomic.Int32
	result           error
	entered, release chan struct{}
}

func (m *recoveryAuditStore) RecordRecoveryFinal(ctx context.Context, ref transaction.Reference, outcome transaction.FinalOutcome) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing audit deadline")
	}
	m.writes.Add(1)
	if m.entered != nil {
		close(m.entered)
		<-m.release
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return m.result
}
func TestRecoveryConfirmationAuditIsSeparateFromReadOnlyChecker(t *testing.T) {
	for _, tc := range []struct {
		name             string
		decision         driver.TransactionResolution
		rpcErr, auditErr error
		writes           int32
	}{
		{"commit", driver.COMMIT, nil, nil, 1},
		{"rollback", driver.ROLLBACK, nil, nil, 1},
		{"unknown decision", driver.UNKNOWN, nil, nil, 0},
		{"RPC did not acknowledge", driver.COMMIT, context.DeadlineExceeded, nil, 0},
		{"audit unavailable", driver.COMMIT, nil, errors.New("database unavailable"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, memory := transactionFixture(t, 1)
			store := &recoveryAuditStore{transactionMemoryStore: memory, result: tc.auditErr}
			s := transactionRuntime(t, store, &transactionFakeBackend{}, transaction.DefaultLimits())
			if err := s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			memory.set(transaction.Committed)
			ref := e.References()[0]
			raw, err := transaction.EncodeReference(ref)
			if err != nil {
				t.Fatal(err)
			}
			if s.checkMessage(context.Background(), ref.PhysicalTopic, raw, e.Plan.Input().Branches[0].Wire) != driver.COMMIT || store.writes.Load() != 0 {
				t.Fatal("Checker wrote confirmation audit")
			}
			s.recoveryConfirmedMessage(ref.PhysicalTopic, raw, e.Plan.Input().Branches[0].Wire, tc.decision, tc.rpcErr)
			stats := s.Stats()
			if store.writes.Load() != tc.writes {
				t.Fatal("audit did not match actual acknowledgement", stats)
			}
			if tc.rpcErr != nil && stats.RecoveryRPCFailed != 1 {
				t.Fatal("RPC failure collapsed into confirmation")
			}
			if tc.auditErr != nil && (stats.RecoveryAuditErrors != 1 || stats.RecoveryAuditRecorded != 0 || stats.Ready) {
				t.Fatal("audit failure reported healthy", stats)
			}
			if tc.writes == 1 && tc.auditErr == nil && stats.RecoveryAuditRecorded != 1 {
				t.Fatal("positive audit absent", stats)
			}
		})
	}
}
func TestRecoveryAuditRetainsWorkerCapacityUntilActualDatabaseCompletion(t *testing.T) {
	e, memory := transactionFixture(t, 1)
	store := &recoveryAuditStore{transactionMemoryStore: memory, entered: make(chan struct{}), release: make(chan struct{})}
	limits := transaction.DefaultLimits()
	limits.CheckWorkers = 1
	limits.CheckQueue = 0
	limits.CheckTimeout = 20 * time.Millisecond
	s := transactionRuntime(t, store, &transactionFakeBackend{}, limits)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref := e.References()[0]
	raw, err := transaction.EncodeReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	// Unbuffered dispatch may find the worker before its receive. Retry only this
	// controlled test admission, never an MQ RPC or business execution.
	deadline := time.Now().Add(time.Second)
	for !s.dispatch(func() {
		s.recoveryConfirmedMessage(ref.PhysicalTopic, raw, e.Plan.Input().Branches[0].Wire, driver.COMMIT, nil)
	}) {
		if time.Now().After(deadline) {
			t.Fatal("worker did not enter receive")
		}
		time.Sleep(time.Millisecond)
	}
	<-store.entered
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); err == nil {
		t.Fatal("audit still running but shutdown reported complete")
	}
	if s.Stats().RunningChecks != 1 || s.dispatch(func() {}) {
		t.Fatal("actual audit capacity released before completion")
	}
	close(store.release)
	if err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Stats().RunningChecks != 0 || s.Stats().RecoveryAuditErrors != 1 {
		t.Fatal("audit completion not accounted")
	}
}
func TestRecoveryAuditRejectsActualWireAndUnsupportedStore(t *testing.T) {
	e, memory := transactionFixture(t, 1)
	s := transactionRuntime(t, memory, &transactionFakeBackend{}, transaction.DefaultLimits())
	ref := e.References()[0]
	raw, err := transaction.EncodeReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	s.recoveryConfirmedMessage(ref.PhysicalTopic, raw, []byte("different"), driver.COMMIT, nil)
	if s.Stats().RecoveryAuditErrors != 1 {
		t.Fatal("wire mismatch not rejected")
	}
	s.recoveryConfirmedMessage(ref.PhysicalTopic, raw, e.Plan.Input().Branches[0].Wire, driver.COMMIT, nil)
	if s.Stats().RecoveryAuditUnsupported != 1 || s.Stats().RecoveryAuditRecorded != 0 {
		t.Fatal("unsupported store fabricated audit")
	}
}
