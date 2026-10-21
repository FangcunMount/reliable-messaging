package rocketmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

// This proves sender orchestration, not database atomicity or Broker acceptance.
type authorizedMemoryStore struct {
	*transactionMemoryStore
	calls  atomic.Int32
	before func(context.Context, transaction.ReserveOptions)
}

func (m *authorizedMemoryStore) advance(ctx context.Context, previous transaction.Execution, candidate transaction.FrozenPlan, opts transaction.ReserveOptions, replan bool) (transaction.Reservation, error) {
	m.calls.Add(1)
	if m.before != nil {
		m.before(ctx, opts)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous.Fence != m.e.Fence || previous.Plan.Digest() != m.e.Plan.Digest() {
		return transaction.Reservation{}, transaction.ErrStaleExecution
	}
	proof := make([]*transaction.Evidence, 0, len(previous.References()))
	for _, ref := range previous.References() {
		proof = append(proof, &transaction.Evidence{Reference: ref, ExecutionState: m.state, BranchState: m.state})
	}
	if err := transaction.ValidateAdvance(previous, candidate, proof, replan); err != nil {
		return transaction.Reservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return transaction.Reservation{}, err
	}
	next := transaction.Execution{Plan: candidate, Fence: previous.Fence + 1, WriterEpoch: m.Binding().WriterEpoch, Token: [32]byte{2}, LeaseUntil: time.Now().Add(opts.Lease)}
	m.e, m.state, m.begun = next, transaction.Prepared, false
	return transaction.Reservation{Execution: next, Acquired: true, State: transaction.Prepared}, nil
}
func (m *authorizedMemoryStore) Retry(ctx context.Context, a transaction.RetryAuthorization, opts transaction.ReserveOptions) (transaction.Reservation, error) {
	return m.advance(ctx, a.Previous, a.Previous.Plan, opts, false)
}
func (m *authorizedMemoryStore) Replan(ctx context.Context, a transaction.ReplanAuthorization, opts transaction.ReserveOptions) (transaction.Reservation, error) {
	return m.advance(ctx, a.Previous, a.Candidate, opts, true)
}
func replanFixture(t *testing.T, e transaction.Execution) transaction.FrozenPlan {
	t.Helper()
	input := e.Plan.Input()
	input.OperationID = "explicit-new-operation"
	input.Branches[0].Wire = []byte("explicit-new-first-wire")
	plan, err := transaction.NewPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
func TestAuthorizedSenderRetainsFirstRetryPlanAndRequiresExplicitNewPlan(t *testing.T) {
	for _, replan := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "replan"}[replan], func(t *testing.T) {
			previous, base := transactionFixture(t, 1)
			base.state = transaction.Aborted
			store := &authorizedMemoryStore{transactionMemoryStore: base}
			limits := transaction.DefaultLimits()
			store.before = func(ctx context.Context, opts transaction.ReserveOptions) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > limits.CheckTimeout || opts.Lease != limits.Lease {
					t.Error("missing actual database budget or configured lease")
				}
			}
			backend := &transactionFakeBackend{}
			sender := transactionRuntime(t, store, backend, limits)
			if err := sender.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if store.calls.Load() != 0 {
				t.Fatal("runtime authorized advancement without host")
			}
			previous.Token = [32]byte{} // Exact observed proof may authorize; no old execution capability needed.
			candidate := previous.Plan
			var result transaction.Reservation
			var err error
			if replan {
				candidate = replanFixture(t, previous)
				result, err = sender.Replan(context.Background(), transaction.ReplanAuthorization{Previous: previous, Candidate: candidate})
			} else {
				result, err = sender.Retry(context.Background(), transaction.RetryAuthorization{Previous: previous})
			}
			if err != nil || !result.Acquired || result.Execution.Fence != previous.Fence+1 || result.Execution.Plan.Digest() != candidate.Digest() {
				t.Fatal("incorrect explicit advancement", err)
			}
			if backend.prepares.Load() != 0 || sender.Stats().ActiveSends != 0 || store.calls.Load() != 1 {
				t.Fatal("advancement prepared a half or leaked capacity")
			}
		})
	}
}
func TestAuthorizedSenderNeverAdvancesPreparedOrCommittedExecution(t *testing.T) {
	for _, state := range []transaction.State{transaction.Prepared, transaction.Committed} {
		t.Run(string(state), func(t *testing.T) {
			previous, base := transactionFixture(t, 1)
			base.state = state
			store := &authorizedMemoryStore{transactionMemoryStore: base}
			backend := &transactionFakeBackend{}
			sender := transactionRuntime(t, store, backend, transaction.DefaultLimits())
			if err := sender.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := sender.Retry(context.Background(), transaction.RetryAuthorization{Previous: previous})
			if !errors.Is(err, transaction.ErrStaleExecution) || backend.prepares.Load() != 0 || sender.Stats().DatabaseErrors != 0 {
				t.Fatal("nonterminal proof authorized a retry or poisoned DB health", err)
			}
		})
	}
}
func TestAuthorizedSenderRejectsUnsupportedStoreAndWrongRouteBeforeAdmission(t *testing.T) {
	previous, base := transactionFixture(t, 1)
	backend := &transactionFakeBackend{}
	sender := transactionRuntime(t, base, backend, transaction.DefaultLimits())
	if _, err := sender.Retry(context.Background(), transaction.RetryAuthorization{Previous: previous}); !errors.Is(err, transaction.ErrBinding) {
		t.Fatal("unsupported store accepted", err)
	}
	store := &authorizedMemoryStore{transactionMemoryStore: base}
	sender = transactionRuntime(t, store, backend, transaction.DefaultLimits())
	input := previous.Plan.Input()
	input.Branches[0].PhysicalTopic = "not-configured"
	candidate, err := transaction.NewPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sender.Replan(context.Background(), transaction.ReplanAuthorization{Previous: previous, Candidate: candidate}); !errors.Is(err, transaction.ErrBinding) {
		t.Fatal("unconfigured route accepted", err)
	}
	if store.calls.Load() != 0 || backend.prepares.Load() != 0 {
		t.Fatal("invalid advancement touched store or Broker")
	}
}
func TestAuthorizedSenderCanceledStoreCallRetainsSharedCapacityAndDrain(t *testing.T) {
	previous, base := transactionFixture(t, 1)
	base.state = transaction.Aborted
	entered, release := make(chan struct{}), make(chan struct{})
	store := &authorizedMemoryStore{transactionMemoryStore: base, before: func(context.Context, transaction.ReserveOptions) { close(entered); <-release }}
	backend := &transactionFakeBackend{}
	limits := transaction.DefaultLimits()
	limits.Sends = 1
	sender := transactionRuntime(t, store, backend, limits)
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := sender.Retry(ctx, transaction.RetryAuthorization{Previous: previous}); done <- err }()
	<-entered
	cancel()
	queued := make(chan error, 1)
	go func() {
		_, err := sender.Reserve(context.Background(), transaction.RequestFor(previous.Plan), previous.Plan)
		queued <- err
	}()
	sender.StopAdmission()
	if sender.Stats().ActiveSends != 1 {
		close(release)
		t.Fatal("cancellation released running database capacity")
	}
	closing, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	if err := sender.Close(closing); !errors.Is(err, transaction.ErrShutdownIncomplete) {
		close(release)
		t.Fatal("shutdown claimed completion", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("actual canceled store result lost", err)
	}
	if err := <-queued; err == nil {
		t.Fatal("queued reserve admitted after stop")
	}
	wait, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	if err := sender.Wait(wait); err != nil {
		t.Fatal(err)
	}
	if sender.Stats().ActiveSends != 0 || backend.prepares.Load() != 0 || store.calls.Load() != 1 {
		t.Fatal("advance leaked work or sent without execution")
	}
}

func TestAuthorizedSenderRejectsMalformedFenceOrReplanBeforeStore(t *testing.T) {
	for _, damage := range []string{"zero-fence", "max-fence", "same-operation", "business-input"} {
		t.Run(damage, func(t *testing.T) {
			previous, base := transactionFixture(t, 1)
			store := &authorizedMemoryStore{transactionMemoryStore: base}
			sender := transactionRuntime(t, store, &transactionFakeBackend{}, transaction.DefaultLimits())
			var err error
			if damage == "zero-fence" || damage == "max-fence" {
				previous.Fence = 0
				if damage == "max-fence" {
					previous.Fence = transaction.MaxFence
				}
				_, err = sender.Retry(context.Background(), transaction.RetryAuthorization{Previous: previous})
				if !errors.Is(err, transaction.ErrInvalidPlan) {
					t.Fatal("malformed old execution accepted", err)
				}
			} else {
				candidate := previous.Plan
				if damage == "business-input" {
					input := replanFixture(t, previous).Input()
					input.InputHash = [32]byte{7}
					candidate, err = transaction.NewPlan(input)
					if err != nil {
						t.Fatal(err)
					}
				}
				_, err = sender.Replan(context.Background(), transaction.ReplanAuthorization{Previous: previous, Candidate: candidate})
				expected := transaction.ErrInvalidPlan
				if damage == "business-input" {
					expected = transaction.ErrInputConflict
				}
				if !errors.Is(err, expected) {
					t.Fatal("invalid new plan accepted", err)
				}
			}
			if store.calls.Load() != 0 || sender.Stats().ActiveSends != 0 {
				t.Fatal("invalid authorization touched protocol state")
			}
		})
	}
}
