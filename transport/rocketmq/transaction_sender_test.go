package rocketmq

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// These fakes prove orchestration only. Real adapters/Broker are separate gates.
type transactionMemoryStore struct {
	mu     sync.Mutex
	e      transaction.Execution
	state  transaction.State
	begun  bool
	halves map[string]transaction.HalfReceipt
	finals map[string]transaction.FinalOutcome
	err    error
}

func (m *transactionMemoryStore) Binding() transaction.Binding {
	return transaction.Binding{Owner: "test.mysql", StoreID: "fixture", WriterEpoch: "epoch"}
}
func (m *transactionMemoryStore) Validate(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}
func (m *transactionMemoryStore) Lookup(context.Context, transaction.Request) (transaction.Reservation, error) {
	return transaction.Reservation{}, transaction.ErrNotFound
}
func (m *transactionMemoryStore) Reserve(context.Context, transaction.Request, transaction.FrozenPlan, transaction.ReserveOptions) (transaction.Reservation, error) {
	return transaction.Reservation{}, transaction.ErrInvalidPlan
}
func (m *transactionMemoryStore) BeginPrepare(ctx context.Context, e transaction.Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if m.err != nil {
		return m.err
	}
	if m.begun || e.Token != m.e.Token {
		return transaction.ErrStaleExecution
	}
	m.begun = true
	return nil
}
func (m *transactionMemoryStore) Evidence(_ context.Context, r transaction.Reference) (*transaction.Evidence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &transaction.Evidence{Reference: r, ExecutionState: m.state, BranchState: m.state}, nil
}
func (m *transactionMemoryStore) Abort(context.Context, transaction.Execution) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if m.state != transaction.Prepared {
		return false, nil
	}
	m.state = transaction.Aborted
	return true, nil
}
func (m *transactionMemoryStore) ReconcileOnce(context.Context, int) (int, error) { return 0, nil }
func (m *transactionMemoryStore) RecordHalf(_ context.Context, _ transaction.Execution, r transaction.Reference, h transaction.HalfReceipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.halves[r.BranchID] = h
	return m.err
}
func (m *transactionMemoryStore) RecordFinal(_ context.Context, _ transaction.Execution, r transaction.Reference, f transaction.FinalOutcome, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finals[r.BranchID] = f
	return m.err
}
func (m *transactionMemoryStore) set(state transaction.State) {
	m.mu.Lock()
	m.state = state
	m.mu.Unlock()
}

type transactionFakeBackend struct {
	prepare                  func(transaction.Reference) (transaction.HalfReceipt, transactionHandle, error)
	starts, closes, prepares atomic.Int32
	startErr                 error
}

func (b *transactionFakeBackend) Start(context.Context) error { b.starts.Add(1); return b.startErr }
func (b *transactionFakeBackend) Close() error                { b.closes.Add(1); return nil }
func (b *transactionFakeBackend) Prepare(_ context.Context, r transaction.Reference, _ []byte) (transaction.HalfReceipt, transactionHandle, error) {
	b.prepares.Add(1)
	if b.prepare != nil {
		return b.prepare(r)
	}
	return transaction.HalfReceipt{Outcome: transaction.HalfPrepared, BrokerMessageID: "broker-" + r.MessageID, BrokerTransactionID: "broker-tx-" + r.BranchID}, transactionFakeHandle{}, nil
}

type transactionFakeHandle struct {
	err     error
	decided *transaction.Decision
}

func (h transactionFakeHandle) Finalize(_ context.Context, d transaction.Decision) error {
	if h.decided != nil {
		*h.decided = d
	}
	return h.err
}
func transactionFixture(t *testing.T, count int) (transaction.Execution, *transactionMemoryStore) {
	t.Helper()
	in := transaction.PlanInput{Owner: "test.mysql", Scope: "scope", OperationID: "operation", IdempotencyKey: "key", InputHash: sha256.Sum256([]byte("input"))}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("branch-%d", i)
		m, err := message.New(message.Input{Producer: "fixture", ID: "business-" + id, Destination: "logical", EventType: "created", SchemaVersion: "1", Scope: "scope", ContentType: "application/octet-stream", OccurredAt: "2026-10-19T00:00:00Z", Payload: []byte{0, 255}})
		if err != nil {
			t.Fatal(err)
		}
		in.Branches = append(in.Branches, transaction.BranchInput{ID: id, PhysicalTopic: transactionTestTopic(t), RouteRevision: "v2", Message: m, Wire: []byte("frozen-" + id)})
	}
	p, err := transaction.NewPlan(in)
	if err != nil {
		t.Fatal(err)
	}
	e := transaction.Execution{Plan: p, Fence: 1, WriterEpoch: "epoch", Token: [32]byte{1}, LeaseUntil: time.Now().Add(time.Minute)}
	return e, &transactionMemoryStore{e: e, state: transaction.Prepared, halves: map[string]transaction.HalfReceipt{}, finals: map[string]transaction.FinalOutcome{}}
}
func transactionRuntime(t *testing.T, store *transactionMemoryStore, backend *transactionFakeBackend, limits transaction.Limits) *TransactionalSender {
	t.Helper()
	s, err := NewTransactionalSender(TransactionConfig{Endpoint: "127.0.0.1:8081", Owner: "test.mysql", AccessKey: "fixture", SecretKey: "fixture", Limits: limits, Routes: []TransactionRoute{{LogicalTopic: "logical", PhysicalTopic: transactionTestTopic(t), Revision: "v2"}}}, store)
	if err != nil {
		t.Fatal(err)
	}
	s.factory = func(func(func()) bool, func(context.Context, *driver.MessageView) driver.TransactionResolution) (transactionBackend, error) {
		return backend, nil
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestTransactionSenderSeparatesLocalCommitFromLostFinalAck(t *testing.T) {
	e, store := transactionFixture(t, 2)
	backend := &transactionFakeBackend{prepare: func(r transaction.Reference) (transaction.HalfReceipt, transactionHandle, error) {
		return transaction.HalfReceipt{Outcome: transaction.HalfPrepared, BrokerMessageID: "broker", BrokerTransactionID: "broker-tx"}, transactionFakeHandle{err: errors.New("ack lost")}, nil
	}}
	s := transactionRuntime(t, store, backend, transaction.DefaultLimits())
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	result, err := s.Execute(context.Background(), e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
		calls++
		store.set(transaction.Committed)
		return transaction.LocalCommitted, nil
	})
	if err == nil || calls != 1 || result.Local != transaction.LocalCommitted {
		t.Fatal("local commit was collapsed into publish failure", err)
	}
	for _, b := range result.Branches {
		if b.Half != transaction.HalfPrepared || b.Final != transaction.FinalUnknown || b.BrokerMessageID == b.Reference.MessageID {
			t.Fatal("phase/Broker/business identities conflated")
		}
	}
	stats := s.Stats()
	if stats.CompletedResults != 1 || stats.LocalCommittedResults != 1 || stats.FinalUnknownResults != 2 || stats.HalfPreparedResults != 2 {
		t.Fatal("lost final reply hidden from phase counters", stats)
	}
	if _, err = s.Execute(context.Background(), e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
		calls++
		return transaction.LocalCommitted, nil
	}); !errors.Is(err, transaction.ErrStaleExecution) || calls != 1 {
		t.Fatal("re-executed consumed capability", err)
	}
}
func TestTransactionPartialPreparationNeverRunsBusinessAndRollsBackPreparedBranch(t *testing.T) {
	e, store := transactionFixture(t, 3)
	var decision transaction.Decision
	backend := &transactionFakeBackend{prepare: func(r transaction.Reference) (transaction.HalfReceipt, transactionHandle, error) {
		if r.BranchID == "branch-1" {
			return transaction.HalfReceipt{Outcome: transaction.HalfUnknown}, nil, errors.New("half acknowledgement lost")
		}
		return transaction.HalfReceipt{Outcome: transaction.HalfPrepared, BrokerMessageID: "broker", BrokerTransactionID: "tx"}, transactionFakeHandle{decided: &decision}, nil
	}}
	s := transactionRuntime(t, store, backend, transaction.DefaultLimits())
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	r, err := s.Execute(context.Background(), e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
		calls++
		return transaction.LocalCommitted, nil
	})
	if err == nil || calls != 0 || r.Local != transaction.LocalNotStarted || backend.prepares.Load() != 2 || decision != transaction.Rollback {
		t.Fatal("partial prepare executed business or guessed commitment", err)
	}
	if r.Branches[0].Final != transaction.FinalRollbackConfirmed || r.Branches[1].Half != transaction.HalfUnknown || r.Branches[2].Half != transaction.HalfNotAttempted {
		t.Fatal("partial phase results lost")
	}
}
func TestTransactionCallbackReturnWithoutWitnessNeverConfirmsCommit(t *testing.T) {
	e, store := transactionFixture(t, 1)
	backend := &transactionFakeBackend{}
	s := transactionRuntime(t, store, backend, transaction.DefaultLimits())
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := s.Execute(context.Background(), e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
		return transaction.LocalCommitted, nil
	})
	if err == nil || r.Local != transaction.LocalUnknown || r.Branches[0].Final != transaction.FinalNotAttempted {
		t.Fatal("callback was used as commit proof", err)
	}
}
func TestTransactionCancellationKeepsRealUOWSlotUntilCompletion(t *testing.T) {
	e, store := transactionFixture(t, 1)
	backend := &transactionFakeBackend{}
	s := transactionRuntime(t, store, backend, transaction.DefaultLimits())
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan transaction.Result, 1)
	go func() {
		r, _ := s.Execute(ctx, e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
			close(entered)
			<-release
			store.set(transaction.Committed)
			return transaction.LocalCommitted, nil
		})
		returned <- r
	}()
	<-entered
	cancel()
	r := <-returned
	if r.Local != transaction.LocalUnknown || s.Stats().ActiveSends != 1 {
		t.Fatal("cancellation released live UOW capacity")
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	err := s.Close(short)
	var incomplete *transaction.ShutdownIncomplete
	if !errors.Is(err, transaction.ErrShutdownIncomplete) || !errors.As(err, &incomplete) || incomplete.ActiveSends != 1 || !incomplete.RuntimePending || backend.closes.Load() != 0 {
		t.Fatal("shutdown falsely drained live business", err)
	}
	releaseOnce.Do(func() { close(release) })
	wait, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := s.Close(wait); err != nil {
		t.Fatal(err)
	}
	if s.Stats().ActiveSends != 0 || backend.closes.Load() != 1 {
		t.Fatal("actual completion did not release owned resources")
	}
}
func TestTransactionCheckerQueueAndShutdownAreBounded(t *testing.T) {
	_, store := transactionFixture(t, 1)
	backend := &transactionFakeBackend{}
	limits := transaction.DefaultLimits()
	limits.CheckWorkers = 1
	limits.CheckQueue = 1
	s := transactionRuntime(t, store, backend, limits)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	if !s.dispatch(func() { close(entered); <-release }) {
		t.Fatal("first check rejected")
	}
	<-entered
	if !s.dispatch(func() {}) || s.dispatch(func() { t.Error("saturated check was executed") }) {
		t.Fatal("queue was not bounded")
	}
	if stats := s.Stats(); stats.RunningChecks != 1 || stats.QueuedChecks != 1 || stats.CheckRejected != 1 {
		t.Fatal(stats)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, transaction.ErrShutdownIncomplete) || backend.closes.Load() != 0 {
		t.Fatal("live checker falsely drained", err)
	}
	once.Do(func() { close(release) })
	wait, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := s.Close(wait); err != nil {
		t.Fatal(err)
	}
}
func TestTransactionCheckerValidatesActualTopicWireAndExactEvidence(t *testing.T) {
	e, store := transactionFixture(t, 1)
	s := transactionRuntime(t, store, &transactionFakeBackend{}, transaction.DefaultLimits())
	ref := e.References()[0]
	raw, err := transaction.EncodeReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	body := e.Plan.Input().Branches[0].Wire
	store.set(transaction.Committed)
	if got := s.checkMessage(context.Background(), transactionTestTopic(t), raw, body); got != driver.COMMIT {
		t.Fatal(got)
	}
	for _, tc := range []struct {
		topic     string
		raw, body []byte
	}{{"wrong", raw, body}, {transactionTestTopic(t), raw, []byte("different wire")}, {transactionTestTopic(t), []byte("{}"), body}} {
		if got := s.checkMessage(context.Background(), tc.topic, tc.raw, tc.body); got != driver.UNKNOWN {
			t.Fatal("invalid recovery committed", got)
		}
	}
	store.mu.Lock()
	store.err = errors.New("database unavailable")
	store.mu.Unlock()
	if got := s.checkMessage(context.Background(), transactionTestTopic(t), raw, body); got != driver.UNKNOWN {
		t.Fatal("DB failure guessed rollback")
	}
}
func TestTransactionConstructorsAndFailedStartupOwnNoHiddenResources(t *testing.T) {
	e, store := transactionFixture(t, 1)
	backend := &transactionFakeBackend{}
	s := transactionRuntime(t, store, backend, transaction.DefaultLimits())
	if backend.starts.Load() != 0 || s.Stats().Ready {
		t.Fatal("constructor started backend")
	}
	if _, err := s.Execute(context.Background(), e, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
		t.Fatal("business before Start")
		return transaction.LocalUnknown, nil
	}); err == nil {
		t.Fatal("unstarted sender accepted")
	}
	store.err = errors.New("bad binding")
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("invalid startup succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if backend.starts.Load() != 0 || !s.Stats().Stopped {
		t.Fatal("failed startup leaked owned work")
	}
}

func TestTransactionCheckerPanicDoesNotLoseBoundedWorker(t *testing.T) {
	_, store := transactionFixture(t, 1)
	limits := transaction.DefaultLimits()
	limits.CheckWorkers = 1
	limits.CheckQueue = 1
	s := transactionRuntime(t, store, &transactionFakeBackend{}, limits)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, next := make(chan struct{}), make(chan struct{})
	if !s.dispatch(func() { close(entered); panic("synthetic callback failure") }) {
		t.Fatal("check rejected")
	}
	<-entered
	if !s.dispatch(func() { close(next) }) {
		t.Fatal("worker disappeared after panic")
	}
	select {
	case <-next:
	case <-time.After(time.Second):
		t.Fatal("checker did not survive failed callback")
	}
	if s.Stats().CallbackErrors != 1 {
		t.Fatal("callback failure not observable")
	}
}

func transactionTestTopic(t *testing.T) string {
	t.Helper()
	topic, err := transport.PhysicalTopicV2("logical", transport.TransactionMessageType, "test.mysql", "v2")
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestHalfRejectionRequiresStructuredProtocolRefusal(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{&driver.ErrRpcStatus{Code: 40300}, "half_rejected_40300"},
		{fmt.Errorf("startup: %w", grpcstatus.Error(codes.PermissionDenied, "synthetic refusal")), "half_rejected_grpc_permission_denied"},
		{grpcstatus.Error(codes.Unauthenticated, "synthetic refusal"), "half_rejected_grpc_unauthenticated"},
		{grpcstatus.Error(codes.Unavailable, "synthetic connection loss"), ""},
		{context.DeadlineExceeded, ""},
		{errors.New("PermissionDenied text is not proof"), ""},
	}
	for _, test := range tests {
		if got := rejectedHalfCode(test.err); got != test.want {
			t.Fatalf("structured rejection %q, want %q", got, test.want)
		}
	}
}
