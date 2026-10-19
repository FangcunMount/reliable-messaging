package rocketmq

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
)

// TransactionStats has bounded cardinality. It contains no message IDs, tokens,
// wire, tenant labels or credentials. Hosts export it through their own metrics.
type TransactionStats struct {
	ActiveSends, RunningChecks, QueuedChecks                                                                           int
	CheckRejected, CheckUnknown, CheckCommit, CheckRollback, InvalidChecks, DatabaseErrors, Reconciled, CallbackErrors int64
	Ready, Stopping, Stopped                                                                                           bool
}
type TransactionalSender struct {
	config                                                            TransactionConfig
	store                                                             transaction.PreparedStore
	routes                                                            map[string]TransactionRoute
	factory                                                           backendFactory
	backend                                                           transactionBackend
	mu                                                                sync.Mutex
	everStarted, started, stopping, checksClosing, dbHealthy, stopped bool
	startDone, closeDone, sendStop, maintStop, maintDone              chan struct{}
	closeOnce                                                         sync.Once
	closeErr                                                          error
	slots                                                             chan struct{}
	checks                                                            chan func()
	sendWG, checkWG                                                   sync.WaitGroup
	stats                                                             TransactionStats
}

var _ transaction.Runtime = (*TransactionalSender)(nil)
var _ transaction.Sender = (*TransactionalSender)(nil)

func NewTransactionalSender(c TransactionConfig, store transaction.PreparedStore) (*TransactionalSender, error) {
	c, err := validateTransactionConfig(c, store)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	close(done)
	maintDone := make(chan struct{})
	close(maintDone)
	s := &TransactionalSender{config: c, store: store, routes: map[string]TransactionRoute{}, startDone: done, closeDone: make(chan struct{}), sendStop: make(chan struct{}), maintStop: make(chan struct{}), maintDone: maintDone, slots: make(chan struct{}, c.Limits.Sends), checks: make(chan func(), c.Limits.CheckQueue)}
	for _, r := range c.Routes {
		s.routes[r.PhysicalTopic] = r
	}
	s.factory = s.driverFactory
	return s, nil
}
func (s *TransactionalSender) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.everStarted || s.stopping {
		s.mu.Unlock()
		return transaction.ErrStaleExecution
	}
	s.everStarted = true
	s.startDone = make(chan struct{})
	startDone := s.startDone
	s.mu.Unlock()
	startCtx, cancel := context.WithTimeout(ctx, s.config.Limits.RPCTimeout)
	defer cancel()
	checkCtx, checkCancel := context.WithTimeout(startCtx, s.config.Limits.CheckTimeout)
	err := s.store.Validate(checkCtx)
	checkCancel()
	if err == nil {
		for i := 0; i < s.config.Limits.CheckWorkers; i++ {
			s.checkWG.Add(1)
			go s.checkWorker()
		}
		var backend transactionBackend
		backend, err = s.factory(s.dispatch, s.check)
		if err == nil {
			s.mu.Lock()
			s.backend = backend
			s.mu.Unlock()
			err = backend.Start(startCtx)
		}
	}
	s.mu.Lock()
	s.started = err == nil
	s.dbHealthy = err == nil
	stopping := s.stopping
	if err == nil && !stopping {
		s.maintDone = make(chan struct{})
		go s.maintain()
	}
	close(startDone)
	s.mu.Unlock()
	if err != nil || stopping {
		s.StopAdmission()
		s.closeAsync()
		if err == nil {
			err = transaction.ErrShutdownIncomplete
		}
		select {
		case <-s.closeDone:
			return errors.Join(err, s.closeErr)
		case <-startCtx.Done():
			return errors.Join(err, s.incomplete(startCtx.Err()))
		}
	}
	return nil
}
func (s *TransactionalSender) checkWorker() {
	defer s.checkWG.Done()
	for work := range s.checks {
		s.runCheckWork(work)
	}
}
func (s *TransactionalSender) runCheckWork(work func()) {
	s.mu.Lock()
	s.stats.RunningChecks++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.stats.RunningChecks--
		if recover() != nil {
			s.stats.CallbackErrors++
			s.stats.CheckUnknown++
			s.dbHealthy = false
		}
		s.mu.Unlock()
	}()
	work()
}
func (s *TransactionalSender) dispatch(work func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checksClosing {
		s.stats.CheckRejected++
		return false
	}
	select {
	case s.checks <- work:
		return true
	default:
		s.stats.CheckRejected++
		return false
	}
}
func (s *TransactionalSender) check(ctx context.Context, msg *driver.MessageView) driver.TransactionResolution {
	if msg == nil {
		return driver.UNKNOWN
	}
	return s.checkMessage(ctx, msg.GetTopic(), []byte(msg.GetProperties()[TransactionReferenceProperty]), msg.GetBody())
}
func (s *TransactionalSender) checkMessage(ctx context.Context, physical string, raw, body []byte) driver.TransactionResolution {
	ref, err := transaction.DecodeReference(raw)
	route, ok := s.routes[physical]
	if err != nil || !ok || ref.Owner != s.config.Owner || ref.PhysicalTopic != physical || ref.LogicalTopic != route.LogicalTopic || ref.RouteRevision != route.Revision || len(body) == 0 || len(body) > 262144 || sha256.Sum256(body) != ref.WireHash {
		s.mu.Lock()
		s.stats.InvalidChecks++
		s.stats.CheckUnknown++
		s.mu.Unlock()
		return driver.UNKNOWN
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Limits.CheckTimeout)
	defer cancel()
	evidence, err := s.store.Evidence(ctx, ref)
	decision := transaction.Unknown
	if err == nil && ctx.Err() == nil {
		decision = transaction.Resolve(ref, evidence)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil || ctx.Err() != nil {
		s.stats.DatabaseErrors++
		s.dbHealthy = false
	}
	switch decision {
	case transaction.Commit:
		s.stats.CheckCommit++
		return driver.COMMIT
	case transaction.Rollback:
		s.stats.CheckRollback++
		return driver.ROLLBACK
	default:
		s.stats.CheckUnknown++
		return driver.UNKNOWN
	}
}
func (s *TransactionalSender) maintain() {
	defer close(s.maintDone)
	ticker := time.NewTicker(s.config.Limits.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.maintStop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
			err := s.store.Validate(ctx)
			cancel()
			s.mu.Lock()
			s.dbHealthy = err == nil
			if err != nil {
				s.stats.DatabaseErrors++
			}
			s.mu.Unlock()
			if err != nil {
				continue
			}
			ctx, cancel = context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
			n, err := s.store.ReconcileOnce(ctx, s.config.Limits.ReconcileBatch)
			cancel()
			s.mu.Lock()
			s.stats.Reconciled += int64(n)
			if err != nil {
				s.stats.DatabaseErrors++
			}
			s.mu.Unlock()
		}
	}
}
func (s *TransactionalSender) StopAdmission() {
	s.mu.Lock()
	if !s.stopping {
		s.stopping = true
		close(s.sendStop)
	}
	s.mu.Unlock()
}
func (s *TransactionalSender) closeAsync() {
	s.closeOnce.Do(func() {
		go func() {
			s.mu.Lock()
			startDone := s.startDone
			s.mu.Unlock()
			<-startDone
			s.sendWG.Wait()
			close(s.maintStop)
			<-s.maintDone
			s.mu.Lock()
			s.checksClosing = true
			close(s.checks)
			s.mu.Unlock()
			s.checkWG.Wait()
			if s.backend != nil {
				s.closeErr = s.backend.Close()
			}
			s.mu.Lock()
			s.stopped = true
			s.started = false
			s.mu.Unlock()
			close(s.closeDone)
		}()
	})
}
func (s *TransactionalSender) Close(ctx context.Context) error {
	s.StopAdmission()
	s.closeAsync()
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return s.incomplete(ctx.Err())
	}
}
func (s *TransactionalSender) Wait(ctx context.Context) error {
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return s.incomplete(ctx.Err())
	}
}
func (s *TransactionalSender) Stats() TransactionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.ActiveSends = len(s.slots)
	out.QueuedChecks = len(s.checks)
	out.Stopping = s.stopping
	out.Stopped = s.stopped
	out.Ready = s.started && !s.stopping && s.dbHealthy && out.RunningChecks+out.QueuedChecks < s.config.Limits.CheckWorkers+s.config.Limits.CheckQueue
	return out
}

func (s *TransactionalSender) incomplete(cause error) error {
	stats := s.Stats()
	if stats.Stopped {
		return s.closeErr
	}
	return &transaction.ShutdownIncomplete{ActiveSends: stats.ActiveSends, RunningChecks: stats.RunningChecks, QueuedChecks: stats.QueuedChecks, RuntimePending: !stats.Stopped, Cause: cause}
}
