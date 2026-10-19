package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

type transactionOperation struct {
	mu           sync.Mutex
	result       transaction.Result
	err          error
	localRunning bool
	done         chan struct{}
}

func (o *transactionOperation) snapshot() (transaction.Result, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r := o.result
	r.Branches = append([]transaction.BranchResult(nil), r.Branches...)
	if o.localRunning && r.Local == transaction.LocalNotStarted {
		r.Local = transaction.LocalUnknown
	}
	return r, o.err
}
func (o *transactionOperation) update(fn func(*transaction.Result)) {
	o.mu.Lock()
	fn(&o.result)
	o.mu.Unlock()
}

// Execute consumes the reservation capability once. A canceled caller receives
// a snapshot; the bounded owned operation retains its slot until actual UOW,
// protocol RPCs and audit calls finish. An error never authorizes business replay.
func (s *TransactionalSender) Execute(ctx context.Context, e transaction.Execution, local transaction.LocalExecutor) (transaction.Result, error) {
	initial := transaction.InitialResult(e)
	if !e.Valid() || local == nil || e.Plan.Input().Owner != s.config.Owner || e.WriterEpoch != s.store.Binding().WriterEpoch || len(e.Plan.Input().Branches) > s.config.Limits.Branches {
		return initial, transaction.ErrInvalidPlan
	}
	if err := s.validatePlan(e.Plan); err != nil {
		return initial, err
	}
	if err := s.admit(ctx); err != nil {
		return initial, err
	}
	op := &transactionOperation{result: initial, done: make(chan struct{})}
	go func() {
		defer func() { <-s.slots; s.sendWG.Done(); close(op.done) }()
		err := s.execute(ctx, e, local, op)
		result, _ := op.snapshot()
		s.recordResult(result)
		op.mu.Lock()
		op.err = err
		op.mu.Unlock()
	}()
	select {
	case <-op.done:
		return op.snapshot()
	case <-ctx.Done():
		r, _ := op.snapshot()
		return r, ctx.Err()
	}
}

// Reserve freezes the winner's original plan before any Broker I/O. Duplicate
// reservations return the observed original plan with no execution capability.
func (s *TransactionalSender) Reserve(ctx context.Context, request transaction.Request, candidate transaction.FrozenPlan) (transaction.Reservation, error) {
	if err := s.validatePlan(candidate); err != nil {
		return transaction.Reservation{}, err
	}
	if err := request.Match(candidate); err != nil {
		return transaction.Reservation{}, err
	}
	if err := s.admit(ctx); err != nil {
		return transaction.Reservation{}, err
	}
	defer func() { <-s.slots; s.sendWG.Done() }()
	r, err := s.store.Reserve(ctx, request, candidate, transaction.ReserveOptions{Lease: s.config.Limits.Lease})
	s.storeError(err)
	return r, err
}
func (s *TransactionalSender) validatePlan(p transaction.FrozenPlan) error {
	if !p.Valid() || p.Input().Owner != s.config.Owner || len(p.Input().Branches) > s.config.Limits.Branches {
		return transaction.ErrInvalidPlan
	}
	for _, b := range p.Input().Branches {
		r, ok := s.routes[b.PhysicalTopic]
		if !ok || r.LogicalTopic != b.Message.Input().Destination || r.Revision != b.RouteRevision || len(b.Wire) > 262144 {
			return transaction.ErrBinding
		}
	}
	return nil
}
func (s *TransactionalSender) admit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.slots <- struct{}{}:
	case <-s.sendStop:
		return transaction.ErrShutdownIncomplete
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	ready := s.started && !s.stopping && s.dbHealthy && s.stats.RunningChecks+len(s.checks) < s.config.Limits.CheckWorkers+s.config.Limits.CheckQueue
	if !ready {
		s.mu.Unlock()
		<-s.slots
		return errors.New("transaction sender admission unavailable")
	}
	s.sendWG.Add(1)
	s.mu.Unlock()
	return nil
}

func (s *TransactionalSender) storeError(err error) {
	if err == nil || errors.Is(err, transaction.ErrStaleExecution) {
		return
	}
	s.mu.Lock()
	s.stats.DatabaseErrors++
	s.dbHealthy = false
	s.mu.Unlock()
}
func (s *TransactionalSender) execute(ctx context.Context, e transaction.Execution, local transaction.LocalExecutor, op *transactionOperation) (err error) {
	defer func() {
		if recover() != nil {
			op.update(func(r *transaction.Result) {
				if r.Local == transaction.LocalNotStarted {
					r.Local = transaction.LocalUnknown
				}
			})
			err = errors.New("transaction operation panicked; authoritative recovery required")
		}
	}()
	admitCtx, cancel := context.WithTimeout(ctx, s.config.Limits.CheckTimeout)
	err = s.store.BeginPrepare(admitCtx, e)
	cancel()
	if err != nil {
		s.storeError(err)
		if errors.Is(err, transaction.ErrStaleExecution) {
			known, proofErr := s.localProof(e)
			op.update(func(r *transaction.Result) { r.Local = known })
			return errors.Join(err, proofErr)
		}
		return err
	}
	branches, refs := e.Plan.Input().Branches, e.References()
	handles := make([]transactionHandle, len(branches))
	for i, b := range branches {
		if err = ctx.Err(); err != nil {
			return errors.Join(err, s.abortUnstarted(e, handles, op))
		}
		op.update(func(r *transaction.Result) { r.Branches[i].Half = transaction.HalfUnknown })
		callCtx, stop := context.WithTimeout(ctx, s.config.Limits.RPCTimeout)
		h, handle, sendErr := s.backend.Prepare(callCtx, refs[i], b.Wire)
		stop()
		if !h.Valid() {
			h = transaction.HalfReceipt{Outcome: transaction.HalfUnknown, ErrorCode: "invalid_half_receipt"}
			if sendErr == nil {
				sendErr = transaction.ErrInvalidPlan
			}
		}
		handles[i] = handle
		op.update(func(r *transaction.Result) {
			r.Branches[i].Half = h.Outcome
			r.Branches[i].BrokerMessageID = h.BrokerMessageID
			r.Branches[i].BrokerTransactionID = h.BrokerTransactionID
			r.Branches[i].ErrorCode = h.ErrorCode
		})
		auditCtx, auditCancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
		auditErr := s.store.RecordHalf(auditCtx, e, refs[i], h)
		auditCancel()
		s.storeError(auditErr)
		if auditErr != nil {
			op.update(func(r *transaction.Result) { r.Branches[i].ErrorCode = "half_audit_failed" })
		}
		if sendErr != nil || auditErr != nil || h.Outcome != transaction.HalfPrepared || handle == nil {
			return errors.Join(sendErr, auditErr, errors.New("all half messages must be confirmed before local execution"), s.abortUnstarted(e, handles, op))
		}
	}
	if err = ctx.Err(); err != nil {
		return errors.Join(err, s.abortUnstarted(e, handles, op))
	}
	op.mu.Lock()
	op.localRunning = true
	op.mu.Unlock()
	// Expiration only cancels the cooperative UOW context. It is not a persisted
	// Rollback decision; the original guard/fence remains authoritative.
	localCtx, localCancel := context.WithDeadline(ctx, e.LeaseUntil)
	reported, localErr := local(localCtx, e)
	localCancel()
	known, proofErr := s.localProof(e)
	s.storeError(proofErr)
	if known == transaction.LocalUnknown && reported == transaction.LocalAborted {
		abortCtx, abortCancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
		aborted, abortErr := s.store.Abort(abortCtx, e)
		abortCancel()
		s.storeError(abortErr)
		if aborted && abortErr == nil {
			known = transaction.LocalAborted
		}
		proofErr = errors.Join(proofErr, abortErr)
	}
	op.mu.Lock()
	op.localRunning = false
	op.result.Local = known
	op.mu.Unlock()
	if known == transaction.LocalUnknown {
		return errors.Join(localErr, proofErr, errors.New("local transaction outcome unknown; do not rerun business"))
	}
	decision := transaction.Commit
	if known == transaction.LocalAborted {
		decision = transaction.Rollback
	}
	return errors.Join(localErr, proofErr, s.finalize(e, handles, decision, op))
}
func (s *TransactionalSender) localProof(e transaction.Execution) (transaction.LocalOutcome, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
	defer cancel()
	refs := e.References()
	evidence := make([]*transaction.Evidence, len(refs))
	for i, ref := range refs {
		value, err := s.store.Evidence(ctx, ref)
		if err != nil {
			return transaction.LocalUnknown, err
		}
		evidence[i] = value
	}
	if ctx.Err() != nil {
		return transaction.LocalUnknown, ctx.Err()
	}
	return transaction.DecideLocal(refs, evidence), nil
}
func (s *TransactionalSender) abortUnstarted(e transaction.Execution, handles []transactionHandle, op *transactionOperation) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
	aborted, err := s.store.Abort(ctx, e)
	cancel()
	s.storeError(err)
	if err != nil {
		return err
	}
	if !aborted {
		known, proofErr := s.localProof(e)
		if proofErr != nil || known != transaction.LocalAborted {
			return errors.Join(proofErr, errors.New("unstarted execution has no durable abort proof"))
		}
	}
	return s.finalize(e, handles, transaction.Rollback, op)
}
func (s *TransactionalSender) finalize(e transaction.Execution, handles []transactionHandle, decision transaction.Decision, op *transactionOperation) error {
	var combined error
	refs := e.References()
	for i, h := range handles {
		if h == nil {
			continue
		}
		op.update(func(r *transaction.Result) { r.Branches[i].Final = transaction.FinalUnknown })
		ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.RPCTimeout)
		err := h.Finalize(ctx, decision)
		cancel()
		outcome, code := transaction.FinalUnknown, "final_rpc_unknown"
		if err == nil {
			code = ""
			if decision == transaction.Commit {
				outcome = transaction.FinalCommitConfirmed
			} else {
				outcome = transaction.FinalRollbackConfirmed
			}
		}
		op.update(func(r *transaction.Result) {
			r.Branches[i].Final = outcome
			if code != "" {
				r.Branches[i].ErrorCode = code
			}
		})
		ctx, cancel = context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
		auditErr := s.store.RecordFinal(ctx, e, refs[i], outcome, code)
		cancel()
		s.storeError(auditErr)
		if auditErr != nil {
			op.update(func(r *transaction.Result) { r.Branches[i].ErrorCode = "final_audit_failed" })
		}
		if err != nil || auditErr != nil {
			combined = errors.Join(combined, fmt.Errorf("branch finalization: %w", errors.Join(err, auditErr)))
		}
	}
	return combined
}

// Counters describe completed SDK result observations, not distinct business
// effects or consumer completion. No IDs or caller-controlled labels are used.
func (s *TransactionalSender) recordResult(result transaction.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.CompletedResults++
	switch result.Local {
	case transaction.LocalCommitted:
		s.stats.LocalCommittedResults++
	case transaction.LocalAborted:
		s.stats.LocalAbortedResults++
	case transaction.LocalUnknown:
		s.stats.LocalUnknownResults++
	}
	for _, b := range result.Branches {
		switch b.Half {
		case transaction.HalfPrepared:
			s.stats.HalfPreparedResults++
		case transaction.HalfRejected:
			s.stats.HalfRejectedResults++
		case transaction.HalfUnknown:
			s.stats.HalfUnknownResults++
		}
		switch b.Final {
		case transaction.FinalCommitConfirmed:
			s.stats.FinalCommitResults++
		case transaction.FinalRollbackConfirmed:
			s.stats.FinalRollbackResults++
		case transaction.FinalUnknown:
			s.stats.FinalUnknownResults++
		}
	}
}
