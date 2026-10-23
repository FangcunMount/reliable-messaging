package rocketmq

import (
	"context"
	"crypto/sha256"

	"github.com/FangcunMount/reliable-messaging/transaction"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
)

// afterRecovery remains within the bounded recovery worker through the actual
// audit I/O. Checker itself remains read-only. No business executor is available.
func (s *TransactionalSender) afterRecovery(msg *driver.MessageView, decision driver.TransactionResolution, rpcErr error) {
	if msg == nil {
		if rpcErr != nil {
			s.mu.Lock()
			s.stats.RecoveryRPCFailed++
			s.mu.Unlock()
		} else {
			s.recoveryAuditError()
		}
		return
	}
	s.recoveryConfirmedMessage(msg.GetTopic(), []byte(msg.GetProperties()[TransactionReferenceProperty]), msg.GetBody(), decision, rpcErr)
}

func (s *TransactionalSender) recoveryConfirmedMessage(physical string, raw, body []byte, decision driver.TransactionResolution, rpcErr error) {
	if rpcErr != nil {
		s.mu.Lock()
		s.stats.RecoveryRPCFailed++
		s.mu.Unlock()
		return
	}
	var outcome transaction.FinalOutcome
	switch decision {
	case driver.COMMIT:
		outcome = transaction.FinalCommitConfirmed
	case driver.ROLLBACK:
		outcome = transaction.FinalRollbackConfirmed
	default:
		return
	}
	s.mu.Lock()
	if decision == driver.COMMIT {
		s.stats.RecoveryCommitConfirmed++
	} else {
		s.stats.RecoveryRollbackConfirmed++
	}
	s.mu.Unlock()
	ref, err := transaction.DecodeReference(raw)
	route, ok := s.routes[physical]
	if err != nil || !ok || ref.Owner != s.config.Owner || ref.PhysicalTopic != physical || ref.LogicalTopic != route.LogicalTopic || ref.RouteRevision != route.Revision || len(body) == 0 || len(body) > 262144 || sha256.Sum256(body) != ref.WireHash {
		s.recoveryAuditError()
		return
	}
	recorder, ok := s.store.(transaction.RecoveryReceiptRecorder)
	if !ok {
		s.mu.Lock()
		s.stats.RecoveryAuditUnsupported++
		s.mu.Unlock()
		return
	}
	// Use a fresh budget after the RPC; its deadline may already be exhausted.
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.CheckTimeout)
	defer cancel()
	if err := recorder.RecordRecoveryFinal(ctx, ref, outcome); err != nil || ctx.Err() != nil {
		s.recoveryAuditError()
		return
	}
	s.mu.Lock()
	s.stats.RecoveryAuditRecorded++
	s.mu.Unlock()
}
func (s *TransactionalSender) recoveryAuditError() {
	s.mu.Lock()
	s.stats.RecoveryAuditErrors++
	s.dbHealthy = false
	s.mu.Unlock()
}
