package transaction

import (
	"context"
	"crypto/sha256"
)

type State string

const (
	Prepared  State = "prepared"
	Committed State = "committed"
	Aborted   State = "aborted"
)

type Decision string

const (
	Unknown  Decision = "unknown"
	Commit   Decision = "commit"
	Rollback Decision = "rollback"
)

// Reference is protocol identity, not a broker message ID or business receipt.
// The host stores the fence/epoch and all branch witnesses before any half send.
type Reference struct {
	Owner, Scope, OperationID, BranchID                                string
	MessageID, LogicalTopic, PhysicalTopic, RouteRevision, WriterEpoch string
	PlanDigest, WireHash, MessageFingerprint                           [32]byte
	Fence                                                              uint64
}

func (r Reference) Valid() bool {
	return r.Fence > 0 && r.Fence <= MaxFence && required(r.Owner, r.Scope, r.OperationID, r.BranchID, r.MessageID, r.LogicalTopic, r.PhysicalTopic, r.RouteRevision, r.WriterEpoch)
}

func (p FrozenPlan) Reference(branchID string, fence uint64, epoch string) (Reference, error) {
	if !p.Valid() || fence == 0 || fence > MaxFence || !required(epoch) {
		return Reference{}, ErrInvalidPlan
	}
	for _, branch := range p.input.Branches {
		if branch.ID != branchID {
			continue
		}
		in := branch.Message.Input()
		return Reference{Owner: p.input.Owner, Scope: p.input.Scope, OperationID: p.input.OperationID, BranchID: branch.ID,
			MessageID: in.ID, LogicalTopic: in.Destination, PhysicalTopic: branch.PhysicalTopic, RouteRevision: branch.RouteRevision,
			WriterEpoch: epoch, PlanDigest: p.digest, WireHash: sha256.Sum256(branch.Wire), MessageFingerprint: branch.Message.Fingerprint(), Fence: fence}, nil
	}
	return Reference{}, ErrInvalidPlan
}

// Evidence is read from the authoritative host database, using a fresh read.
// ExecutionState is for this exact (operation, fence, writer epoch), not the
// latest attempt of the root request. Both terminal states must have been atomically written under the original
// host transaction/fence. Adapter proofs, not this struct, establish that fact.
type Evidence struct {
	Reference                   Reference
	ExecutionState, BranchState State
}

func Resolve(ref Reference, evidence *Evidence) Decision {
	if !ref.Valid() || evidence == nil || evidence.Reference != ref {
		return Unknown
	}
	if evidence.ExecutionState == Committed && evidence.BranchState == Committed {
		return Commit
	}
	if evidence.ExecutionState == Aborted && evidence.BranchState == Aborted {
		return Rollback
	}
	// Missing/in-progress/contradictory state is never an abort proof. Lease
	// expiry is deliberately absent: only a committed fence can prove abortion.
	return Unknown
}

// Checker does not rerun business logic. Database errors/timeouts map to Unknown.
type Checker interface {
	Check(context.Context, Reference) Decision
}

type HalfOutcome string

const (
	HalfNotAttempted HalfOutcome = "not_attempted"
	HalfPrepared     HalfOutcome = "prepared"
	HalfRejected     HalfOutcome = "rejected"
	HalfUnknown      HalfOutcome = "unknown"
)

type LocalOutcome string

const (
	LocalNotStarted LocalOutcome = "not_started"
	LocalCommitted  LocalOutcome = "committed"
	LocalAborted    LocalOutcome = "aborted"
	LocalUnknown    LocalOutcome = "unknown"
)

type FinalOutcome string

const (
	FinalNotAttempted      FinalOutcome = "not_attempted"
	FinalCommitConfirmed   FinalOutcome = "commit_confirmed"
	FinalRollbackConfirmed FinalOutcome = "rollback_confirmed"
	FinalUnknown           FinalOutcome = "unknown"
)

// Result keeps the three producer phases separate. Even LocalCommitted and
// FinalCommitConfirmed do not report consumer effects or business receipts.
type BranchResult struct {
	Reference Reference
	Half      HalfOutcome
	Final     FinalOutcome
}
type Result struct {
	Local    LocalOutcome
	Branches []BranchResult
}
