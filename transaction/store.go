package transaction

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"
)

var (
	ErrStaleExecution      = errors.New("transaction execution is not owned or no longer prepared")
	ErrTransactionRequired = errors.New("active original root transaction required")
	ErrBinding             = errors.New("transaction owner, store identity or epoch mismatch")
	ErrSchema              = errors.New("transaction schema version mismatch")
	ErrNotFound            = errors.New("transaction request not found")
	ErrShutdownIncomplete  = errors.New("transaction runtime shutdown incomplete")
)

type Request struct {
	Owner, Scope, Key string
	InputHash         [32]byte
}

func RequestFor(p FrozenPlan) Request {
	in := p.Input()
	return Request{in.Owner, in.Scope, in.IdempotencyKey, in.InputHash}
}
func (r Request) Match(p FrozenPlan) error {
	return p.MatchRequest(r.Owner, r.Scope, r.Key, r.InputHash)
}
func (r Request) Valid() bool { return required(r.Owner, r.Scope, r.Key) }

// KeyDigest is an index key only. Stores must also compare the original fields.
func (r Request) KeyDigest() [32]byte {
	h := sha256.New()
	h.Write([]byte("rm-tx-request-v1\x00"))
	for _, s := range []string{r.Owner, r.Scope, r.Key} {
		field(h, []byte(s))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

type Binding struct{ Owner, StoreID, WriterEpoch string }

func (b Binding) Valid() bool { return required(b.Owner, b.StoreID, b.WriterEpoch) }

type ReserveOptions struct{ Lease time.Duration }

func (o ReserveOptions) Valid() bool { return o.Lease >= time.Millisecond && o.Lease <= 24*time.Hour }

// Execution includes a capability held only by the reservation winner. It is
// not included in broker properties or exported in normal logs.
type Execution struct {
	Plan        FrozenPlan
	Fence       uint64
	WriterEpoch string
	Token       [32]byte
	LeaseUntil  time.Time
}

func (e Execution) Valid() bool {
	return e.Plan.Valid() && e.Fence > 0 && e.Fence <= MaxFence && required(e.WriterEpoch) && e.Token != [32]byte{} && !e.LeaseUntil.IsZero()
}
func (e Execution) References() []Reference {
	if !e.Plan.Valid() || e.Fence == 0 || e.Fence > MaxFence || !required(e.WriterEpoch) {
		return nil
	}
	refs := make([]Reference, 0, len(e.Plan.Input().Branches))
	for _, b := range e.Plan.Input().Branches {
		r, _ := e.Plan.Reference(b.ID, e.Fence, e.WriterEpoch)
		refs = append(refs, r)
	}
	return refs
}

type Reservation struct {
	Execution Execution
	Acquired  bool
	State     State
}

// Store owns only protocol transactions. Bind on an adapter borrows the host's
// ORIGINAL transaction for Guard and CommitWitness; these methods do not commit it.
type Store interface {
	Lookup(context.Context, Request) (Reservation, error)
	Reserve(context.Context, Request, FrozenPlan, ReserveOptions) (Reservation, error)
	// BeginPrepare consumes the winner capability once, BEFORE Broker I/O. A
	// repeated Execute cannot create another half-message set for this fence.
	BeginPrepare(context.Context, Execution) error
	Evidence(context.Context, Reference) (*Evidence, error)
	Abort(context.Context, Execution) (bool, error)
	ReconcileOnce(context.Context, int) (int, error)
}

// RetryAuthorization and ReplanAuthorization are explicit host decisions, never
// made by Checker. Stores atomically require the exact prior execution to be
// Aborted, preserve it, and CAS the root request before granting a new winner.
type RetryAuthorization struct{ Previous Execution }
type ReplanAuthorization struct {
	Previous  Execution
	Candidate FrozenPlan
}
type AuthorizedStore interface {
	Store
	Retry(context.Context, RetryAuthorization, ReserveOptions) (Reservation, error)
	Replan(context.Context, ReplanAuthorization, ReserveOptions) (Reservation, error)
}
type Guard interface{ CommitWitness(context.Context) error }

// LocalExecutor invokes the host-owned UOW once. Committed means the host
// received the root commit ACK, not merely that its callback returned nil.
// Commit ACK uncertainty must be LocalUnknown. A subsequent fresh evidence read
// can establish a terminal outcome without re-executing the callback.
type LocalExecutor func(context.Context, Execution) (LocalOutcome, error)
type Sender interface {
	Execute(context.Context, Execution, LocalExecutor) (Result, error)
}

// AuthorizedSender exposes host decisions without teaching a sender or Checker
// when business may retry. Existing Sender implementations remain unchanged.
type AuthorizedSender interface {
	Sender
	Retry(context.Context, RetryAuthorization) (Reservation, error)
	Replan(context.Context, ReplanAuthorization) (Reservation, error)
}

type StoreChecker struct{ Store Store }

func (c StoreChecker) Check(ctx context.Context, ref Reference) Decision {
	if c.Store == nil || !ref.Valid() {
		return Unknown
	}
	e, err := c.Store.Evidence(ctx, ref)
	if err != nil {
		return Unknown
	}
	return Resolve(ref, e)
}

// Classify returns Unknown for transport errors unless rejection was positively
// established. A timeout, cancellation or connection reset is not a rejection.
func ClassifyHalf(confirmed, rejected bool) HalfOutcome {
	if confirmed && !rejected {
		return HalfPrepared
	}
	if rejected && !confirmed {
		return HalfRejected
	}
	return HalfUnknown
}

// DecideLocal requires every exact branch witness; contradictory/missing facts
// cannot establish commitment or abortion. A callback return is not evidence.
func DecideLocal(refs []Reference, evidence []*Evidence) LocalOutcome {
	if len(refs) == 0 || len(refs) != len(evidence) {
		return LocalUnknown
	}
	decision := Resolve(refs[0], evidence[0])
	if decision == Unknown {
		return LocalUnknown
	}
	for i := 1; i < len(refs); i++ {
		if refs[i].Owner != refs[0].Owner || refs[i].Scope != refs[0].Scope || refs[i].OperationID != refs[0].OperationID || refs[i].Fence != refs[0].Fence || refs[i].WriterEpoch != refs[0].WriterEpoch || refs[i].PlanDigest != refs[0].PlanDigest {
			return LocalUnknown
		}
		if Resolve(refs[i], evidence[i]) != decision {
			return LocalUnknown
		}
	}
	if decision == Commit {
		return LocalCommitted
	}
	return LocalAborted
}

func InitialResult(e Execution) Result {
	r := Result{Local: LocalNotStarted}
	for _, ref := range e.References() {
		r.Branches = append(r.Branches, BranchResult{Reference: ref, Half: HalfNotAttempted, Final: FinalNotAttempted})
	}
	return r
}

// ValidateAdvance checks an explicit host authorization against the exact old
// execution. Storage adapters must obtain evidence under their root CAS/lock;
// caller-supplied states are never a substitute for that authoritative read.
func ValidateAdvance(previous Execution, candidate FrozenPlan, evidence []*Evidence, replan bool) error {
	if previous.Fence == 0 || previous.Fence >= MaxFence || !previous.Plan.Valid() || !candidate.Valid() {
		return ErrInvalidPlan
	}
	if err := RequestFor(previous.Plan).Match(candidate); err != nil {
		return err
	}
	if DecideLocal(previous.References(), evidence) != LocalAborted {
		return ErrStaleExecution
	}
	if replan {
		if candidate.Input().OperationID == previous.Plan.Input().OperationID {
			return ErrInvalidPlan
		}
	} else if candidate.Digest() != previous.Plan.Digest() {
		return ErrInvalidPlan
	}
	return nil
}
