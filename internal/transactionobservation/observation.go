// Package transactionobservation classifies metadata without consulting or
// changing business facts. Adapters stream rows; memory does not grow with history.
package transactionobservation

import (
	"context"
	"errors"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

var ErrMetadata = errors.New("transaction observation metadata unavailable or invalid")

type Branch struct {
	State   transaction.State
	Half    transaction.HalfOutcome
	Final   transaction.FinalOutcome
	HalfAt  time.Time
	FinalAt time.Time
}

type Accumulator struct {
	Snapshot transaction.ProtocolObservation
}

func Budget(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, 2*time.Second)
}

func validState(s transaction.State) bool {
	return s == transaction.Prepared || s == transaction.Committed || s == transaction.Aborted
}

func age(now, at time.Time) float64 {
	if at.IsZero() || at.After(now) {
		return 0
	}
	return now.Sub(at).Seconds()
}

func (a *Accumulator) Execution(state transaction.State, started bool, lease time.Time) error {
	if a.Snapshot.AsOf.IsZero() || !validState(state) || lease.IsZero() {
		return ErrMetadata
	}
	if state == transaction.Prepared {
		if !started {
			a.Snapshot.ReservedExecutions++
		} else {
			a.Snapshot.PreparingExecutions++
			a.Snapshot.PreparedExpiryAgeSeconds = max(a.Snapshot.PreparedExpiryAgeSeconds, age(a.Snapshot.AsOf, lease))
		}
	}
	return nil
}

func (a *Accumulator) Branch(state transaction.State, started bool, b Branch) error {
	if !validState(state) || !validState(b.State) {
		return ErrMetadata
	}
	// Mongo's older documents omit default audit fields.
	if b.Half == "" {
		b.Half = transaction.HalfNotAttempted
	}
	if b.Final == "" {
		b.Final = transaction.FinalNotAttempted
	}
	switch b.Half {
	case transaction.HalfNotAttempted, transaction.HalfPrepared, transaction.HalfRejected, transaction.HalfUnknown:
	default:
		return ErrMetadata
	}
	switch b.Final {
	case transaction.FinalNotAttempted, transaction.FinalCommitConfirmed, transaction.FinalRollbackConfirmed, transaction.FinalUnknown:
	default:
		return ErrMetadata
	}
	if b.State != state || (b.Final == transaction.FinalCommitConfirmed && state != transaction.Committed) ||
		(b.Final == transaction.FinalRollbackConfirmed && state != transaction.Aborted) {
		a.Snapshot.MetadataConflicts++
	}
	if state == transaction.Committed && b.Half != transaction.HalfPrepared {
		a.Snapshot.UnobservedCommittedBranches++
	}
	if state == transaction.Aborted && started && b.Half == transaction.HalfNotAttempted &&
		b.Final != transaction.FinalCommitConfirmed && b.Final != transaction.FinalRollbackConfirmed {
		a.Snapshot.UntrackedAbortedBranches++
	}
	if state == transaction.Prepared || (b.Half != transaction.HalfPrepared && b.Half != transaction.HalfUnknown) ||
		b.Final == transaction.FinalCommitConfirmed || b.Final == transaction.FinalRollbackConfirmed {
		return nil
	}
	a.Snapshot.ConfirmationGapBranches++
	at := b.HalfAt
	if at.IsZero() {
		at = b.FinalAt
	}
	if at.IsZero() {
		a.Snapshot.UnagedConfirmationGapBranches++
	} else {
		a.Snapshot.OldestConfirmationObservationAgeSeconds = max(a.Snapshot.OldestConfirmationObservationAgeSeconds, age(a.Snapshot.AsOf, at))
	}
	return nil
}
