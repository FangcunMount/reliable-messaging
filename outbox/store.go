// Package outbox contains provisional delivery and lease contracts.
package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
)

var (
	ErrConflict   = errors.New("same delivery identity has different immutable content")
	ErrStaleClaim = errors.New("claim expired or superseded")
)

type Claim struct {
	// RecordID is an opaque storage key. Relay must not parse or reconstruct it.
	RecordID   string
	Token      string
	Version    uint64
	Attempts   uint64
	LeaseUntil time.Time
	Message    message.Message
}

// Implementations use their database clock for lease validity. Mutations must
// match record, state, token AND version, and reject expired claims. Retry takes
// a positive delay from the database clock at the conditional write, rounded up
// to storage precision; it must not depend on the Relay host wall clock.
type Store interface {
	ClaimDue(context.Context, int, time.Duration) ([]Claim, error)
	Confirm(context.Context, Claim) error
	Retry(context.Context, Claim, time.Duration, string) error
	Quarantine(context.Context, Claim, string) error
}
