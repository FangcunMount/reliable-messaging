package transaction

import (
	"context"
	"time"
)

// ProtocolObservation is a read-only metadata snapshot, not a Broker backlog or
// consumer receipt. Positive audit can be missing after a successful Broker RPC.
// Ages use the database clock. No message identity or business content is exposed.
type ProtocolObservation struct {
	AsOf                time.Time
	ReservedExecutions  int64
	PreparingExecutions int64
	// PreparedExpiryAgeSeconds is time since lease expiry, not time since send.
	// Expiry permits reconciliation; it never proves rollback.
	PreparedExpiryAgeSeconds float64
	ConfirmationGapBranches  int64
	// OldestConfirmationObservationAgeSeconds starts at the first available
	// half audit (otherwise final audit), a lower bound on exposure age.
	OldestConfirmationObservationAgeSeconds float64
	UnagedConfirmationGapBranches           int64
	// These branches may be unsent or have lost the half acknowledgement before
	// metadata persistence. They must not be counted as known Broker backlog.
	UntrackedAbortedBranches    int64
	UnobservedCommittedBranches int64
	MetadataConflicts           int64
}

// ObservationReader borrows the configured primary pool. Errors mean unavailable,
// never an empty snapshot. The caller supplies its database budget via context.
// Implementations add a two-second budget when none was supplied.
type ObservationReader interface {
	ObserveProtocol(context.Context) (ProtocolObservation, error)
}
