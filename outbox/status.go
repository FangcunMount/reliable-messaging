package outbox

import (
	"context"
	"time"
)

// StatusBucket describes one storage state without assigning business meaning
// to it. Hosts may expose additional legacy states during a migration window.
type StatusBucket struct {
	Status           string     `json:"status"`
	Count            int64      `json:"count"`
	OldestCreatedAt  *time.Time `json:"oldest_created_at,omitempty"`
	OldestAgeSeconds float64    `json:"oldest_age_seconds"`
}

// StatusSnapshot is a point-in-time view. It does not prove broker delivery or
// downstream business completion.
type StatusSnapshot struct {
	Store       string         `json:"store"`
	GeneratedAt time.Time      `json:"generated_at"`
	Buckets     []StatusBucket `json:"buckets"`
}

// StatusReader is implemented by a host against its own business database.
// Reading status never starts a Relay or changes message ownership.
type StatusReader interface {
	OutboxStatusSnapshot(ctx context.Context, now time.Time) (StatusSnapshot, error)
}
