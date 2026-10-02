package outbox

import (
	"fmt"
	"time"
)

var standardUnfinishedStates = []string{"pending", "retry_wait", "publishing", "quarantined"}

// StandardStatusCount is one grouped database observation. Duplicate or
// unknown states, negative counts and missing ages for nonempty rows are rejected.
type StandardStatusCount struct {
	State           string
	Count           int64
	OldestCreatedAt *time.Time
}

// BuildStandardStatusSnapshot builds a read-only observation of SDK storage
// states. Hosts supply already grouped database rows and an explicit observation
// time; this helper owns no resource, reads no database and changes no state.
// Legacy states are deliberately outside this contract.
func BuildStandardStatusSnapshot(store string, now time.Time, counts []StandardStatusCount) (StatusSnapshot, error) {
	if store == "" || now.IsZero() {
		return StatusSnapshot{}, fmt.Errorf("standard status requires store and observation time")
	}
	byState := make(map[string]StandardStatusCount, len(counts))
	for _, row := range counts {
		if !standardState(row.State) || row.Count < 0 || (row.Count > 0 && (row.OldestCreatedAt == nil || row.OldestCreatedAt.IsZero())) {
			return StatusSnapshot{}, fmt.Errorf("invalid standard outbox status row %q", row.State)
		}
		if _, exists := byState[row.State]; exists {
			return StatusSnapshot{}, fmt.Errorf("duplicate standard outbox state %q", row.State)
		}
		byState[row.State] = row
	}
	buckets := make([]StatusBucket, 0, len(standardUnfinishedStates))
	for _, state := range standardUnfinishedStates {
		row := byState[state]
		var age float64
		if row.OldestCreatedAt != nil {
			age = now.Sub(*row.OldestCreatedAt).Seconds()
			if age < 0 {
				age = 0
			}
		}
		buckets = append(buckets, StatusBucket{
			Status: state, Count: row.Count, OldestCreatedAt: row.OldestCreatedAt, OldestAgeSeconds: age,
		})
	}
	return StatusSnapshot{Store: store, GeneratedAt: now, Buckets: buckets}, nil
}

func standardState(value string) bool {
	for _, state := range standardUnfinishedStates {
		if value == state {
			return true
		}
	}
	return false
}

// IsStandardUnfinishedState reports whether a state belongs to the standard
// snapshot; it does not classify business completion or broker delivery.
func IsStandardUnfinishedState(value string) bool { return standardState(value) }
