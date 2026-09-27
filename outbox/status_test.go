package outbox

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatusSnapshotJSONContract(t *testing.T) {
	zone := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 9, 28, 10, 0, 0, 0, zone)
	oldest := now.Add(-90 * time.Second)
	snapshot := StatusSnapshot{
		Store: "mongo-domain-events", GeneratedAt: now,
		Buckets: []StatusBucket{
			{Status: "pending", Count: 2, OldestCreatedAt: &oldest, OldestAgeSeconds: 90},
			{Status: "retry_wait"},
		},
	}
	got, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"store":"mongo-domain-events","generated_at":"2026-09-28T10:00:00+08:00","buckets":[{"status":"pending","count":2,"oldest_created_at":"2026-09-28T09:58:30+08:00","oldest_age_seconds":90},{"status":"retry_wait","count":0,"oldest_age_seconds":0}]}`
	if string(got) != want {
		t.Fatalf("status JSON contract changed:\n got %s\nwant %s", got, want)
	}
}
