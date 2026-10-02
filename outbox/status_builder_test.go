package outbox

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStandardSnapshotPreservesHostContract(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	oldest := now.Add(-time.Hour)
	got, e := BuildStandardStatusSnapshot("mongo-domain-events", now, []StandardStatusCount{{State: "quarantined", Count: 2, OldestCreatedAt: &oldest}})
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(got)
	if e != nil {
		t.Fatal(e)
	}
	const want = `{"store":"mongo-domain-events","generated_at":"2026-09-23T10:00:00+08:00","buckets":[{"status":"pending","count":0,"oldest_age_seconds":0},{"status":"retry_wait","count":0,"oldest_age_seconds":0},{"status":"publishing","count":0,"oldest_age_seconds":0},{"status":"quarantined","count":2,"oldest_created_at":"2026-09-23T09:00:00+08:00","oldest_age_seconds":3600}]}`
	if string(b) != want {
		t.Fatalf("snapshot changed: %s", b)
	}
	future := now.Add(time.Minute)
	empty, e := BuildStandardStatusSnapshot("mysql", now, []StandardStatusCount{{State: "publishing", Count: 1, OldestCreatedAt: &future}, {State: "pending", OldestCreatedAt: &oldest}})
	if e != nil {
		t.Fatal(e)
	}
	if empty.Buckets[2].OldestAgeSeconds != 0 || empty.Buckets[0].OldestAgeSeconds != 3600 {
		t.Fatalf("age boundary changed: %+v", empty)
	}
}
func TestStandardSnapshotRejectsInvalidObservation(t *testing.T) {
	now := time.Now()
	zero := time.Time{}
	for _, rows := range [][]StandardStatusCount{{{State: "failed"}}, {{State: "pending", Count: -1}}, {{State: "pending", Count: 1}}, {{State: "pending", Count: 1, OldestCreatedAt: &zero}}, {{State: "pending"}, {State: "pending"}}} {
		if _, e := BuildStandardStatusSnapshot("mysql", now, rows); e == nil {
			t.Fatalf("invalid observation accepted: %+v", rows)
		}
	}
	for _, args := range []struct {
		store string
		time  time.Time
	}{{"", now}, {"mysql", time.Time{}}} {
		if _, e := BuildStandardStatusSnapshot(args.store, args.time, nil); e == nil {
			t.Fatal("missing identity/time accepted")
		}
	}
}
