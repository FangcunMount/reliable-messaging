//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	mongostore "github.com/FangcunMount/reliable-messaging/storage/mongo"
	mysqlstore "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const fairScanProducer = "m4-fair-scan"

func fairScanMessage(t *testing.T, id string) message.Message {
	t.Helper()
	scope := "org-501"
	if id == "expired-lease" || (len(id) >= 4 && id[:4] == "hot-") {
		scope = "org-502"
	}
	m, err := message.New(message.Input{Producer: fairScanProducer, ID: id,
		Destination: "events", EventType: "created", SchemaVersion: "1",
		Scope: scope, ContentType: "application/json",
		OccurredAt: "2026-09-24T00:00:00Z", Payload: []byte(fmt.Sprintf(`{"id":%q}`, id))})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// The retry and expired lease precede a mixed-scope backlog of newer due work.
// One more hot row arrives between claims. This is a storage scheduling proof,
// not a broker or service capacity test.
func proveFairScan(t *testing.T, ctx context.Context, store outbox.Store, addHot func(string, time.Time) error) {
	t.Helper()
	for i := 0; i < 64; i++ {
		if err := addHot(fmt.Sprintf("hot-%03d", i), time.Now().UTC().Add(-5*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	claimOne := func(want string) {
		t.Helper()
		claims, err := store.ClaimDue(ctx, 1, time.Minute)
		if err != nil || len(claims) != 1 {
			t.Fatalf("claim %s: len=%d err=%v", want, len(claims), err)
		}
		if got := claims[0].Message.Input().ID; got != want {
			t.Fatalf("older due identity starved: want=%s got=%s", want, got)
		}
		if err := store.Confirm(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
	}
	claimOne("retry-due")
	if err := addHot("hot-after-first-claim", time.Now().UTC().Add(-5*time.Second)); err != nil {
		t.Fatal(err)
	}
	claimOne("expired-lease")
	claims, err := store.ClaimDue(ctx, 8, time.Minute)
	if err != nil || len(claims) != 8 {
		t.Fatalf("hot work did not remain claimable: len=%d err=%v", len(claims), err)
	}
	seen := make(map[string]bool, len(claims))
	for _, claim := range claims {
		id := claim.Message.Input().ID
		if len(id) < 4 || id[:4] != "hot-" || seen[id] {
			t.Fatalf("unexpected or duplicate hot claim %q", id)
		}
		seen[id] = true
		if err := store.Confirm(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("retry and expired lease confirmed ahead of mixed-scope hot backlog in %s", time.Since(start))
}

func TestMySQLFairScanWithRetryAndExpiredLease(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated MySQL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, mysqlstore.Schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM rm_outbox WHERE producer=?", fairScanProducer); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DELETE FROM rm_outbox WHERE producer=?", fairScanProducer)
	insert := func(id, state string, due time.Time) error {
		m := fairScanMessage(t, id)
		in := m.Input()
		fingerprint := m.Fingerprint()
		var token, lease any
		attempts, failures := 0, 0
		if state == "publishing" {
			token, lease, attempts = "expired-original-token", due.UTC().Format("2006-01-02 15:04:05.000000"), 1
		}
		if state == "retry_wait" {
			attempts, failures = 1, 1
		}
		_, err := db.ExecContext(ctx, `INSERT INTO rm_outbox
  (producer,message_id,destination,event_type,schema_version,scope,content_type,occurred_at,payload,fingerprint,
   state,next_attempt_at,claim_token,lease_until,version,attempt_count,failure_count)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			in.Producer, in.ID, in.Destination, in.EventType, in.SchemaVersion, in.Scope, in.ContentType,
			in.OccurredAt, in.Payload, fingerprint[:], state, due.UTC().Format("2006-01-02 15:04:05.000000"),
			token, lease, 1, attempts, failures)
		return err
	}
	if err := insert("retry-due", "retry_wait", time.Now().UTC().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := insert("expired-lease", "publishing", time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	store, err := mysqlstore.New(db)
	if err != nil {
		t.Fatal(err)
	}
	proveFairScan(t, ctx, store, func(id string, due time.Time) error { return insert(id, "pending", due) })
}

func TestMongoFairScanWithRetryAndExpiredLease(t *testing.T) {
	uri := os.Getenv("RM_TEST_MONGO_URI")
	if uri == "" {
		t.Fatal("isolated Mongo URI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	db := client.Database("rm_m4_fair_scan")
	defer db.Drop(context.Background())
	collection := db.Collection("outbox")
	if _, err := collection.Indexes().CreateMany(ctx, mongostore.Indexes()); err != nil {
		t.Fatal(err)
	}
	insert := func(id, state string, due time.Time) error {
		m := fairScanMessage(t, id)
		in := m.Input()
		fingerprint := m.Fingerprint()
		now := time.Now().UTC()
		doc := bson.M{
			"_id":      bson.D{{Key: "producer", Value: in.Producer}, {Key: "message_id", Value: in.ID}, {Key: "destination", Value: in.Destination}},
			"producer": in.Producer, "message_id": in.ID, "destination": in.Destination,
			"event_type": in.EventType, "schema_version": in.SchemaVersion, "scope": in.Scope,
			"content_type": in.ContentType, "occurred_at": in.OccurredAt, "payload": in.Payload,
			"fingerprint": fingerprint[:], "state": state, "next_attempt_at": due,
			"version": int64(1), "created_at": now, "updated_at": now,
		}
		if state == "publishing" {
			doc["claim_token"], doc["lease_until"], doc["attempt_count"] = "expired-original-token", due, int64(1)
		}
		if state == "retry_wait" {
			doc["attempt_count"], doc["failure_count"] = int64(1), int64(1)
		}
		_, err := collection.InsertOne(ctx, doc)
		return err
	}
	if err := insert("retry-due", "retry_wait", time.Now().UTC().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := insert("expired-lease", "publishing", time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	store, err := mongostore.New(collection)
	if err != nil {
		t.Fatal(err)
	}
	proveFairScan(t, ctx, store, func(id string, due time.Time) error { return insert(id, "pending", due) })
}
