//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	mongostore "github.com/FangcunMount/reliable-messaging/storage/mongo"
	mysqlstore "github.com/FangcunMount/reliable-messaging/storage/mysql"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const backlogDue, backlogFuture, backlogBatch = 600, 5400, 8

func backlogMessage(t *testing.T, i int) message.Message {
	t.Helper()
	id := fmt.Sprintf("backlog-%04d", i)
	scope := "org-501"
	if i%2 != 0 {
		scope = "org-502"
	}
	m, err := message.New(message.Input{Producer: "backlog-profile", ID: id,
		Destination: "events", EventType: "created", SchemaVersion: "1", Scope: scope,
		ContentType: "application/json", OccurredAt: "2026-09-23T00:00:00Z", Payload: []byte(`{"i":` + fmt.Sprint(i) + `}`)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func backlogDueAt(now time.Time, i int) time.Time {
	if i < backlogDue {
		return now.Add(-time.Minute).Add(time.Duration(i) * time.Millisecond)
	}
	return now.Add(time.Hour)
}

func checkBacklogClaims(t *testing.T, seen map[string]bool, ids []string) {
	t.Helper()
	for _, id := range ids {
		if !strings.HasPrefix(id, "backlog-") {
			t.Fatalf("unexpected message identity: %s", id)
		}
		index, err := strconv.Atoi(strings.TrimPrefix(id, "backlog-"))
		if err != nil {
			t.Fatal(err)
		}
		if index >= backlogDue || seen[id] {
			t.Fatalf("future or duplicate message claimed: %s", id)
		}
		seen[id] = true
	}
}

func TestMySQLBoundedBacklogScan(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated MySQL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, mysqlstore.Schema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "DELETE FROM rm_outbox WHERE producer='backlog-profile'"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DELETE FROM rm_outbox WHERE producer='backlog-profile'")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO rm_outbox
  (producer,message_id,destination,event_type,schema_version,scope,content_type,occurred_at,payload,fingerprint,next_attempt_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i := 0; i < backlogDue+backlogFuture; i++ {
		m := backlogMessage(t, i)
		in := m.Input()
		fingerprint := m.Fingerprint()
		if _, err = stmt.ExecContext(ctx, in.Producer, in.ID, in.Destination, in.EventType, in.SchemaVersion,
			in.Scope, in.ContentType, in.OccurredAt, in.Payload, fingerprint[:], backlogDueAt(now, i).Format("2006-01-02 15:04:05.000000")); err != nil {
			t.Fatal(err)
		}
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	s, err := mysqlstore.New(db)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, backlogDue)
	start := time.Now()
	var firstBatch time.Duration
	for len(seen) < backlogDue {
		claims, claimErr := s.ClaimDue(ctx, backlogBatch, time.Minute)
		if claimErr != nil || len(claims) == 0 || len(claims) > backlogBatch {
			t.Fatalf("bounded MySQL claim: len=%d err=%v", len(claims), claimErr)
		}
		if firstBatch == 0 {
			firstBatch = time.Since(start)
		}
		ids := make([]string, 0, len(claims))
		for _, claim := range claims {
			ids = append(ids, claim.Message.Input().ID)
			if err = s.Confirm(ctx, claim); err != nil {
				t.Fatal(err)
			}
		}
		checkBacklogClaims(t, seen, ids)
	}
	more, err := s.ClaimDue(ctx, backlogBatch, time.Minute)
	if err != nil || len(more) != 0 {
		t.Fatalf("future MySQL records claimed: len=%d err=%v", len(more), err)
	}
	t.Logf("MySQL backlog: due=%d future=%d batch=%d first=%s drain=%s", backlogDue, backlogFuture, backlogBatch, firstBatch, time.Since(start))
}

func TestMongoBoundedBacklogScan(t *testing.T) {
	uri := os.Getenv("RM_TEST_MONGO_URI")
	if uri == "" {
		t.Fatal("isolated Mongo URI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	db := client.Database("rm_backlog_profile")
	defer db.Drop(context.Background())
	collection := db.Collection("outbox")
	if _, err = collection.Indexes().CreateMany(ctx, mongostore.Indexes()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	docs := make([]interface{}, 0, backlogDue+backlogFuture)
	for i := 0; i < backlogDue+backlogFuture; i++ {
		m := backlogMessage(t, i)
		in := m.Input()
		fingerprint := m.Fingerprint()
		docs = append(docs, bson.M{
			"_id":      bson.D{{Key: "producer", Value: in.Producer}, {Key: "message_id", Value: in.ID}, {Key: "destination", Value: in.Destination}},
			"producer": in.Producer, "message_id": in.ID, "destination": in.Destination,
			"event_type": in.EventType, "schema_version": in.SchemaVersion, "scope": in.Scope,
			"content_type": in.ContentType, "occurred_at": in.OccurredAt, "payload": in.Payload,
			"fingerprint": fingerprint[:], "state": "pending", "next_attempt_at": backlogDueAt(now, i),
			"version": int64(0), "created_at": now, "updated_at": now,
		})
	}
	if _, err = collection.InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}
	s, err := mongostore.New(collection)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, backlogDue)
	start := time.Now()
	var firstBatch time.Duration
	for len(seen) < backlogDue {
		claims, claimErr := s.ClaimDue(ctx, backlogBatch, time.Minute)
		if claimErr != nil || len(claims) == 0 || len(claims) > backlogBatch {
			t.Fatalf("bounded Mongo claim: len=%d err=%v", len(claims), claimErr)
		}
		if firstBatch == 0 {
			firstBatch = time.Since(start)
		}
		ids := make([]string, 0, len(claims))
		for _, claim := range claims {
			ids = append(ids, claim.Message.Input().ID)
			if err = s.Confirm(ctx, claim); err != nil {
				t.Fatal(err)
			}
		}
		checkBacklogClaims(t, seen, ids)
	}
	more, err := s.ClaimDue(ctx, backlogBatch, time.Minute)
	if err != nil || len(more) != 0 {
		t.Fatalf("future Mongo records claimed: len=%d err=%v", len(more), err)
	}
	t.Logf("Mongo backlog: due=%d future=%d batch=%d first=%s drain=%s", backlogDue, backlogFuture, backlogBatch, firstBatch, time.Since(start))
}
