//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
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

func mixedDueMessage(t *testing.T, id, scope string) message.Message {
	t.Helper()
	m, err := message.New(message.Input{Producer: "mixed-due-probe", ID: id,
		Destination: "events", EventType: "created", SchemaVersion: "1", Scope: scope,
		ContentType: "application/json", OccurredAt: "2026-09-23T00:00:00Z", Payload: []byte(`{"id":"` + id + `"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMixedDueMySQLOrder(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated MySQL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, mysqlstore.Schema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "DELETE FROM rm_outbox WHERE producer='mixed-due-probe'"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DELETE FROM rm_outbox WHERE producer='mixed-due-probe'")
	appendMessage := func(id, scope string, due time.Time) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		a, err := mysqlstore.Bind(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err = a.Append(ctx, mixedDueMessage(t, id, scope), due); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	appendMessage("expired", "org-501", time.Now().Add(-2*time.Hour))
	s, err := mysqlstore.New(db)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimDue(ctx, 1, time.Hour)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial claim: %v %d", err, len(first))
	}
	var dueAndLeaseDiff int64
	if err = db.QueryRowContext(ctx, "SELECT TIMESTAMPDIFF(MICROSECOND,next_attempt_at,lease_until) FROM rm_outbox WHERE message_id='expired' AND producer='mixed-due-probe'").Scan(&dueAndLeaseDiff); err != nil {
		t.Fatal(err)
	}
	if dueAndLeaseDiff != 0 {
		t.Fatalf("publishing due time differs from lease expiry by %d microseconds", dueAndLeaseDiff)
	}
	if _, err = db.ExecContext(ctx, "UPDATE rm_outbox SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND,next_attempt_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE message_id='expired' AND producer='mixed-due-probe'"); err != nil {
		t.Fatal(err)
	}
	appendMessage("pending", "org-502", time.Now().Add(-time.Hour))
	next, err := s.ClaimDue(ctx, 1, time.Hour)
	if err != nil || len(next) != 1 {
		t.Fatalf("mixed claim: %v %d", err, len(next))
	}
	t.Logf("MySQL first mixed claim=%s; pending actual due=-1h, expired lease due=-1s", next[0].Message.Input().ID)
	if next[0].Message.Input().ID != "pending" {
		t.Errorf("actual due order violated: got %s", next[0].Message.Input().ID)
	}
}

func TestMixedDueMongoOrder(t *testing.T) {
	uri := os.Getenv("RM_TEST_MONGO_URI")
	if uri == "" {
		t.Fatal("isolated Mongo URI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	db := client.Database("rm_mixed_due_probe")
	defer db.Drop(context.Background())
	collection := db.Collection("outbox")
	if _, err = collection.Indexes().CreateMany(ctx, mongostore.Indexes()); err != nil {
		t.Fatal(err)
	}
	appendMessage := func(id, scope string, due time.Time) {
		t.Helper()
		session, err := client.StartSession()
		if err != nil {
			t.Fatal(err)
		}
		defer session.EndSession(ctx)
		_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
			a, err := mongostore.Bind(sc, collection)
			if err != nil {
				return nil, err
			}
			return nil, a.Append(mixedDueMessage(t, id, scope), due)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	appendMessage("expired", "org-501", time.Now().Add(-2*time.Hour))
	s, err := mongostore.New(collection)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimDue(ctx, 1, time.Hour)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial claim: %v %d", err, len(first))
	}
	var claimed struct {
		NextAttemptAt time.Time `bson:"next_attempt_at"`
		LeaseUntil    time.Time `bson:"lease_until"`
	}
	if err = collection.FindOne(ctx, bson.M{"message_id": "expired"}).Decode(&claimed); err != nil {
		t.Fatal(err)
	}
	if !claimed.NextAttemptAt.Equal(claimed.LeaseUntil) {
		t.Fatalf("publishing due time %s differs from lease expiry %s", claimed.NextAttemptAt, claimed.LeaseUntil)
	}
	expiredAt := time.Now().Add(-time.Second)
	if _, err = collection.UpdateOne(ctx, bson.M{"message_id": "expired"}, bson.M{"$set": bson.M{"lease_until": expiredAt, "next_attempt_at": expiredAt}}); err != nil {
		t.Fatal(err)
	}
	appendMessage("pending", "org-502", time.Now().Add(-time.Hour))
	next, err := s.ClaimDue(ctx, 1, time.Hour)
	if err != nil || len(next) != 1 {
		t.Fatalf("mixed claim: %v %d", err, len(next))
	}
	t.Logf("Mongo first mixed claim=%s; pending actual due=-1h, expired lease due=-1s", next[0].Message.Input().ID)
	if next[0].Message.Input().ID != "pending" {
		t.Errorf("actual due order violated: got %s", next[0].Message.Input().ID)
	}
}
