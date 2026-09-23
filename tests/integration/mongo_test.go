//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	adapter "github.com/FangcunMount/reliable-messaging/storage/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func TestMongoOriginalTransactionAndReentry(t *testing.T) {
	uri := os.Getenv("RM_TEST_MONGO_URI")
	if uri == "" {
		t.Fatal("isolated replica-set URI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	db := client.Database("rm_sdk_proof")
	must(db.CreateCollection(ctx, "business"))
	must(db.CreateCollection(ctx, "outbox"))
	defer db.Drop(context.Background())
	session, err := client.StartSession()
	must(err)
	defer session.EndSession(ctx)
	sessionCtx := driver.NewSessionContext(ctx, session)
	if _, err = adapter.Bind(sessionCtx, db.Collection("outbox")); !errors.Is(err, adapter.ErrTransactionRequired) {
		t.Fatal("inactive session accepted", err)
	}
	in := message.Input{Producer: "mongo-proof", ID: "stable", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{ "id":"stable", "extension":true }`)}
	m, err := message.New(in)
	must(err)
	attempts := 0
	due := time.Now()
	opts := options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority())
	var held *adapter.Appender
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
		attempts++
		a, err := adapter.Bind(sc, db.Collection("outbox"))
		if err != nil {
			return nil, err
		}
		held = a
		if _, err = db.Collection("business").InsertOne(sc, bson.M{"_id": "stable"}); err != nil {
			return nil, err
		}
		if err = a.Append(m, due); err != nil {
			return nil, err
		}
		if attempts == 1 {
			return nil, driver.CommandError{Code: 112, Message: "controlled callback retry", Labels: []string{"TransientTransactionError"}}
		}
		return nil, a.Append(m, due)
	}, opts)
	must(err)
	if attempts != 2 {
		t.Fatalf("callback attempts %d", attempts)
	}
	if err = held.Append(m, due); !errors.Is(err, adapter.ErrTransactionRequired) {
		t.Fatal("completed session accepted", err)
	}
	rollback := errors.New("host abort")
	in.ID = "abort"
	aborted, err := message.New(in)
	must(err)
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
		a, e := adapter.Bind(sc, db.Collection("outbox"))
		if e != nil {
			return nil, e
		}
		if _, e = db.Collection("business").InsertOne(sc, bson.M{"_id": "abort"}); e != nil {
			return nil, e
		}
		if e = a.Append(aborted, due); e != nil {
			return nil, e
		}
		return nil, rollback
	}, opts)
	if !errors.Is(err, rollback) {
		t.Fatal("wrong abort", err)
	}
	for _, name := range []string{"business", "outbox"} {
		n, e := db.Collection(name).CountDocuments(ctx, bson.M{})
		must(e)
		if n != 1 {
			t.Fatalf("%s count %d", name, n)
		}
	}
	in.ID = "stable"
	in.Payload = []byte(`{"changed":true}`)
	changed, err := message.New(in)
	must(err)
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
		a, e := adapter.Bind(sc, db.Collection("outbox"))
		if e != nil {
			return nil, e
		}
		return nil, a.Append(changed, due)
	}, opts)
	if !errors.Is(err, outbox.ErrConflict) {
		t.Fatal("content conflict not rejected", err)
	}
	var record struct {
		Payload   []byte    `bson:"payload"`
		CreatedAt time.Time `bson:"created_at"`
		UpdatedAt time.Time `bson:"updated_at"`
	}
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&record))
	if string(record.Payload) != string(m.Input().Payload) {
		t.Fatal("original bytes lost")
	}
	if record.CreatedAt.IsZero() || time.Since(record.CreatedAt) < 0 || time.Since(record.CreatedAt) > time.Minute {
		t.Fatalf("standard document has no usable creation time: %s", record.CreatedAt)
	}
	if record.UpdatedAt.IsZero() || record.UpdatedAt.Sub(record.CreatedAt).Abs() > time.Millisecond {
		t.Fatalf("new Mongo record lacks original update time: created=%s updated=%s", record.CreatedAt, record.UpdatedAt)
	}
	s, err := adapter.New(db.Collection("outbox"))
	must(err)
	_, err = db.Collection("outbox").Indexes().CreateMany(ctx, adapter.Indexes())
	must(err)
	_, err = db.Collection("outbox").UpdateOne(ctx, bson.M{"message_id": "stable"}, bson.M{"$set": bson.M{"updated_at": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}})
	must(err)
	claims, err := s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatalf("claims %d", len(claims))
	}
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&record))
	if time.Since(record.UpdatedAt) < 0 || time.Since(record.UpdatedAt) > time.Minute {
		t.Fatalf("claim did not advance Mongo update time: %s", record.UpdatedAt)
	}
	old := claims[0]
	if old.FailureCount != 0 {
		t.Fatalf("initial failure count %d", old.FailureCount)
	}
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("live claim stolen")
	}
	_, err = db.Collection("outbox").UpdateOne(ctx, bson.M{"message_id": "stable"}, bson.M{"$set": bson.M{"lease_until": time.Now().Add(-time.Minute)}})
	must(err)
	if err = s.Confirm(ctx, old); !errors.Is(err, outbox.ErrStaleClaim) {
		t.Fatal("expired confirm accepted", err)
	}
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatal("expired claim not recovered")
	}
	fresh := claims[0]
	if fresh.RecordID != old.RecordID || fresh.Token == old.Token || fresh.Version <= old.Version || fresh.Attempts != 2 || fresh.FailureCount != 0 {
		t.Fatal("reclaim fencing changed incorrectly")
	}
	for _, e := range []error{s.Confirm(ctx, old), s.Retry(ctx, old, time.Second, "unknown"), s.Quarantine(ctx, old, "invalid")} {
		if !errors.Is(e, outbox.ErrStaleClaim) {
			t.Fatal("stale write accepted", e)
		}
	}
	for _, delay := range []time.Duration{0, -time.Second} {
		if e := s.Retry(ctx, fresh, delay, "unknown"); e == nil {
			t.Fatal("nonpositive retry delay accepted")
		}
	}
	_, err = db.Collection("outbox").UpdateOne(ctx, bson.M{"message_id": "stable"}, bson.M{"$set": bson.M{"updated_at": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}})
	must(err)
	must(s.Retry(ctx, fresh, time.Hour, "$literal-error"))
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&record))
	if time.Since(record.UpdatedAt) < 0 || time.Since(record.UpdatedAt) > time.Minute {
		t.Fatalf("failed transition did not advance Mongo update time: %s", record.UpdatedAt)
	}
	var retried struct {
		FailureCount uint64 `bson:"failure_count"`
	}
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&retried))
	if retried.FailureCount != 1 {
		t.Fatalf("retry failure count %d", retried.FailureCount)
	}
	cursor, e := db.Collection("outbox").Aggregate(ctx, bson.A{
		bson.M{"$match": bson.M{"message_id": "stable"}},
		bson.M{"$project": bson.M{"remaining_ms": bson.M{"$subtract": bson.A{"$next_attempt_at", "$$NOW"}}, "last_error_code": 1}},
	})
	must(e)
	var remaining []struct {
		Millis int64  `bson:"remaining_ms"`
		Code   string `bson:"last_error_code"`
	}
	must(cursor.All(ctx, &remaining))
	if len(remaining) != 1 || remaining[0].Code != "$literal-error" || remaining[0].Millis > time.Hour.Milliseconds() || remaining[0].Millis < (time.Hour-10*time.Second).Milliseconds() {
		t.Fatalf("retry database clock/literal: %+v", remaining)
	}

	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("retry before due")
	}
	_, err = db.Collection("outbox").UpdateOne(ctx, bson.M{"message_id": "stable"}, bson.M{"$set": bson.M{"next_attempt_at": time.Now().Add(-time.Minute)}})
	must(err)
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatal("due retry missing")
	}
	if claims[0].FailureCount != 1 || claims[0].Attempts != 3 {
		t.Fatalf("reclaim lost failure budget: attempts=%d failures=%d", claims[0].Attempts, claims[0].FailureCount)
	}
	must(s.Confirm(ctx, claims[0]))
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&retried))
	if retried.FailureCount != 1 {
		t.Fatalf("confirm changed failure count %d", retried.FailureCount)
	}
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("published record reclaimed")
	}
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
		a, e := adapter.Bind(sc, db.Collection("outbox"))
		if e != nil {
			return nil, e
		}
		for i := 0; i < 6; i++ {
			in.ID = fmt.Sprintf("parallel-%d", i)
			item, e := message.New(in)
			if e != nil {
				return nil, e
			}
			if e = a.Append(item, due); e != nil {
				return nil, e
			}
		}
		return nil, nil
	}, opts)
	must(err)
	type claimResult struct {
		claims []outbox.Claim
		err    error
	}
	results := make(chan claimResult, 2)
	for i := 0; i < 2; i++ {
		go func() { cs, e := s.ClaimDue(ctx, 6, time.Minute); results <- claimResult{cs, e} }()
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		r := <-results
		must(r.err)
		for _, c := range r.claims {
			if seen[c.RecordID] {
				t.Fatal("duplicate concurrent claim")
			}
			seen[c.RecordID] = true
			must(s.Confirm(ctx, c))
		}
	}
	if len(seen) != 6 {
		t.Fatalf("claimed %d of 6", len(seen))
	}
	// A malformed immutable payload stays visible in quarantine.
	_, err = db.Collection("outbox").UpdateOne(ctx, bson.M{"message_id": "stable"}, bson.M{"$set": bson.M{"state": "pending", "payload": []byte("tampered")}})
	must(err)
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("corrupt record published")
	}
	n, err := db.Collection("outbox").CountDocuments(ctx, bson.M{"message_id": "stable", "state": "quarantined", "payload": []byte("tampered")})
	must(err)
	if n != 1 {
		t.Fatal("quarantine evidence lost")
	}
	var quarantined struct {
		FailureCount uint64 `bson:"failure_count"`
	}
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&quarantined))
	if quarantined.FailureCount != 2 {
		t.Fatalf("corruption quarantine failure count %d", quarantined.FailureCount)
	}

}

func TestMongoUnknownCommitRetriesCommitNotBusiness(t *testing.T) {
	uri := os.Getenv("RM_TEST_MONGO_URI")
	if uri == "" {
		t.Fatal("isolated replica-set URI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var commits, unknownReplies atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "commitTransaction" {
			commits.Add(1)
		}
	}, Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		if e.CommandName == "commitTransaction" && e.Reply.Lookup("writeConcernError").Type != 0 {
			unknownReplies.Add(1)
		}
	}}
	client, err := driver.Connect(ctx, options.Client().ApplyURI(uri).SetMonitor(monitor))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	db := client.Database("rm_commit_unknown")
	must(db.CreateCollection(ctx, "business"))
	must(db.CreateCollection(ctx, "outbox"))
	defer db.Drop(context.Background())
	admin := client.Database("admin")
	must(admin.RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: bson.M{"failCommands": bson.A{"commitTransaction"}, "writeConcernError": bson.M{"code": 64, "errmsg": "isolated unknown commit result"}, "errorLabels": bson.A{"UnknownTransactionCommitResult"}}}}).Err())
	defer admin.RunCommand(context.Background(), bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}})
	session, err := client.StartSession()
	must(err)
	defer session.EndSession(ctx)
	m, err := message.New(message.Input{Producer: "mongo-proof", ID: "commit-unknown", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"id":"commit-unknown"}`)})
	must(err)
	callbacks := 0
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
		callbacks++
		a, e := adapter.Bind(sc, db.Collection("outbox"))
		if e != nil {
			return nil, e
		}
		if _, e = db.Collection("business").InsertOne(sc, bson.M{"_id": "original"}); e != nil {
			return nil, e
		}
		return nil, a.Append(m, time.Now())
	}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
	must(err)
	if callbacks != 1 || commits.Load() != 2 || unknownReplies.Load() != 1 {
		t.Fatalf("callbacks=%d commits=%d unknownReplies=%d", callbacks, commits.Load(), unknownReplies.Load())
	}
	for _, name := range []string{"business", "outbox"} {
		n, e := db.Collection(name).CountDocuments(ctx, bson.M{})
		must(e)
		if n != 1 {
			t.Fatalf("%s has %d rows", name, n)
		}
	}
}
