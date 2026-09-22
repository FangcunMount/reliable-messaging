//go:build integration

package integration

import (
	"context"
	"errors"
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
		Payload []byte `bson:"payload"`
	}
	must(db.Collection("outbox").FindOne(ctx, bson.M{"message_id": "stable"}).Decode(&record))
	if string(record.Payload) != string(m.Input().Payload) {
		t.Fatal("original bytes lost")
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
