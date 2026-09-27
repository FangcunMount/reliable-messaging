package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	adapter "github.com/FangcunMount/reliable-messaging/storage/mongo"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const mongoURI = "mongodb://mongo:27017/?replicaSet=rm-test"

func mongoClient(ctx context.Context) (*driver.Client, error) {
	if os.Getenv("RM_HANDOFF_MONGO_URI") != mongoURI {
		return nil, errors.New("disposable internal Mongo replica set URI required")
	}
	client, err := driver.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

func mongoAppendFact(ctx context.Context, client *driver.Client, id string) error {
	m, err := message.New(message.Input{
		Producer: "version-handoff", ID: id, Destination: "events",
		EventType: "assessment.requested", SchemaVersion: "1", Scope: "scope:global",
		ContentType: "application/json", OccurredAt: "2026-09-27T08:00:00+08:00",
		Payload: []byte(`{"message_id":"` + id + `"}`),
	})
	if err != nil {
		return err
	}
	db := client.Database(database)
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	transaction := options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority())
	_, err = session.WithTransaction(ctx, func(sc driver.SessionContext) (any, error) {
		if _, err := db.Collection("host_fact").InsertOne(sc, bson.M{"_id": id}); err != nil {
			return nil, err
		}
		appender, err := adapter.Bind(sc, db.Collection("rm_outbox"))
		if err != nil {
			return nil, err
		}
		return nil, appender.Append(m, time.Now().Add(-time.Minute))
	}, transaction)
	return err
}

func mongoDrain(ctx context.Context, client *driver.Client, expected ...string) error {
	s, err := adapter.New(client.Database(database).Collection("rm_outbox"))
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 10, time.Minute)
	if err != nil {
		return err
	}
	if len(claims) != len(expected) {
		return fmt.Errorf("mongo claimed %d rows, want %d", len(claims), len(expected))
	}
	want := make(map[string]bool, len(expected))
	for _, id := range expected {
		want[id] = true
	}
	for _, claim := range claims {
		in := claim.Message.Input()
		id := in.ID
		if !want[id] || !ids[id] || in.Producer != "version-handoff" || in.Destination != "events" ||
			in.EventType != "assessment.requested" || string(in.Payload) != `{"message_id":"`+id+`"}` {
			return fmt.Errorf("unexpected Mongo old/new identity or bytes: %q", id)
		}
		delete(want, id)
		if err := s.Confirm(ctx, claim); err != nil {
			return err
		}
	}
	if len(want) != 0 {
		return fmt.Errorf("missing Mongo claims: %v", want)
	}
	return nil
}

func mongoRetryState(ctx context.Context, client *driver.Client, code string, wantAttempts, wantFailures int, quarantine bool) error {
	collection := client.Database(database).Collection("rm_outbox")
	s, err := adapter.New(collection)
	if err != nil {
		return err
	}
	var claim outbox.Claim
	claimed := false
	for attempt := 0; attempt < 50; attempt++ {
		claims, err := s.ClaimDue(ctx, 1, time.Minute)
		if err != nil {
			return err
		}
		if len(claims) == 1 {
			claim, claimed = claims[0], true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !claimed {
		return errors.New("original mongo retry identity was not due")
	}
	in := claim.Message.Input()
	if in.ID != "state-retry" || in.Producer != "version-handoff" || in.Destination != "events" || string(in.Payload) != `{"message_id":"state-retry"}` {
		return fmt.Errorf("unexpected mongo retry identity or bytes %q", in.ID)
	}
	if quarantine {
		err = s.Quarantine(ctx, claim, code)
	} else {
		err = s.Retry(ctx, claim, 10*time.Millisecond, code)
	}
	if err != nil {
		return err
	}
	var row struct {
		State        string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
		LastCode     string `bson:"last_error_code"`
	}
	if err := collection.FindOne(ctx, bson.M{"message_id": "state-retry"}).Decode(&row); err != nil {
		return err
	}
	wantState := "retry_wait"
	if quarantine {
		wantState = "quarantined"
	}
	if row.State != wantState || row.Attempts != wantAttempts || row.FailureCount != wantFailures || row.LastCode != code {
		return fmt.Errorf("mongo retry state=%s attempts=%d failures=%d code=%s, want %s/%d/%d/%s", row.State, row.Attempts, row.FailureCount, row.LastCode, wantState, wantAttempts, wantFailures, code)
	}
	return nil
}

func runMongo(ctx context.Context, client *driver.Client, phase string) error {
	db := client.Database(database)
	outbox := db.Collection("rm_outbox")
	switch phase {
	case "mongo-old-seed":
		if err := db.CreateCollection(ctx, "host_fact"); err != nil {
			return err
		}
		if err := db.CreateCollection(ctx, "rm_outbox"); err != nil {
			return err
		}
		if _, err := outbox.Indexes().CreateMany(ctx, adapter.Indexes()); err != nil {
			return err
		}
		if err := mongoAppendFact(ctx, client, "old-published"); err != nil {
			return err
		}
		if err := mongoDrain(ctx, client, "old-published"); err != nil {
			return err
		}
		if err := mongoAppendFact(ctx, client, "old-pending"); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.1.0 transaction left one published and one pending")
	case "mongo-new-index":
		if _, err := outbox.Indexes().CreateMany(ctx, adapter.Indexes()); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.2.1 indexes installed alongside old standard documents")
	case "mongo-old-after-index":
		if err := mongoAppendFact(ctx, client, "old-after-ddl"); err != nil {
			return err
		}
		fmt.Println("PASS actual Mongo v0.1.0 Appender still wrote after new indexes")
	case "mongo-new-drain":
		if err := mongoDrain(ctx, client, "old-pending", "old-after-ddl"); err != nil {
			return err
		}
		if err := mongoAppendFact(ctx, client, "new-pending"); err != nil {
			return err
		}
		fmt.Println("PASS actual Mongo v0.2.1 drained old identities and left new pending")
	case "mongo-old-drain":
		if err := mongoDrain(ctx, client, "new-pending"); err != nil {
			return err
		}
		facts, err := db.Collection("host_fact").CountDocuments(ctx, bson.M{})
		if err != nil {
			return err
		}
		total, err := outbox.CountDocuments(ctx, bson.M{})
		if err != nil {
			return err
		}
		published, err := outbox.CountDocuments(ctx, bson.M{"state": "published"})
		if err != nil {
			return err
		}
		missingCreated, err := outbox.CountDocuments(ctx, bson.M{"created_at": bson.M{"$exists": false}})
		if err != nil {
			return err
		}
		if facts != 4 || total != 4 || published != 4 || missingCreated != 3 {
			return fmt.Errorf("mongo final facts=%d outbox=%d published=%d old-without-created-at=%d", facts, total, published, missingCreated)
		}
		fmt.Println("PASS actual Mongo v0.1.0 drained ordinary v0.2.1 pending; four host facts and original identities remain")
		fmt.Println("OBSERVED three v0.1.0 documents still lack created_at; host status reader needs separate compatibility handling")
	case "mongo-old-retry-seed":
		if err := mongoAppendFact(ctx, client, "state-retry"); err != nil {
			return err
		}
		if err := mongoRetryState(ctx, client, "old_failure", 1, 0, false); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.1.0 retry retained original identity and did not write future failure_count")
	case "mongo-new-retry":
		if err := mongoRetryState(ctx, client, "new_failure", 2, 1, false); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.2.1 retry incremented failure_count on old intent")
	case "mongo-old-retry-downgrade":
		if err := mongoRetryState(ctx, client, "old_after_downgrade", 3, 1, false); err != nil {
			return err
		}
		fmt.Println("OBSERVED Mongo v0.1.0 retry after downgrade did not advance failure_count")
	case "mongo-new-retry-quarantine":
		if err := mongoRetryState(ctx, client, "quarantine_after_downgrade", 4, 2, true); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.2.1 quarantined original identity; diagnostic failure_count undercounts old retries")
	case "mongo-old-lease-seed":
		if err := mongoLeaseSeed(ctx, client, "old-lease"); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.1.0 left original old-lease in publishing")
	case "mongo-new-lease-recover":
		if err := mongoLeaseRecover(ctx, client, "old-lease"); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.2.1 respected and recovered v0.1.0 lease; stale claim fenced")
	case "mongo-new-lease-seed":
		if err := mongoLeaseSeed(ctx, client, "new-lease"); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.2.1 left original new-lease in publishing")
	case "mongo-old-lease-recover":
		if err := mongoLeaseRecover(ctx, client, "new-lease"); err != nil {
			return err
		}
		fmt.Println("PASS Mongo v0.1.0 respected and recovered v0.2.1 lease; stale claim fenced")
	case "mongo-parallel-seed":
		return mongoParallelSeed(ctx, client)
	case "mongo-parallel-claim":
		return mongoParallelClaim(ctx, client)
	case "mongo-parallel-verify":
		return mongoParallelVerify(ctx, client)
	case "mongo-relay-parallel-seed":
		return mongoRelaySeed(ctx, client)
	case "mongo-relay-parallel-run":
		return mongoRelayRun(ctx, client)
	case "mongo-relay-parallel-verify":
		return mongoRelayVerify(ctx, client)
	case "mongo-lost-ack-seed":
		return mongoLostAckSeed(ctx, client)
	case "mongo-lost-ack-old":
		return mongoLostAckOld(ctx, client)
	case "mongo-lost-ack-new":
		return mongoLostAckNew(ctx, client)
	case "mongo-lost-ack-verify":
		return mongoLostAckVerify(ctx, client)
	default:
		return fmt.Errorf("unknown Mongo phase %q", phase)
	}
	return nil
}
