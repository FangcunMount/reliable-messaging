package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
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
		return fmt.Errorf("Mongo claimed %d rows, want %d", len(claims), len(expected))
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
			return fmt.Errorf("Mongo final facts=%d outbox=%d published=%d old-without-created-at=%d", facts, total, published, missingCreated)
		}
		fmt.Println("PASS actual Mongo v0.1.0 drained ordinary v0.2.1 pending; four host facts and original identities remain")
		fmt.Println("OBSERVED three v0.1.0 documents still lack created_at; host status reader needs separate compatibility handling")
	default:
		return fmt.Errorf("unknown Mongo phase %q", phase)
	}
	return nil
}
