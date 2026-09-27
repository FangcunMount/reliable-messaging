package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/relay"
	adapter "github.com/FangcunMount/reliable-messaging/storage/mongo"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"github.com/FangcunMount/reliable-messaging/transport"
	nsqadapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	driver "github.com/nsqio/go-nsq"
	"go.mongodb.org/mongo-driver/bson"
	mongoDriver "go.mongodb.org/mongo-driver/mongo"
)

const (
	relayMySQLID    = "relay-parallel-mysql"
	relayMongoID    = "relay-parallel-mongo"
	relayMySQLTopic = "rm-m6-relay-mysql"
	relayMongoTopic = "rm-m6-relay-mongo"
	nsqTCP          = "nsqd:4150"
	nsqHTTP         = "http://nsqd:4151"
	nsqChannel      = "handoff-proof"
)

type relayBarrierStore struct {
	outbox.Store
	id     string
	first  atomic.Bool
	before func(context.Context) error
	after  func(context.Context, bool) error
	wait   func(context.Context) error
}

func (s *relayBarrierStore) ClaimDue(ctx context.Context, n int, lease time.Duration) ([]outbox.Claim, error) {
	if !s.first.CompareAndSwap(false, true) {
		return s.Store.ClaimDue(ctx, n, lease)
	}
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	claims, err := s.Store.ClaimDue(ctx, n, lease)
	if err != nil {
		return nil, err
	}
	if len(claims) > 1 {
		return nil, fmt.Errorf("parallel relay claimed %d intents", len(claims))
	}
	if len(claims) == 1 {
		if err := checkLeaseIdentity(claims[0], s.id); err != nil {
			return nil, err
		}
		if claims[0].Attempts != 1 {
			return nil, fmt.Errorf("parallel relay attempts=%d", claims[0].Attempts)
		}
	}
	if err := s.after(ctx, len(claims) == 1); err != nil {
		return nil, err
	}
	if len(claims) == 1 {
		// Keep the winner inside ClaimDue until the other released Relay has
		// attempted the same live lease; only then allow NSQ publication.
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}
	return claims, nil
}

func requireNSQ() error {
	if os.Getenv("RM_HANDOFF_NSQ_TCP") != nsqTCP || os.Getenv("RM_HANDOFF_NSQ_HTTP") != nsqHTTP {
		return errors.New("disposable internal NSQ addresses required")
	}
	return nil
}

func createNSQChannel(ctx context.Context, topic string) error {
	if err := requireNSQ(); err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	for _, path := range []string{"/topic/create?topic=" + topic, "/channel/create?topic=" + topic + "&channel=" + nsqChannel} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, nsqHTTP+path, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("NSQ setup %s: status %d", path, response.StatusCode)
		}
	}
	return nil
}

func relayProducer() (*driver.Producer, error) {
	if err := requireNSQ(); err != nil {
		return nil, err
	}
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	producer, err := driver.NewProducer(nsqTCP, cfg)
	if err != nil {
		return nil, err
	}
	producer.SetLogger(nil, driver.LogLevelError)
	if err := producer.Ping(); err != nil {
		producer.Stop()
		return nil, err
	}
	return producer, nil
}

func runParallelRelay(ctx context.Context, store outbox.Store, topic, label string) error {
	producer, err := relayProducer()
	if err != nil {
		return err
	}
	defer producer.Stop()
	publisher, err := nsqadapter.New(producer, map[string]string{"events": topic}, 1)
	if err != nil {
		return err
	}
	r, err := relay.New(store, publisher, relay.Config{
		Concurrency: 1, PollInterval: 50 * time.Millisecond, Lease: 10 * time.Second,
		PublishTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
		Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
			return relay.RetryDecision{Delay: time.Second}
		},
		Observe: func(e relay.Event) {
			if e.Kind == "write_succeeded" && e.Outcome == transport.Confirmed {
				fmt.Printf("%s Relay confirmed NSQ and Outbox\n", label)
			}
		},
	})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := r.Run(runCtx); err != nil {
		return err
	}
	return publisher.Drain(ctx)
}

func verifyNSQ(ctx context.Context, topic, id string) error {
	return verifyNSQCount(ctx, topic, id, 1)
}

func verifyNSQCount(ctx context.Context, topic, id string, expected int) error {
	if err := requireNSQ(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nsqHTTP+"/stats?format=json", nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("NSQ stats status %d", response.StatusCode)
	}
	var stats struct {
		Topics []struct {
			Name         string `json:"topic_name"`
			MessageCount int    `json:"message_count"`
			Channels     []struct {
				Name  string `json:"channel_name"`
				Depth int    `json:"depth"`
			} `json:"channels"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		return err
	}
	found := false
	for _, current := range stats.Topics {
		if current.Name != topic {
			continue
		}
		found = true
		if current.MessageCount != expected || len(current.Channels) != 1 || current.Channels[0].Name != nsqChannel || current.Channels[0].Depth != expected {
			return fmt.Errorf("NSQ topic %s count=%d channels=%+v", topic, current.MessageCount, current.Channels)
		}
	}
	if !found {
		return fmt.Errorf("NSQ topic %s missing", topic)
	}
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	consumer, err := driver.NewConsumer(topic, nsqChannel, cfg)
	if err != nil {
		return err
	}
	consumer.SetLogger(nil, driver.LogLevelError)
	received := make(chan []byte, expected+1)
	consumer.AddHandler(driver.HandlerFunc(func(m *driver.Message) error {
		received <- append([]byte(nil), m.Body...)
		return nil
	}))
	if err := consumer.ConnectToNSQD(nsqTCP); err != nil {
		consumer.Stop()
		return err
	}
	defer consumer.Stop()
	for i := 0; i < expected; i++ {
		select {
		case body := <-received:
			if string(body) != `{"message_id":"`+id+`"}` {
				return fmt.Errorf("NSQ changed original payload %q", string(body))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func mysqlRelaySeed(ctx context.Context, db *sql.DB) error {
	if err := createNSQChannel(ctx, relayMySQLTopic); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE handoff_relay_barrier (participant VARCHAR(8) PRIMARY KEY, claimed BOOLEAN NULL) ENGINE=InnoDB`); err != nil {
		return err
	}
	return appendFact(ctx, db, relayMySQLID)
}

func mysqlRelayRun(ctx context.Context, db *sql.DB) error {
	label, err := participant()
	if err != nil {
		return err
	}
	s, err := store.New(db)
	if err != nil {
		return err
	}
	barrier := &relayBarrierStore{Store: s, id: relayMySQLID,
		before: func(ctx context.Context) error {
			if _, err := db.ExecContext(ctx, "INSERT INTO handoff_relay_barrier(participant) VALUES (?)", label); err != nil {
				return err
			}
			return waitForBoth(ctx, func(ctx context.Context) (int64, error) {
				var count int64
				err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM handoff_relay_barrier").Scan(&count)
				return count, err
			})
		},
		after: func(ctx context.Context, claimed bool) error {
			_, err := db.ExecContext(ctx, "UPDATE handoff_relay_barrier SET claimed=? WHERE participant=?", claimed, label)
			return err
		},
		wait: func(ctx context.Context) error {
			return waitForBoth(ctx, func(ctx context.Context) (int64, error) {
				var count int64
				err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM handoff_relay_barrier WHERE claimed IS NOT NULL").Scan(&count)
				return count, err
			})
		},
	}
	return runParallelRelay(ctx, barrier, relayMySQLTopic, label)
}

func mysqlRelayVerify(ctx context.Context, db *sql.DB) error {
	var participants, winners int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(claimed),0) FROM handoff_relay_barrier").Scan(&participants, &winners); err != nil {
		return err
	}
	var state string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, "SELECT state,attempt_count,failure_count FROM rm_outbox WHERE message_id=?", relayMySQLID).
		Scan(&state, &attempts, &failures); err != nil {
		return err
	}
	if participants != 2 || winners != 1 || state != "published" || attempts != 1 || failures != 0 {
		return fmt.Errorf("mysql relay participants=%d winners=%d state=%s attempts=%d failures=%d", participants, winners, state, attempts, failures)
	}
	if err := verifyNSQ(ctx, relayMySQLTopic, relayMySQLID); err != nil {
		return err
	}
	fmt.Println("PASS MySQL mixed-version Relays published one original intent once to real NSQ")
	return nil
}

func mongoRelaySeed(ctx context.Context, client *mongoDriver.Client) error {
	if err := createNSQChannel(ctx, relayMongoTopic); err != nil {
		return err
	}
	return mongoAppendFact(ctx, client, relayMongoID)
}

func mongoRelayRun(ctx context.Context, client *mongoDriver.Client) error {
	label, err := participant()
	if err != nil {
		return err
	}
	db := client.Database(database)
	s, err := adapter.New(db.Collection("rm_outbox"))
	if err != nil {
		return err
	}
	collection := db.Collection("handoff_relay_barrier")
	barrier := &relayBarrierStore{Store: s, id: relayMongoID,
		before: func(ctx context.Context) error {
			if _, err := collection.InsertOne(ctx, bson.M{"_id": label}); err != nil {
				return err
			}
			return waitForBoth(ctx, func(ctx context.Context) (int64, error) {
				return collection.CountDocuments(ctx, bson.M{})
			})
		},
		after: func(ctx context.Context, claimed bool) error {
			_, err := collection.UpdateByID(ctx, label, bson.M{"$set": bson.M{"claimed": claimed}})
			return err
		},
		wait: func(ctx context.Context) error {
			return waitForBoth(ctx, func(ctx context.Context) (int64, error) {
				return collection.CountDocuments(ctx, bson.M{"claimed": bson.M{"$exists": true}})
			})
		},
	}
	return runParallelRelay(ctx, barrier, relayMongoTopic, label)
}

func mongoRelayVerify(ctx context.Context, client *mongoDriver.Client) error {
	db := client.Database(database)
	participants, err := db.Collection("handoff_relay_barrier").CountDocuments(ctx, bson.M{})
	if err != nil {
		return err
	}
	winners, err := db.Collection("handoff_relay_barrier").CountDocuments(ctx, bson.M{"claimed": true})
	if err != nil {
		return err
	}
	var final struct {
		State        string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
	}
	if err := db.Collection("rm_outbox").FindOne(ctx, bson.M{"message_id": relayMongoID}).Decode(&final); err != nil {
		return err
	}
	if participants != 2 || winners != 1 || final.State != "published" || final.Attempts != 1 || final.FailureCount != 0 {
		return fmt.Errorf("mongo relay participants=%d winners=%d state=%s attempts=%d failures=%d", participants, winners, final.State, final.Attempts, final.FailureCount)
	}
	if err := verifyNSQ(ctx, relayMongoTopic, relayMongoID); err != nil {
		return err
	}
	fmt.Println("PASS Mongo mixed-version Relays published one original intent once to real NSQ")
	return nil
}
