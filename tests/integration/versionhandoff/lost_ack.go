package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
	lostAckMySQLID    = "lost-ack-mysql"
	lostAckMongoID    = "lost-ack-mongo"
	lostAckMySQLTopic = "rm-m6-lost-ack-mysql"
	lostAckMongoTopic = "rm-m6-lost-ack-mongo"
)

// dropPublishOK forwards the real NSQ connection and drops only the successful
// PUB response after nsqd accepted the bytes. It is used only in disposable CI.
func dropPublishOK() (string, <-chan error, func(), error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, nil, err
	}
	done := make(chan error, 1)
	go func() {
		client, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer client.Close()
		_ = client.SetDeadline(time.Now().Add(7 * time.Second))
		upstream, err := net.DialTimeout("tcp", nsqTCP, time.Second)
		if err != nil {
			done <- err
			return
		}
		defer upstream.Close()
		_ = upstream.SetDeadline(time.Now().Add(7 * time.Second))
		copied := make(chan struct{})
		go func() { defer close(copied); _, _ = io.Copy(upstream, client) }()
		defer func() { client.Close(); upstream.Close(); <-copied }()
		for {
			var header [4]byte
			if _, err := io.ReadFull(upstream, header[:]); err != nil {
				done <- err
				return
			}
			n := binary.BigEndian.Uint32(header[:])
			if n < 4 || n > 1024*1024 {
				done <- fmt.Errorf("unexpected NSQ frame %d", n)
				return
			}
			frame := make([]byte, n)
			if _, err := io.ReadFull(upstream, frame); err != nil {
				done <- err
				return
			}
			if binary.BigEndian.Uint32(frame[:4]) == 0 && string(frame[4:]) == "OK" {
				done <- nil
				return
			}
			if _, err := client.Write(append(header[:], frame...)); err != nil {
				done <- err
				return
			}
		}
	}()
	return listener.Addr().String(), done, func() { _ = listener.Close() }, nil
}

func outcomeProducer(address string) (*driver.Producer, error) {
	if err := requireNSQ(); err != nil {
		return nil, err
	}
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		return nil, err
	}
	producer.SetLogger(nil, driver.LogLevelError)
	return producer, nil
}

func runOutcomeRelay(ctx context.Context, s outbox.Store, topic, address string, expected transport.Outcome) error {
	producer, err := outcomeProducer(address)
	if err != nil {
		return err
	}
	defer producer.Stop()
	publisher, err := nsqadapter.New(producer, map[string]string{"events": topic}, 1)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	observed := 0
	r, err := relay.New(s, publisher, relay.Config{
		Concurrency: 1, PollInterval: 20 * time.Millisecond, Lease: 10 * time.Second,
		PublishTimeout: 4 * time.Second, WriteTimeout: 2 * time.Second,
		Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
			return relay.RetryDecision{Delay: 250 * time.Millisecond}
		},
		Observe: func(e relay.Event) {
			if e.Kind == "write_succeeded" {
				observed++
				if e.Outcome == expected {
					cancel()
				}
			}
		},
	})
	if err != nil {
		return err
	}
	if err := r.Run(runCtx); err != nil {
		return err
	}
	if err := publisher.Drain(ctx); err != nil {
		return err
	}
	if observed != 1 || !errors.Is(runCtx.Err(), context.Canceled) {
		return fmt.Errorf("expected one %v writeback, observed %d (context %v)", expected, observed, runCtx.Err())
	}
	return nil
}

func mysqlLostAckSeed(ctx context.Context, db *sql.DB) error {
	if err := createNSQChannel(ctx, lostAckMySQLTopic); err != nil {
		return err
	}
	return appendFact(ctx, db, lostAckMySQLID)
}

func mysqlLostAckOld(ctx context.Context, db *sql.DB) error {
	address, dropped, closeProxy, err := dropPublishOK()
	if err != nil {
		return err
	}
	defer closeProxy()
	s, err := store.New(db)
	if err != nil {
		return err
	}
	if err := runOutcomeRelay(ctx, s, lostAckMySQLTopic, address, transport.Unknown); err != nil {
		return err
	}
	select {
	case err := <-dropped:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	var state string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, "SELECT state,attempt_count,failure_count FROM rm_outbox WHERE message_id=?", lostAckMySQLID).
		Scan(&state, &attempts, &failures); err != nil {
		return err
	}
	if state != "retry_wait" || attempts != 1 || failures != 0 {
		return fmt.Errorf("old MySQL lost ACK state=%s attempts=%d failures=%d", state, attempts, failures)
	}
	fmt.Println("PASS MySQL v0.1.0 real NSQ accepted payload but PUB OK was lost; original intent awaits retry")
	return nil
}

func mysqlLostAckNew(ctx context.Context, db *sql.DB) error {
	s, err := store.New(db)
	if err != nil {
		return err
	}
	return runOutcomeRelay(ctx, s, lostAckMySQLTopic, nsqTCP, transport.Confirmed)
}

func mysqlLostAckVerify(ctx context.Context, db *sql.DB) error {
	var state string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, "SELECT state,attempt_count,failure_count FROM rm_outbox WHERE message_id=?", lostAckMySQLID).
		Scan(&state, &attempts, &failures); err != nil {
		return err
	}
	if state != "published" || attempts != 2 || failures != 0 {
		return fmt.Errorf("MySQL recovered lost ACK state=%s attempts=%d failures=%d", state, attempts, failures)
	}
	if err := verifyNSQCount(ctx, lostAckMySQLTopic, lostAckMySQLID, 2); err != nil {
		return err
	}
	fmt.Println("PASS MySQL v0.2.1 recovered old unknown outcome with original identity; two physical NSQ copies")
	return nil
}

func mongoLostAckSeed(ctx context.Context, client *mongoDriver.Client) error {
	if err := createNSQChannel(ctx, lostAckMongoTopic); err != nil {
		return err
	}
	return mongoAppendFact(ctx, client, lostAckMongoID)
}

func mongoLostAckOld(ctx context.Context, client *mongoDriver.Client) error {
	address, dropped, closeProxy, err := dropPublishOK()
	if err != nil {
		return err
	}
	defer closeProxy()
	collection := client.Database(database).Collection("rm_outbox")
	s, err := adapter.New(collection)
	if err != nil {
		return err
	}
	if err := runOutcomeRelay(ctx, s, lostAckMongoTopic, address, transport.Unknown); err != nil {
		return err
	}
	select {
	case err := <-dropped:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	var state struct {
		Name         string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
	}
	if err := collection.FindOne(ctx, bson.M{"message_id": lostAckMongoID}).Decode(&state); err != nil {
		return err
	}
	if state.Name != "retry_wait" || state.Attempts != 1 || state.FailureCount != 0 {
		return fmt.Errorf("old Mongo lost ACK state=%s attempts=%d failures=%d", state.Name, state.Attempts, state.FailureCount)
	}
	fmt.Println("PASS Mongo v0.1.0 real NSQ accepted payload but PUB OK was lost; original intent awaits retry")
	return nil
}

func mongoLostAckNew(ctx context.Context, client *mongoDriver.Client) error {
	s, err := adapter.New(client.Database(database).Collection("rm_outbox"))
	if err != nil {
		return err
	}
	return runOutcomeRelay(ctx, s, lostAckMongoTopic, nsqTCP, transport.Confirmed)
}

func mongoLostAckVerify(ctx context.Context, client *mongoDriver.Client) error {
	var state struct {
		Name         string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
	}
	if err := client.Database(database).Collection("rm_outbox").FindOne(ctx, bson.M{"message_id": lostAckMongoID}).Decode(&state); err != nil {
		return err
	}
	if state.Name != "published" || state.Attempts != 2 || state.FailureCount != 0 {
		return fmt.Errorf("mongo recovered lost ACK state=%s attempts=%d failures=%d", state.Name, state.Attempts, state.FailureCount)
	}
	if err := verifyNSQCount(ctx, lostAckMongoTopic, lostAckMongoID, 2); err != nil {
		return err
	}
	fmt.Println("PASS Mongo v0.2.1 recovered old unknown outcome with original identity; two physical NSQ copies")
	return nil
}
