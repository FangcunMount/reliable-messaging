//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/relay"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	sqlDriver "github.com/go-sql-driver/mysql"
	driver "github.com/nsqio/go-nsq"
)

type crashBarrierStore struct {
	outbox.Store
	point string
}

func (s crashBarrierStore) ClaimDue(ctx context.Context, n int, lease time.Duration) ([]outbox.Claim, error) {
	cs, err := s.Store.ClaimDue(ctx, n, lease)
	if err == nil && len(cs) > 0 && s.point == "before-publish" {
		crashBarrier()
	}
	return cs, err
}
func (s crashBarrierStore) Confirm(ctx context.Context, c outbox.Claim) error {
	if s.point == "before-writeback" {
		crashBarrier()
	}
	return s.Store.Confirm(ctx, c)
}
func crashBarrier() { fmt.Println("RM_CRASH_READY"); time.Sleep(time.Hour) }
func nsqTestConfig() *driver.Config {
	c := driver.NewConfig()
	c.DialTimeout = time.Second
	c.HeartbeatInterval = time.Second
	c.ReadTimeout = 3 * time.Second
	c.WriteTimeout = time.Second
	return c
}
func crashRelayConfig(observe relay.Observer) relay.Config {
	return relay.Config{Concurrency: 1, PollInterval: 10 * time.Millisecond, Lease: 2 * time.Second, PublishTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond, Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
		return relay.RetryDecision{Delay: 100 * time.Millisecond}
	}, Observe: observe}
}

// Helper is selected explicitly in a separate OS process by the required test.
func TestCrashProcessHelper(t *testing.T) {
	point := os.Getenv("RM_CRASH_POINT")
	if point == "" {
		return
	}
	if point != "before-publish" && point != "before-writeback" {
		t.Fatal("invalid crash point")
	}
	db, err := sql.Open("mysql", os.Getenv("RM_CRASH_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := store.New(db)
	if err != nil {
		t.Fatal(err)
	}
	p, err := driver.NewProducer(os.Getenv("RM_TEST_NSQ_TCP"), nsqTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.SetLogger(nil, driver.LogLevelError)
	defer p.Stop()
	publisher, err := adapter.New(p, map[string]string{"events": os.Getenv("RM_CRASH_TOPIC")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	r, err := relay.New(crashBarrierStore{s, point}, publisher, crashRelayConfig(func(relay.Event) {}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Fatal("child exited without being killed at crash barrier")
}

func TestRelayProcessCrashRecovery(t *testing.T) {
	baseDSN, address, httpAddress := os.Getenv("RM_TEST_MYSQL_DSN"), os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if baseDSN == "" || address == "" || httpAddress == "" {
		t.Fatal("isolated MySQL and NSQ required")
	}
	for _, point := range []string{"before-publish", "before-writeback"} {
		t.Run(point, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := sqlDriver.ParseDSN(baseDSN)
			must(err)
			admin, err := sql.Open("mysql", baseDSN)
			must(err)
			defer admin.Close()
			dbName := "rm_crash_" + strings.ReplaceAll(point, "-", "_")
			_, err = admin.ExecContext(ctx, "CREATE DATABASE "+dbName)
			must(err)
			defer admin.ExecContext(context.Background(), "DROP DATABASE "+dbName)
			cfg.DBName = dbName
			dsn := cfg.FormatDSN()
			db, err := sql.Open("mysql", dsn)
			must(err)
			defer db.Close()
			_, err = db.ExecContext(ctx, store.Schema)
			must(err)
			_, err = db.ExecContext(ctx, "CREATE TABLE effects (message_id VARCHAR(128) PRIMARY KEY, payload LONGBLOB NOT NULL)")
			must(err)
			topic := "rm-crash-" + point
			client := &http.Client{Timeout: 3 * time.Second}
			for _, path := range []string{"/topic/create?topic=" + topic, "/channel/create?topic=" + topic + "&channel=proof"} {
				req, e := http.NewRequestWithContext(ctx, "POST", httpAddress+path, nil)
				must(e)
				res, e := client.Do(req)
				must(e)
				res.Body.Close()
				if res.StatusCode != 200 {
					t.Fatalf("setup status %d", res.StatusCode)
				}
			}
			payload := []byte(fmt.Sprintf(`{"id":%q,"extension":{"keep":true}}`, point))
			m, err := message.New(message.Input{Producer: "crash-proof", ID: point, Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: payload})
			must(err)
			tx, err := db.BeginTx(ctx, nil)
			must(err)
			a, err := store.Bind(tx)
			must(err)
			if err = a.Append(ctx, m, time.Now().Add(-time.Second)); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			must(tx.Commit())
			received := make(chan []byte, 8)
			consumer, err := driver.NewConsumer(topic, "proof", nsqTestConfig())
			must(err)
			consumer.SetLogger(nil, driver.LogLevelError)
			consumer.AddHandler(driver.HandlerFunc(func(msg *driver.Message) error {
				var body struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				if body.ID != point || !bytes.Equal(msg.Body, payload) {
					return fmt.Errorf("unexpected identity or wire")
				}
				// Explicit fixture consumer idempotency: actual business consumers are a
				// separate compatibility gate. Do not infer their behavior from this table.
				if _, err := db.ExecContext(ctx, "INSERT INTO effects VALUES (?,?) ON DUPLICATE KEY UPDATE message_id=message_id", body.ID, msg.Body); err != nil {
					return err
				}
				select {
				case received <- append([]byte(nil), msg.Body...):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}))
			must(consumer.ConnectToNSQD(address))
			defer func() {
				consumer.Stop()
				select {
				case <-consumer.StopChan:
				case <-time.After(5 * time.Second):
					t.Error("consumer drain timeout")
				}
			}()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashProcessHelper$", "-test.v")
			child.Env = append(os.Environ(), "RM_CRASH_POINT="+point, "RM_CRASH_MYSQL_DSN="+dsn, "RM_CRASH_TOPIC="+topic)
			stdout, err := child.StdoutPipe()
			must(err)
			var stderr bytes.Buffer
			child.Stderr = &stderr
			must(child.Start())
			ready := make(chan struct{})
			scanDone := make(chan struct{})
			go func() {
				defer close(scanDone)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "RM_CRASH_READY" {
						close(ready)
						return
					}
				}
			}()
			exited := make(chan error, 1)
			go func() { exited <- child.Wait() }()
			reaped := false
			defer func() {
				if !reaped {
					_ = child.Process.Kill()
					<-exited
				}
				stdout.Close()
				<-scanDone
			}()
			select {
			case <-ready:
			case err := <-exited:
				reaped = true
				t.Fatalf("child failed before barrier: %v %s", err, stderr.String())
			case <-ctx.Done():
				t.Fatal("child did not reach barrier")
			}
			var state string
			must(db.QueryRowContext(ctx, "SELECT state FROM rm_outbox WHERE message_id=?", point).Scan(&state))
			if state != "publishing" {
				t.Fatalf("crash state %s", state)
			}
			must(child.Process.Kill())
			err = <-exited
			reaped = true
			exitErr, ok := err.(*exec.ExitError)
			if !ok || exitErr.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("not an actual SIGKILL: %v", err)
			}
			// No manual lease mutation: wait for DB time to permit the new Relay to claim.
			s, err := store.New(db)
			must(err)
			producer, err := driver.NewProducer(address, nsqTestConfig())
			must(err)
			producer.SetLogger(nil, driver.LogLevelError)
			defer producer.Stop()
			publisher, err := adapter.New(producer, map[string]string{"events": topic}, 1)
			must(err)
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			r, err := relay.New(s, publisher, crashRelayConfig(func(e relay.Event) {
				if e.Kind == "write_succeeded" && e.Outcome == transport.Confirmed {
					stop()
				}
			}))
			must(err)
			must(r.Run(runCtx))
			must(publisher.Drain(ctx))
			expected := 1
			if point == "before-writeback" {
				expected = 2
			}
			for i := 0; i < expected; i++ {
				select {
				case wire := <-received:
					if !bytes.Equal(wire, payload) {
						t.Fatal("changed payload")
					}
				case <-ctx.Done():
					t.Fatal("missing recovered physical delivery")
				}
			}
			var attempts, effects int
			must(db.QueryRowContext(ctx, "SELECT state,attempt_count FROM rm_outbox WHERE message_id=?", point).Scan(&state, &attempts))
			must(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects WHERE message_id=?", point).Scan(&effects))
			if state != "published" || attempts != 2 || effects != 1 {
				t.Fatalf("state=%s attempts=%d effects=%d", state, attempts, effects)
			}
			t.Logf("SIGKILL %s: original ID=%s, claim attempts=%d, observed physical deliveries=%d, fixture effects=%d", point, point, attempts, expected, effects)
		})
	}
}
