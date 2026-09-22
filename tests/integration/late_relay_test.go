//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/relay"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"github.com/FangcunMount/reliable-messaging/transport"
)

// Real database and two real Relays, with a controlled late publisher result.
// The double deliberately ignores its deadline to represent a suspended old
// execution returning late. This does not prove broker durability or shutdown
// guarantees for arbitrary non-cooperative callbacks.
func TestLateRelayCannotOverwriteRecoveredClaim(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated MySQL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	db, err := sql.Open("mysql", dsn)
	must(err)
	defer db.Close()
	_, err = db.ExecContext(ctx, store.Schema)
	must(err)
	_, err = db.ExecContext(ctx, "DELETE FROM rm_outbox WHERE producer='late-relay'")
	must(err)
	defer db.ExecContext(context.Background(), "DELETE FROM rm_outbox WHERE producer='late-relay'")
	m, err := message.New(message.Input{Producer: "late-relay", ID: "stable", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"unchanged":true}`)})
	must(err)
	tx, err := db.BeginTx(ctx, nil)
	must(err)
	defer tx.Rollback()
	appender, err := store.Bind(tx)
	must(err)
	must(appender.Append(ctx, m, time.Now().Add(-time.Hour)))
	must(tx.Commit())
	s, err := store.New(db)
	must(err)
	started := make(chan struct{})
	release := make(chan struct{})
	oldObserved := make(chan relay.Event, 1)
	oldCtx, stopOld := context.WithCancel(ctx)
	defer stopOld()
	oldDone := make(chan error, 1)
	config := func(observe relay.Observer) relay.Config {
		return relay.Config{Concurrency: 1, PollInterval: 10 * time.Millisecond, Lease: time.Second, PublishTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
			return relay.RetryDecision{Delay: time.Second}
		}, Observe: observe}
	}
	old, err := relay.New(s, integrationPublisher(func(context.Context, message.Message) transport.Result {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return transport.Result{Outcome: transport.Confirmed}
	}), config(func(e relay.Event) {
		if e.Kind == "stale_write_rejected" || e.Kind == "write_failed" || e.Kind == "write_succeeded" {
			oldObserved <- e
			stopOld()
		}
	}))
	must(err)
	go func() { oldDone <- old.Run(oldCtx) }()
	// Even failed assertions must release the deliberately suspended callback.
	oldJoined := false
	defer func() {
		cancel()
		if oldJoined {
			return
		}
		select {
		case <-oldDone:
		case <-time.After(time.Second):
			t.Error("old Relay did not drain during cleanup")
		}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newCtx, stopNew := context.WithCancel(ctx)
	defer stopNew()
	newObserved := make(chan relay.Event, 1)
	delivered := make(chan message.Message, 1)
	replacement, err := relay.New(s, integrationPublisher(func(_ context.Context, got message.Message) transport.Result {
		delivered <- got
		return transport.Result{Outcome: transport.Confirmed}
	}), config(func(e relay.Event) {
		if e.Kind == "write_succeeded" || e.Kind == "write_failed" {
			newObserved <- e
			stopNew()
		}
	}))
	must(err)
	must(replacement.Run(newCtx))
	select {
	case e := <-newObserved:
		if e.Kind != "write_succeeded" {
			t.Fatalf("replacement: %+v", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := <-delivered; got.Fingerprint() != m.Fingerprint() {
		t.Fatal("replacement changed immutable message")
	}
	var before, after uint64
	must(db.QueryRowContext(ctx, "SELECT version FROM rm_outbox WHERE message_id='stable'").Scan(&before))
	close(release)
	select {
	case e := <-oldObserved:
		if e.Kind != "stale_write_rejected" {
			t.Fatalf("old relay: %+v", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case e := <-oldDone:
		must(e)
		oldJoined = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var state string
	var attempts int
	must(db.QueryRowContext(ctx, "SELECT version,state,attempt_count FROM rm_outbox WHERE message_id='stable'").Scan(&after, &state, &attempts))
	if before != after || state != "published" || attempts != 2 {
		t.Fatalf("late write changed recovery: versions=%d/%d state=%s attempts=%d", before, after, state, attempts)
	}
}
