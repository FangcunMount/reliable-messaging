//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/relay"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"github.com/FangcunMount/reliable-messaging/transport"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLTransactionAndFencing(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated RM_TEST_MYSQL_DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.ExecContext(ctx, store.Schema)
	must(err)
	_, err = db.ExecContext(ctx, "CREATE TABLE business (id BIGINT PRIMARY KEY)")
	must(err)
	in := message.Input{Producer: "test", ID: "committed", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"version":1}`)}
	appendTx := func(id int, commit bool) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		must(err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "INSERT INTO business VALUES (?)", id)
		must(err)
		a, err := store.Bind(tx)
		must(err)
		m, err := message.New(in)
		must(err)
		must(a.Append(ctx, m, time.Now().Add(-time.Minute)))
		if commit {
			must(tx.Commit())
		} else {
			must(tx.Rollback())
		}
	}
	appendTx(1, true)
	in.ID = "rolled-back"
	appendTx(2, false)
	var n int
	must(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM business").Scan(&n))
	if n != 1 {
		t.Fatalf("business count %d", n)
	}
	must(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rm_outbox").Scan(&n))
	if n != 1 {
		t.Fatalf("outbox count %d", n)
	}
	in.ID = "committed"
	tx, err := db.BeginTx(ctx, nil)
	must(err)
	a, err := store.Bind(tx)
	must(err)
	m, err := message.New(in)
	must(err)
	must(a.Append(ctx, m, time.Now()))
	in.Payload = []byte(`{"version":2}`)
	changed, err := message.New(in)
	must(err)
	if err = a.Append(ctx, changed, time.Now()); !errors.Is(err, outbox.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	must(tx.Rollback())
	s, err := store.New(db)
	must(err)
	claims, err := s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatalf("claims: %d", len(claims))
	}
	old := claims[0]
	if old.FailureCount != 0 {
		t.Fatalf("initial failure count %d", old.FailureCount)
	}
	again, err := s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(again) != 0 {
		t.Fatal("live claim stolen")
	}
	// Advance only this isolated record's lease, avoiding timing-dependent sleeps.
	_, err = db.ExecContext(ctx, "UPDATE rm_outbox SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", old.RecordID)
	must(err)
	if err = s.Confirm(ctx, old); !errors.Is(err, outbox.ErrStaleClaim) {
		t.Fatalf("expired confirm: %v", err)
	}
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatal("expired lease not reclaimed")
	}
	fresh := claims[0]
	if fresh.Token == old.Token || fresh.Version <= old.Version || fresh.Attempts != 2 || fresh.FailureCount != 0 || fresh.Message.Fingerprint() != m.Fingerprint() {
		t.Fatal("reclaim identity/fencing violated")
	}
	for _, err := range []error{s.Confirm(ctx, old), s.Retry(ctx, old, time.Second, "network"), s.Quarantine(ctx, old, "invalid")} {
		if !errors.Is(err, outbox.ErrStaleClaim) {
			t.Fatalf("stale writer: %v", err)
		}
	}
	for _, delay := range []time.Duration{0, -time.Second} {
		if e := s.Retry(ctx, fresh, delay, "unknown"); e == nil {
			t.Fatal("nonpositive retry delay accepted")
		}
	}
	must(s.Retry(ctx, fresh, time.Hour, "unknown"))
	var failures uint64
	must(db.QueryRowContext(ctx, "SELECT failure_count FROM rm_outbox WHERE id=?", fresh.RecordID).Scan(&failures))
	if failures != 1 {
		t.Fatalf("retry failure count %d", failures)
	}
	var remainingMicros int64
	must(db.QueryRowContext(ctx, "SELECT TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),next_attempt_at) FROM rm_outbox WHERE id=?", fresh.RecordID).Scan(&remainingMicros))
	if remainingMicros > time.Hour.Microseconds() || remainingMicros < (time.Hour-10*time.Second).Microseconds() {
		t.Fatalf("retry not relative to DB clock: %d us", remainingMicros)
	}

	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("retry before due")
	}
	_, err = db.ExecContext(ctx, "UPDATE rm_outbox SET next_attempt_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND")
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
	must(db.QueryRowContext(ctx, "SELECT failure_count FROM rm_outbox WHERE id=?", fresh.RecordID).Scan(&failures))
	if failures != 1 {
		t.Fatalf("confirm changed failure count %d", failures)
	}
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 0 {
		t.Fatal("published record reclaimed")
	}

	// Independently scheduled workers must never receive the same live record.
	in.Payload = []byte(`{"version":1}`)
	for i := 3; i < 11; i++ {
		in.ID = fmt.Sprintf("concurrent-%d", i)
		appendTx(i, true)
	}
	type result struct {
		claims []outbox.Claim
		err    error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() { cs, e := s.ClaimDue(ctx, 8, time.Minute); results <- result{cs, e} }()
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		r := <-results
		must(r.err)
		for _, c := range r.claims {
			if seen[c.RecordID] {
				t.Fatal("duplicate live claim")
			}
			seen[c.RecordID] = true
			must(s.Confirm(ctx, c))
		}
	}
	if len(seen) != 8 {
		t.Fatalf("claimed %d of 8", len(seen))
	}
	// Corruption is preserved in quarantine, while valid work still advances.
	in.ID = "corrupt"
	appendTx(11, true)
	in.ID = "valid-after-corrupt"
	appendTx(12, true)
	_, err = db.ExecContext(ctx, "UPDATE rm_outbox SET payload='tampered' WHERE message_id='corrupt'")
	must(err)
	claims, err = s.ClaimDue(ctx, 10, time.Minute)
	must(err)
	if len(claims) != 1 || claims[0].Message.Input().ID != "valid-after-corrupt" {
		t.Fatal("corrupt row starved valid work")
	}
	must(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rm_outbox WHERE message_id='corrupt' AND state='quarantined' AND payload='tampered'").Scan(&n))
	if n != 1 {
		t.Fatal("corruption evidence lost")
	}
	must(s.Quarantine(ctx, claims[0], "host_rejected"))
	must(db.QueryRowContext(ctx, "SELECT failure_count FROM rm_outbox WHERE message_id='corrupt'").Scan(&failures))
	if failures != 1 {
		t.Fatalf("corruption quarantine failure count %d", failures)
	}
	must(db.QueryRowContext(ctx, "SELECT failure_count FROM rm_outbox WHERE message_id='valid-after-corrupt'").Scan(&failures))
	if failures != 1 {
		t.Fatalf("host quarantine failure count %d", failures)
	}
	if _, err = store.Bind(nil); err == nil {
		t.Fatal("missing transaction accepted")
	}
}

// This injects an actual MySQL write rejection. The publisher is an in-process
// transport double; broker confirmation-loss/crash tests remain a separate gate.
func TestRelayRecoversRealDatabaseWriteFailure(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.ExecContext(ctx, store.Schema)
	must(err)
	_, err = db.ExecContext(ctx, "DELETE FROM rm_outbox")
	must(err)
	in := message.Input{Producer: "relay", ID: "write-failure", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"version":7,"extension":"preserved"}`)}
	m, err := message.New(in)
	must(err)
	tx, err := db.BeginTx(ctx, nil)
	must(err)
	defer tx.Rollback()
	a, err := store.Bind(tx)
	must(err)
	must(a.Append(ctx, m, time.Now().Add(-time.Second)))
	must(tx.Commit())
	_, err = db.ExecContext(ctx, `CREATE TRIGGER fail_confirmation BEFORE UPDATE ON rm_outbox FOR EACH ROW BEGIN IF NEW.state='published' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated confirmation write failure'; END IF; END`)
	must(err)
	defer db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS fail_confirmation")
	s, err := store.New(db)
	must(err)
	var sent []message.Message
	publisher := integrationPublisher(func(_ context.Context, m message.Message) transport.Result {
		sent = append(sent, m)
		return transport.Result{Outcome: transport.Confirmed}
	})
	run := func(want string) {
		t.Helper()
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		observed := ""
		r, err := relay.New(s, publisher, relay.Config{Concurrency: 1, PollInterval: time.Millisecond, Lease: 3 * time.Second, PublishTimeout: time.Second, WriteTimeout: time.Second, Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
			return relay.RetryDecision{Delay: time.Second}
		}, Observe: func(e relay.Event) {
			if e.Kind == "write_failed" || e.Kind == "write_succeeded" {
				observed = e.Kind
				stop()
			}
		}})
		must(err)
		must(r.Run(runCtx))
		if observed != want {
			t.Fatalf("observed %q want %q", observed, want)
		}
	}
	run("write_failed")
	var state string
	must(db.QueryRowContext(ctx, "SELECT state FROM rm_outbox WHERE message_id=?", in.ID).Scan(&state))
	if state != "publishing" {
		t.Fatalf("lost recoverable record: %s", state)
	}
	_, err = db.ExecContext(ctx, "DROP TRIGGER fail_confirmation")
	must(err)
	_, err = db.ExecContext(ctx, "UPDATE rm_outbox SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND")
	must(err)
	run("write_succeeded")
	if len(sent) != 2 || sent[0].Fingerprint() != m.Fingerprint() || sent[1].Fingerprint() != m.Fingerprint() || string(sent[1].Input().Payload) != string(in.Payload) {
		t.Fatal("recovery changed original identity/bytes")
	}
	must(db.QueryRowContext(ctx, "SELECT state FROM rm_outbox WHERE message_id=?", in.ID).Scan(&state))
	if state != "published" {
		t.Fatalf("not confirmed: %s", state)
	}
}

type integrationPublisher func(context.Context, message.Message) transport.Result

func (f integrationPublisher) Publish(ctx context.Context, m message.Message) transport.Result {
	return f(ctx, m)
}
