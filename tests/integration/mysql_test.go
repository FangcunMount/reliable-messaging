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
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
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
	if fresh.Token == old.Token || fresh.Version <= old.Version || fresh.Attempts != 2 || fresh.Message.Fingerprint() != m.Fingerprint() {
		t.Fatal("reclaim identity/fencing violated")
	}
	for _, err := range []error{s.Confirm(ctx, old), s.Retry(ctx, old, time.Now(), "network"), s.Quarantine(ctx, old, "invalid")} {
		if !errors.Is(err, outbox.ErrStaleClaim) {
			t.Fatalf("stale writer: %v", err)
		}
	}
	must(s.Retry(ctx, fresh, time.Now().Add(time.Hour), "unknown"))
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
	must(s.Confirm(ctx, claims[0]))
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
	seen := map[uint64]bool{}
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
	if _, err = store.Bind(nil); err == nil {
		t.Fatal("missing transaction accepted")
	}
}
