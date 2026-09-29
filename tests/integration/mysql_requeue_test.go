//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLConfirmedRequeueWithinHostTransaction(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_REQUEUE_DSN")
	if dsn == "" {
		t.Fatal("isolated RM_TEST_MYSQL_REQUEUE_DSN required")
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
	_, err = db.ExecContext(ctx, `CREATE TABLE host_recovery_audit (request_id VARBINARY(64) PRIMARY KEY, decision VARCHAR(32) NOT NULL)`)
	must(err)
	in := message.Input{Producer: "qs", ID: "original", Destination: "evaluation", EventType: "evaluation.requested",
		SchemaVersion: "1", Scope: "org:1", ContentType: "application/json", OccurredAt: "2026-09-29T00:00:00Z",
		Payload: []byte(`{"frozen_model":"v1"}`)}
	m, err := message.New(in)
	must(err)
	tx, err := db.BeginTx(ctx, nil)
	must(err)
	appender, err := store.Bind(tx)
	must(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO host_recovery_audit VALUES ('original-business','created')`)
	must(err)
	must(appender.Append(ctx, m, time.Now().Add(-time.Minute)))
	must(tx.Commit())
	owner, err := store.New(db)
	must(err)
	claims, err := owner.ClaimDue(ctx, 1, time.Minute)
	must(err)
	if len(claims) != 1 {
		t.Fatalf("claims = %d", len(claims))
	}
	must(owner.Confirm(ctx, claims[0]))
	var id, version uint64
	var originalPayload []byte
	var confirmedAt string
	must(db.QueryRowContext(ctx, `SELECT id,version,payload,DATE_FORMAT(transport_confirmed_at,'%Y-%m-%d %H:%i:%s.%f') FROM rm_outbox WHERE message_id='original'`).
		Scan(&id, &version, &originalPayload, &confirmedAt))
	requeue := store.ConfirmedRequeue{RecordID: claims[0].RecordID, ExpectedVersion: version,
		ExpectedFingerprint: m.Fingerprint(), RequestID: "review-1"}
	assertPublished := func() {
		t.Helper()
		var state string
		var currentVersion uint64
		must(db.QueryRowContext(ctx, `SELECT state,version FROM rm_outbox WHERE id=?`, id).Scan(&state, &currentVersion))
		if state != "published" || currentVersion != version {
			t.Fatalf("unexpected original state %s/%d", state, currentVersion)
		}
	}
	for _, changed := range []store.ConfirmedRequeue{
		{RecordID: requeue.RecordID, ExpectedVersion: version + 1, ExpectedFingerprint: requeue.ExpectedFingerprint, RequestID: "stale"},
		{RecordID: requeue.RecordID, ExpectedVersion: version, RequestID: "bad-fingerprint"},
	} {
		if changed.ExpectedFingerprint == [32]byte{} {
			changed.ExpectedFingerprint[0] = 1
		}
		tx, err := db.BeginTx(ctx, nil)
		must(err)
		bound, err := store.Bind(tx)
		must(err)
		_, err = bound.RequeueConfirmed(ctx, changed)
		if !errors.Is(err, outbox.ErrStaleRequeue) {
			t.Fatalf("bad precondition accepted: %v", err)
		}
		must(tx.Rollback())
		assertPublished()
	}
	tx, err = db.BeginTx(ctx, nil)
	must(err)
	bound, err := store.Bind(tx)
	must(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO host_recovery_audit VALUES ('review-1','authorized')`)
	must(err)
	next, err := bound.RequeueConfirmed(ctx, requeue)
	must(err)
	if next != version+1 {
		t.Fatalf("next version = %d", next)
	}
	must(tx.Rollback())
	assertPublished()
	var n int
	must(db.QueryRowContext(ctx, `SELECT COUNT(*) FROM host_recovery_audit WHERE request_id='review-1'`).Scan(&n))
	if n != 0 {
		t.Fatal("audit survived rollback")
	}
	tx, err = db.BeginTx(ctx, nil)
	must(err)
	bound, err = store.Bind(tx)
	must(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO host_recovery_audit VALUES ('review-1','authorized')`)
	must(err)
	next, err = bound.RequeueConfirmed(ctx, requeue)
	must(err)
	must(tx.Commit())
	var state, auditID, confirmation string
	var after, attempts, failures, auditVersion uint64
	var payload []byte
	must(db.QueryRowContext(ctx, `SELECT state,version,payload,attempt_count,failure_count,manual_replay_request_id,
		manual_replay_version,DATE_FORMAT(transport_confirmed_at,'%Y-%m-%d %H:%i:%s.%f') FROM rm_outbox WHERE id=?`, id).
		Scan(&state, &after, &payload, &attempts, &failures, &auditID, &auditVersion, &confirmation))
	if state != "retry_wait" || after != next || auditVersion != next || auditID != "review-1" ||
		attempts != 1 || failures != 0 || confirmation != confirmedAt || !bytes.Equal(payload, originalPayload) {
		t.Fatalf("original changed unexpectedly: state=%s version=%d audit=%s/%d attempts=%d failures=%d", state, after, auditID, auditVersion, attempts, failures)
	}
	tx, err = db.BeginTx(ctx, nil)
	must(err)
	bound, err = store.Bind(tx)
	must(err)
	_, err = bound.RequeueConfirmed(ctx, requeue)
	if !errors.Is(err, outbox.ErrStaleRequeue) {
		t.Fatalf("duplicate request changed original row: %v", err)
	}
	must(tx.Rollback())
	claims, err = owner.ClaimDue(ctx, 1, time.Minute)
	must(err)
	if len(claims) != 1 || claims[0].Message.Input().ID != "original" {
		t.Fatal("Relay cannot reclaim original message")
	}
}
