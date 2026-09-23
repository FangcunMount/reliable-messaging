//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestMySQLAdditiveFailureCountUpgrade(t *testing.T) {
	config, err := mysqlDriver.ParseDSN(os.Getenv("RM_TEST_MYSQL_DSN"))
	if err != nil || config.DBName != "rm_sdk_test" || config.Addr != "127.0.0.1:3306" || config.User != "root" {
		t.Fatal("disposable rm_sdk_test MySQL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	const database = "rm_sdk_failure_upgrade"
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	defer admin.ExecContext(context.Background(), "DROP DATABASE "+database)
	config.DBName = database
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	oldSchema := strings.Replace(store.Schema, "  failure_count BIGINT UNSIGNED NOT NULL DEFAULT 0,\n", "", 1)
	if oldSchema == store.Schema {
		t.Fatal("old schema fixture no longer omits failure_count")
	}
	if _, err := db.ExecContext(ctx, oldSchema); err != nil {
		t.Fatal(err)
	}
	appendMessage := func(id string) message.Message {
		t.Helper()
		m, err := message.New(message.Input{
			Producer: "iam", ID: id, Destination: "events", EventType: "changed",
			SchemaVersion: "1", Scope: "scope:global", ContentType: "application/json",
			OccurredAt: "2026-09-22T08:00:00+08:00", Payload: []byte(`{"id":"` + id + `"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		appender, err := store.Bind(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err := appender.Append(ctx, m, time.Now().Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return m
	}
	retained := appendMessage("published-before-upgrade")
	pending := appendMessage("pending-before-upgrade")
	if _, err := db.ExecContext(ctx, "UPDATE rm_outbox SET state='published' WHERE message_id=?", retained.Input().ID); err != nil {
		t.Fatal(err)
	}
	s, err := store.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimDue(ctx, 1, time.Minute); err == nil {
		t.Fatal("new store unexpectedly accepted schema without failure_count")
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE rm_outbox ADD COLUMN failure_count BIGINT UNSIGNED NOT NULL DEFAULT 0"); err != nil {
		t.Fatal(err)
	}
	// The old Appender SQL still works after the additive column arrives.
	appendMessage("written-after-upgrade")
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rm_outbox WHERE message_id=? AND state='published' AND payload=? AND failure_count=0", retained.Input().ID, retained.Input().Payload).Scan(&count); err != nil || count != 1 {
		t.Fatalf("published evidence changed: count=%d err=%v", count, err)
	}
	claims, err := s.ClaimDue(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 2 {
		t.Fatalf("pending rows after upgrade: %d", len(claims))
	}
	for _, claim := range claims {
		if claim.FailureCount != 0 {
			t.Fatalf("old row initialized with failure_count=%d", claim.FailureCount)
		}
		if claim.Message.Input().ID == pending.Input().ID && claim.Message.Fingerprint() != pending.Fingerprint() {
			t.Fatal("pending immutable content changed during upgrade")
		}
		if err := s.Confirm(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}
