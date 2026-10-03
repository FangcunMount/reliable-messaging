//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// This new adapter-only fixture uses an explicitly supplied disposable schema.
// It does not invoke the original Go M0-M6 acceptance scripts or any business host.
func ackBudgetFixture(t *testing.T) (*sql.DB, *Outbox, Identity) {
	t.Helper()
	dsn := os.Getenv("RM_MQ_DURABLE_TEST_DSN")
	if dsn == "" {
		t.Fatal("required isolated RM_MQ_DURABLE_TEST_DSN is missing")
	}
	configuration, err := driver.ParseDSN(dsn)
	if err != nil || (configuration.DBName != "rm_m7" && !strings.HasPrefix(configuration.DBName, "rm_ack_budget_")) {
		t.Fatal("explicitly named disposable adapter schema required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("isolated adapter storage unavailable")
	}
	created := false
	t.Cleanup(func() {
		if created {
			if _, err := db.Exec("DROP TABLE rm_durable_outbox"); err != nil {
				t.Error("isolated adapter table cleanup failed")
			}
		}
		if err := db.Close(); err != nil {
			t.Error("host test pool close failed")
		}
	})
	ddl, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(ddl)); err != nil {
		t.Fatal("required isolated adapter schema could not be created")
	}
	created = true
	store, err := New("rm_durable_outbox")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{"qs-server", "qs-ai", "original-final-ack"}
	_, err = db.Exec(`INSERT INTO rm_durable_outbox
 (producer,destination,message_id,body_sha256,body,wire,topic,aggregate_key,aggregate_sequence,ordered,requires_receipt,stage,attempts,available_at,created_at)
 VALUES(?,?,?,'original-hash',?,?, 'qs.ai.acks.v1','original',1,0,0,'staged',0,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, id.Producer, id.Destination, id.MessageID, []byte("original body"), []byte("original saved wire"))
	if err != nil {
		t.Fatal("original fixture append failed")
	}
	return db, store, id
}

func ackBudgetTx(t *testing.T, db *sql.DB, apply func(*sql.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error("host rollback failed")
		}
	}()
	if err = apply(tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestMQSuccessfulAckReplaysPreserveFailureBudgetAndOriginalWire(t *testing.T) {
	db, store, id := ackBudgetFixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		ackBudgetTx(t, db, func(tx *sql.Tx) error { return store.Retry(ctx, tx, id, "original-hash", 1, "publish_unknown") })
	}
	var original time.Time
	if err := db.QueryRow("SELECT created_at FROM rm_durable_outbox").Scan(&original); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		ackBudgetTx(t, db, func(tx *sql.Tx) error { return store.Published(ctx, tx, id, "original-hash", 30) })
		var stage string
		var attempts uint64
		var wire, body []byte
		var created time.Time
		if err := db.QueryRow("SELECT stage,attempts,wire,body,created_at FROM rm_durable_outbox").Scan(&stage, &attempts, &wire, &body, &created); err != nil {
			t.Fatal(err)
		}
		if stage != "confirmed" || attempts != 2 || string(wire) != "original saved wire" || string(body) != "original body" || !created.Equal(original) {
			t.Fatal("successful final ACK spent budget or changed original bytes/time")
		}
		ackBudgetTx(t, db, func(tx *sql.Tx) error { return store.RearmAck(ctx, tx, id, "original-hash") })
	}
	ackBudgetTx(t, db, func(tx *sql.Tx) error { return store.Retry(ctx, tx, id, "original-hash", 1, "publish_unknown") })
	var attempts uint64
	if err := db.QueryRow("SELECT attempts FROM rm_durable_outbox").Scan(&attempts); err != nil || attempts != 3 {
		t.Fatal("failed PUB budget was reset or not counted")
	}
	var usable int
	if err := db.QueryRow("SELECT 1").Scan(&usable); err != nil || usable != 1 {
		t.Fatal("SDK closed the host pool")
	}
}

func TestMQHeldAckCannotBeRearmedByCommittedDuplicate(t *testing.T) {
	db, store, id := ackBudgetFixture(t)
	ctx := context.Background()
	ackBudgetTx(t, db, func(tx *sql.Tx) error {
		for i := 0; i < 8; i++ {
			if err := store.Retry(ctx, tx, id, "original-hash", 1, "publish_unknown"); err != nil {
				return err
			}
		}
		return store.Hold(ctx, tx, id, "original-hash", "delivery_budget_exhausted")
	})
	ackBudgetTx(t, db, func(tx *sql.Tx) error {
		if err := store.RearmAck(ctx, tx, id, "original-hash"); err != nil {
			return err
		}
		return store.Published(ctx, tx, id, "original-hash", 30)
	})
	var stage, code string
	var attempts uint64
	if err := db.QueryRow("SELECT stage,error_code,attempts FROM rm_durable_outbox").Scan(&stage, &code, &attempts); err != nil {
		t.Fatal(err)
	}
	if stage != "held" || code != "delivery_budget_exhausted" || attempts != 8 {
		t.Fatal("duplicate revoked technical hold or renewed failed budget")
	}
	ackBudgetTx(t, db, func(tx *sql.Tx) error {
		rows, err := store.Pending(ctx, tx, 20)
		if err == nil && len(rows) != 0 {
			t.Fatal("technical hold became publishable")
		}
		return err
	})
}
