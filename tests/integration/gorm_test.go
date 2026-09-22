//go:build integration

package integration

import (
	"context"
	"errors"
	"github.com/FangcunMount/reliable-messaging/message"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

func TestGORMOriginalTransactionBridge(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated DSN required")
	}
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "prepared"}[prepared], func(t *testing.T) {
			db, err := gorm.Open(driver.Open(dsn), &gorm.Config{PrepareStmt: prepared})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := db.DB()
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			db = db.WithContext(ctx)
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(db.Exec(store.Schema).Error)
			must(db.Exec("CREATE TABLE IF NOT EXISTS gorm_business (id VARCHAR(64) PRIMARY KEY)").Error)
			defer raw.Exec("DELETE FROM rm_outbox WHERE producer='gorm-proof'")
			defer raw.Exec("DELETE FROM gorm_business")
			if _, err = store.BindGORM(db); err == nil {
				t.Fatal("ordinary GORM DB accepted")
			}
			rollback := errors.New("host abort")
			var retained *store.Appender
			for _, id := range []string{"committed", "rolled-back"} {
				err = db.Transaction(func(tx *gorm.DB) error {
					if err := tx.Exec("INSERT INTO gorm_business VALUES (?)", id).Error; err != nil {
						return err
					}
					appender, err := store.BindGORM(tx)
					if err != nil {
						return err
					}
					retained = appender
					m, err := message.New(message.Input{Producer: "gorm-proof", ID: id, Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"version":1}`)})
					if err != nil {
						return err
					}
					if err = appender.Append(ctx, m, time.Now()); err != nil {
						return err
					}
					if id == "rolled-back" {
						return rollback
					}
					return nil
				})
				if id == "committed" {
					must(err)
				} else if !errors.Is(err, rollback) {
					t.Fatalf("rollback: %v", err)
				}
			}
			var business, intents int64
			must(db.Raw("SELECT COUNT(*) FROM gorm_business WHERE id='committed'").Scan(&business).Error)
			must(db.Raw("SELECT COUNT(*) FROM rm_outbox WHERE producer='gorm-proof' AND message_id='committed'").Scan(&intents).Error)
			if business != 1 || intents != 1 {
				t.Fatalf("commit split: %d %d", business, intents)
			}
			must(db.Raw("SELECT COUNT(*) FROM gorm_business WHERE id='rolled-back'").Scan(&business).Error)
			must(db.Raw("SELECT COUNT(*) FROM rm_outbox WHERE producer='gorm-proof' AND message_id='rolled-back'").Scan(&intents).Error)
			if business != 0 || intents != 0 {
				t.Fatal("rollback escaped original transaction")
			}
			m, err := message.New(message.Input{Producer: "gorm-proof", ID: "late", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{}`)})
			must(err)
			if err = retained.Append(ctx, m, time.Now()); err == nil {
				t.Fatal("completed transaction reused")
			}
		})
	}
}
