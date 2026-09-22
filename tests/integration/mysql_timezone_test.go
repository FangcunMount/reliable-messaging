//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	driver "github.com/go-sql-driver/mysql"
)

func TestMySQLTimezoneIndependentScheduling(t *testing.T) {
	dsn := os.Getenv("RM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("isolated RM_TEST_MYSQL_DSN required")
	}
	base, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	sequence := 0
	for _, offset := range []int{0, 8} {
		for _, sessionZone := range []string{"+00:00", "+08:00"} {
			for _, interpolate := range []bool{false, true} {
				sequence++
				t.Run(fmt.Sprintf("loc%d/session%s/interpolate%v", offset, sessionZone, interpolate), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					must := func(err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
					}
					name := fmt.Sprintf("rm_tz_%d_%d", time.Now().UnixNano(), sequence)
					_, err := admin.ExecContext(ctx, "CREATE DATABASE "+name)
					must(err)
					t.Cleanup(func() {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cleanupCancel()
						_, cleanupErr := admin.ExecContext(cleanupCtx, "DROP DATABASE "+name)
						if cleanupErr != nil {
							t.Error(cleanupErr)
						}
					})
					open := func(hours int) *sql.DB {
						cfg := base.Clone()
						cfg.DBName = name
						cfg.ParseTime = true
						cfg.Loc = time.FixedZone("test-location", hours*3600)
						cfg.InterpolateParams = interpolate
						if cfg.Params == nil {
							cfg.Params = make(map[string]string)
						}
						cfg.Params["time_zone"] = "'" + sessionZone + "'"
						connector, connectorErr := driver.NewConnector(cfg)
						must(connectorErr)
						pool := sql.OpenDB(connector)
						t.Cleanup(func() { _ = pool.Close() })
						return pool
					}
					db, utc := open(offset), open(0)
					_, err = db.ExecContext(ctx, store.Schema)
					must(err)
					appendMessage := func(id string, due time.Time) {
						t.Helper()
						tx, txErr := db.BeginTx(ctx, nil)
						must(txErr)
						defer tx.Rollback()
						a, bindErr := store.Bind(tx)
						must(bindErr)
						m, messageErr := message.New(message.Input{Producer: "timezone-proof", ID: id, Destination: "events", EventType: "changed", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T08:00:00+08:00", Payload: []byte(`{"version":1}`)})
						must(messageErr)
						must(a.Append(ctx, m, due))
						must(tx.Commit())
					}
					due := time.Now().Add(-time.Second).Truncate(time.Microsecond)
					appendMessage("immediate", due)
					var persistedDue string
					must(utc.QueryRowContext(ctx, "SELECT DATE_FORMAT(next_attempt_at,'%Y-%m-%d %H:%i:%s.%f') FROM rm_outbox WHERE message_id='immediate'").Scan(&persistedDue))
					if persistedDue != due.UTC().Format("2006-01-02 15:04:05.000000") {
						t.Fatalf("host driver changed due instant: got %s, want %s", persistedDue, due.UTC().Format("2006-01-02 15:04:05.000000"))
					}
					localStore, err := store.New(db)
					must(err)
					utcStore, err := store.New(utc)
					must(err)
					claim := func(s *store.Store) outbox.Claim {
						t.Helper()
						claims, claimErr := s.ClaimDue(ctx, 1, 10*time.Second)
						must(claimErr)
						if len(claims) != 1 {
							t.Fatalf("expected one due message, got %d", len(claims))
						}
						remaining := time.Until(claims[0].LeaseUntil)
						if remaining < 8*time.Second || remaining > 11*time.Second {
							t.Fatalf("claim lease has wrong instant: %s", remaining)
						}
						return claims[0]
					}
					first := claim(localStore)
					must(utcStore.Retry(ctx, first, 30*time.Second, "fixture_retry"))
					claims, err := localStore.ClaimDue(ctx, 1, 10*time.Second)
					must(err)
					if len(claims) != 0 {
						t.Fatal("retry became due early")
					}
					var retryMicros int64
					must(utc.QueryRowContext(ctx, "SELECT TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),next_attempt_at) FROM rm_outbox WHERE id=?", first.RecordID).Scan(&retryMicros))
					if retryMicros < 28_000_000 || retryMicros > 30_000_000 {
						t.Fatalf("retry shifted by connection timezone: %d us", retryMicros)
					}
					// Force only the fixture's due time to exercise cross-pool recovery
					// without sleeping for the positive retry delay.
					_, err = utc.ExecContext(ctx, "UPDATE rm_outbox SET next_attempt_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", first.RecordID)
					must(err)
					second := claim(utcStore)
					must(localStore.Confirm(ctx, second))
					appendMessage("future", time.Now().Add(time.Minute))
					claims, err = utcStore.ClaimDue(ctx, 1, 10*time.Second)
					must(err)
					if len(claims) != 0 {
						t.Fatal("future message became due early")
					}
				})
			}
		}
	}
}
