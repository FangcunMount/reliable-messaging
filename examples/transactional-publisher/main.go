// Executable public-API reference using a disposable MySQL database.
// The host owns schema setup, the business transaction and the connection pool.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	outboxmysql "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"github.com/go-sql-driver/mysql"
)

func writeIntent(ctx context.Context, db *sql.DB, id string, commit bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // Host owns rollback and commit, never the appender.
	appender, err := outboxmysql.Bind(tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO example_business (id) VALUES (?)", id); err != nil {
		return err
	}
	m, err := message.New(message.Input{
		Producer: "example", ID: id, Destination: "example.events", EventType: "accepted",
		SchemaVersion: "1", Scope: "synthetic", ContentType: "application/json",
		OccurredAt: "2026-10-06T00:00:00+08:00", Payload: []byte(`{"event":"accepted"}`),
	})
	if err != nil {
		return err
	}
	if err := appender.Append(ctx, m, time.Now()); err != nil {
		return err
	}
	if !commit {
		return tx.Rollback()
	}
	// No MQ call here. A host-started Relay reads committed intents separately.
	return tx.Commit()
}

func run() error {
	dsn := os.Getenv("RM_EXAMPLE_MYSQL_DSN")
	if dsn == "" {
		return errors.New("RM_EXAMPLE_MYSQL_DSN is required; use make integration")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil || cfg.Net != "tcp" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || !strings.HasPrefix(cfg.DBName, "rm_example_test") {
		return errors.New("example accepts only loopback TCP and an rm_example_test database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Explicit host-side setup for this disposable example, not constructor DDL.
	for _, statement := range []string{
		"CREATE TABLE example_business (id VARCHAR(64) PRIMARY KEY) ENGINE=InnoDB",
		outboxmysql.Schema,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := outboxmysql.Bind(nil); err == nil {
		return errors.New("missing transaction accepted")
	}
	if err := writeIntent(ctx, db, "committed", true); err != nil {
		return err
	}
	if err := writeIntent(ctx, db, "rolled-back", false); err != nil {
		return err
	}
	for _, row := range []struct{ table, key string }{{"example_business", "id"}, {"rm_outbox", "message_id"}} {
		var committed, rolledBack int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+row.table+" WHERE "+row.key+"='committed'").Scan(&committed); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+row.table+" WHERE "+row.key+"='rolled-back'").Scan(&rolledBack); err != nil {
			return err
		}
		if committed != 1 || rolledBack != 0 {
			return fmt.Errorf("%s: commit=%d rollback=%d", row.table, committed, rolledBack)
		}
	}
	fmt.Println("PASS host-owned SQL transaction: business and intent commit/rollback together")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
