// This M1 example shows the transaction bridge, not a released SDK API.
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

	"github.com/go-sql-driver/mysql"
)

type transactionAppender struct{ tx *sql.Tx }

// The host supplies its existing transaction. This constructor has no I/O.
func bindTransaction(tx *sql.Tx) (transactionAppender, error) {
	if tx == nil {
		return transactionAppender{}, errors.New("active host transaction required")
	}
	return transactionAppender{tx: tx}, nil
}

func (a transactionAppender) append(ctx context.Context, id string, payload []byte) error {
	_, err := a.tx.ExecContext(ctx, "INSERT INTO example_outbox (id, payload) VALUES (?, ?)", id, payload)
	return err
}

func writeIntent(ctx context.Context, db *sql.DB, id string, commit bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // Host owns rollback and commit, never the appender.
	appender, err := bindTransaction(tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO example_business (id) VALUES (?)", id); err != nil {
		return err
	}
	if err := appender.append(ctx, id, []byte(`{"event":"accepted"}`)); err != nil {
		return err
	}
	if !commit {
		return tx.Rollback()
	}
	// No MQ call here. A future Relay reads committed intents independently.
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
		"CREATE TABLE example_outbox (id VARCHAR(64) PRIMARY KEY, payload BLOB NOT NULL) ENGINE=InnoDB",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := bindTransaction(nil); err == nil {
		return errors.New("missing transaction accepted")
	}
	if err := writeIntent(ctx, db, "committed", true); err != nil {
		return err
	}
	if err := writeIntent(ctx, db, "rolled-back", false); err != nil {
		return err
	}
	for _, table := range []string{"example_business", "example_outbox"} {
		var committed, rolledBack int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE id='committed'").Scan(&committed); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE id='rolled-back'").Scan(&rolledBack); err != nil {
			return err
		}
		if committed != 1 || rolledBack != 0 {
			return fmt.Errorf("%s: commit=%d rollback=%d", table, committed, rolledBack)
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
