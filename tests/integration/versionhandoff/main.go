// versionhandoff is built separately against two immutable released SDK tags.
// It runs only against a disposable MySQL container, never a service database.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

const database = "rm_sdk_version_handoff"

var ids = map[string]bool{
	"old-published": true,
	"old-pending":   true,
	"old-after-ddl": true,
	"new-pending":   true,
}

func open() (*sql.DB, error) {
	config, err := mysqlDriver.ParseDSN(os.Getenv("RM_HANDOFF_MYSQL_DSN"))
	if err != nil || config.DBName != database || config.Addr != "127.0.0.1:3306" || config.User != "root" {
		return nil, errors.New("disposable local MySQL DSN required")
	}
	return sql.Open("mysql", config.FormatDSN())
}

func appendFact(ctx context.Context, db *sql.DB, id string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO host_fact (message_id) VALUES (?)", id); err != nil {
		return err
	}
	m, err := message.New(message.Input{
		Producer: "version-handoff", ID: id, Destination: "events",
		EventType: "assessment.requested", SchemaVersion: "1", Scope: "scope:global",
		ContentType: "application/json", OccurredAt: "2026-09-27T08:00:00+08:00",
		Payload: []byte(`{"message_id":"` + id + `"}`),
	})
	if err != nil {
		return err
	}
	appender, err := store.Bind(tx)
	if err != nil {
		return err
	}
	if err := appender.Append(ctx, m, time.Now().Add(-time.Minute)); err != nil {
		return err
	}
	return tx.Commit()
}

func drain(ctx context.Context, db *sql.DB, expected ...string) error {
	s, err := store.New(db)
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 10, time.Minute)
	if err != nil {
		return err
	}
	if len(claims) != len(expected) {
		return fmt.Errorf("claimed %d rows, want %d", len(claims), len(expected))
	}
	want := make(map[string]bool, len(expected))
	for _, id := range expected {
		want[id] = true
	}
	for _, claim := range claims {
		in := claim.Message.Input()
		id := in.ID
		if !want[id] || !ids[id] || in.Producer != "version-handoff" || in.Destination != "events" ||
			in.EventType != "assessment.requested" || string(in.Payload) != `{"message_id":"`+id+`"}` {
			return fmt.Errorf("unexpected old/new identity or bytes: %q", id)
		}
		delete(want, id)
		if err := s.Confirm(ctx, claim); err != nil {
			return err
		}
	}
	if len(want) != 0 {
		return fmt.Errorf("missing claims: %v", want)
	}
	return nil
}

func mysqlRetryState(ctx context.Context, db *sql.DB, code string, wantAttempts, wantFailures int, quarantine bool) error {
	s, err := store.New(db)
	if err != nil {
		return err
	}
	var claim outbox.Claim
	claimed := false
	for attempt := 0; attempt < 50; attempt++ {
		claims, err := s.ClaimDue(ctx, 1, time.Minute)
		if err != nil {
			return err
		}
		if len(claims) == 1 {
			claim, claimed = claims[0], true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !claimed {
		return errors.New("original retry identity was not due")
	}
	in := claim.Message.Input()
	if in.ID != "state-retry" || in.Producer != "version-handoff" || in.Destination != "events" || string(in.Payload) != `{"message_id":"state-retry"}` {
		return fmt.Errorf("unexpected retry identity or bytes %q", in.ID)
	}
	if quarantine {
		err = s.Quarantine(ctx, claim, code)
	} else {
		err = s.Retry(ctx, claim, 10*time.Millisecond, code)
	}
	if err != nil {
		return err
	}
	var state, lastCode string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, "SELECT state,attempt_count,failure_count,last_error_code FROM rm_outbox WHERE message_id=?", "state-retry").Scan(&state, &attempts, &failures, &lastCode); err != nil {
		return err
	}
	wantState := "retry_wait"
	if quarantine {
		wantState = "quarantined"
	}
	if state != wantState || attempts != wantAttempts || failures != wantFailures || lastCode != code {
		return fmt.Errorf("mysql retry state=%s attempts=%d failures=%d code=%s, want %s/%d/%d/%s", state, attempts, failures, lastCode, wantState, wantAttempts, wantFailures, code)
	}
	return nil
}

func run(ctx context.Context, db *sql.DB, phase string) error {
	switch phase {
	case "old-seed":
		if _, err := db.ExecContext(ctx, store.Schema); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "CREATE TABLE host_fact (message_id VARBINARY(128) PRIMARY KEY) ENGINE=InnoDB"); err != nil {
			return err
		}
		if err := appendFact(ctx, db, "old-published"); err != nil {
			return err
		}
		if err := drain(ctx, db, "old-published"); err != nil {
			return err
		}
		if err := appendFact(ctx, db, "old-pending"); err != nil {
			return err
		}
		fmt.Println("PASS v0.1.0 original transaction left one published and one pending")
	case "new-before-ddl":
		s, err := store.New(db)
		if err != nil {
			return err
		}
		if _, err := s.ClaimDue(ctx, 1, time.Minute); err == nil {
			return errors.New("v0.2.1 Store accepted old schema without required columns")
		} else {
			var mysqlErr *mysqlDriver.MySQLError
			if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1054 {
				return fmt.Errorf("expected missing-column failure, got %w", err)
			}
		}
		fmt.Println("PASS v0.2.1 refused old schema before additive migration")
	case "old-after-ddl":
		if err := appendFact(ctx, db, "old-after-ddl"); err != nil {
			return err
		}
		fmt.Println("PASS actual v0.1.0 Appender still wrote on additive schema")
	case "new-drain":
		if err := drain(ctx, db, "old-pending", "old-after-ddl"); err != nil {
			return err
		}
		if err := appendFact(ctx, db, "new-pending"); err != nil {
			return err
		}
		fmt.Println("PASS actual v0.2.1 drained original v0.1.0 identities and left new pending")
	case "old-drain":
		if err := drain(ctx, db, "new-pending"); err != nil {
			return err
		}
		var facts, total, published, distinct int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM host_fact").Scan(&facts); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*), SUM(state='published'), COUNT(DISTINCT message_id) FROM rm_outbox").Scan(&total, &published, &distinct); err != nil {
			return err
		}
		if facts != 4 || total != 4 || published != 4 || distinct != 4 {
			return fmt.Errorf("final facts=%d outbox=%d published=%d distinct=%d", facts, total, published, distinct)
		}
		fmt.Println("PASS actual v0.1.0 drained ordinary v0.2.1 pending; four host facts and original identities remain")
	case "old-retry-seed":
		if err := appendFact(ctx, db, "state-retry"); err != nil {
			return err
		}
		if err := mysqlRetryState(ctx, db, "old_failure", 1, 0, false); err != nil {
			return err
		}
		fmt.Println("PASS MySQL v0.1.0 retry retained original identity and did not write future failure_count")
	case "new-retry":
		if err := mysqlRetryState(ctx, db, "new_failure", 2, 1, false); err != nil {
			return err
		}
		fmt.Println("PASS MySQL v0.2.1 retry incremented failure_count on old intent")
	case "old-retry-downgrade":
		if err := mysqlRetryState(ctx, db, "old_after_downgrade", 3, 1, false); err != nil {
			return err
		}
		fmt.Println("OBSERVED MySQL v0.1.0 retry after downgrade did not advance failure_count")
	case "new-retry-quarantine":
		if err := mysqlRetryState(ctx, db, "quarantine_after_downgrade", 4, 2, true); err != nil {
			return err
		}
		fmt.Println("PASS MySQL v0.2.1 quarantined original identity; diagnostic failure_count undercounts old retries")
	default:
		return fmt.Errorf("unknown phase %q", phase)
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: versionhandoff old-seed|new-before-ddl|old-after-ddl|new-drain|old-drain|mongo-...")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if strings.HasPrefix(os.Args[1], "mongo-") {
		client, err := mongoClient(ctx)
		if err == nil {
			defer client.Disconnect(context.Background())
			err = runMongo(ctx, client, os.Args[1])
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	db, err := open()
	if err == nil {
		defer db.Close()
		err = run(ctx, db, os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
