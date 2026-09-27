package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/FangcunMount/reliable-messaging/outbox"
	adapter "github.com/FangcunMount/reliable-messaging/storage/mongo"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
)

const handoffLease = 3 * time.Second

func checkLeaseIdentity(c outbox.Claim, id string) error {
	in := c.Message.Input()
	if in.ID != id || in.Producer != "version-handoff" || in.Destination != "events" ||
		string(in.Payload) != `{"message_id":"`+id+`"}` {
		return fmt.Errorf("lease handoff changed identity or payload %q", in.ID)
	}
	return nil
}

func mysqlLeaseSeed(ctx context.Context, db *sql.DB, id string) error {
	if err := appendFact(ctx, db, id); err != nil {
		return err
	}
	s, err := store.New(db)
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(claims) != 1 {
		return fmt.Errorf("mysql lease seed claimed %d rows", len(claims))
	}
	if err := checkLeaseIdentity(claims[0], id); err != nil {
		return err
	}
	if claims[0].Attempts != 1 {
		return fmt.Errorf("mysql first lease attempts=%d", claims[0].Attempts)
	}
	return nil // Deliberately leave the real released Store's lease in-flight.
}

func mysqlLeaseRecover(ctx context.Context, db *sql.DB, id string) error {
	var rowID, version uint64
	var token []byte
	var remainingMicros int64
	var state string
	err := db.QueryRowContext(ctx, `SELECT id,claim_token,version,state,
 TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),lease_until) FROM rm_outbox WHERE message_id=?`, id).
		Scan(&rowID, &token, &version, &state, &remainingMicros)
	if err != nil {
		return err
	}
	if state != "publishing" || len(token) == 0 || remainingMicros < 100000 {
		return fmt.Errorf("mysql old lease not live: state=%s remaining_us=%d", state, remainingMicros)
	}
	old := outbox.Claim{RecordID: strconv.FormatUint(rowID, 10), Token: string(token), Version: version}
	s, err := store.New(db)
	if err != nil {
		return err
	}
	early, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(early) != 0 {
		return fmt.Errorf("mysql claimed %d rows before old lease expired", len(early))
	}
	time.Sleep(time.Duration(remainingMicros)*time.Microsecond + 50*time.Millisecond)
	claims, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(claims) != 1 {
		return fmt.Errorf("mysql recovered %d rows, want one", len(claims))
	}
	fresh := claims[0]
	if err := checkLeaseIdentity(fresh, id); err != nil {
		return err
	}
	if fresh.RecordID != old.RecordID || fresh.Token == old.Token || fresh.Version <= old.Version || fresh.Attempts != 2 {
		return fmt.Errorf("mysql lease recovery did not fence old claim: version=%d attempts=%d", fresh.Version, fresh.Attempts)
	}
	if err := s.Confirm(ctx, old); !errors.Is(err, outbox.ErrStaleClaim) {
		return fmt.Errorf("mysql old lease write was not fenced: %v", err)
	}
	if err := s.Confirm(ctx, fresh); err != nil {
		return err
	}
	var finalState string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, `SELECT state,attempt_count,failure_count FROM rm_outbox WHERE message_id=?`, id).
		Scan(&finalState, &attempts, &failures); err != nil {
		return err
	}
	if finalState != "published" || attempts != 2 || failures != 0 {
		return fmt.Errorf("mysql lease final state=%s attempts=%d failures=%d", finalState, attempts, failures)
	}
	return nil
}

func mongoLeaseSeed(ctx context.Context, client *driver.Client, id string) error {
	if err := mongoAppendFact(ctx, client, id); err != nil {
		return err
	}
	s, err := adapter.New(client.Database(database).Collection("rm_outbox"))
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(claims) != 1 {
		return fmt.Errorf("mongo lease seed claimed %d rows", len(claims))
	}
	if err := checkLeaseIdentity(claims[0], id); err != nil {
		return err
	}
	if claims[0].Attempts != 1 {
		return fmt.Errorf("mongo first lease attempts=%d", claims[0].Attempts)
	}
	return nil // Deliberately leave the real released Store's lease in-flight.
}

func mongoLeaseRecover(ctx context.Context, client *driver.Client, id string) error {
	collection := client.Database(database).Collection("rm_outbox")
	var row struct {
		ID         bson.Raw  `bson:"_id"`
		Token      string    `bson:"claim_token"`
		Version    uint64    `bson:"version"`
		State      string    `bson:"state"`
		LeaseUntil time.Time `bson:"lease_until"`
	}
	if err := collection.FindOne(ctx, bson.M{"message_id": id}).Decode(&row); err != nil {
		return err
	}
	remaining := time.Until(row.LeaseUntil)
	if row.State != "publishing" || row.Token == "" || remaining < 100*time.Millisecond {
		return fmt.Errorf("mongo old lease not live: state=%s remaining=%s", row.State, remaining)
	}
	old := outbox.Claim{RecordID: hex.EncodeToString(row.ID), Token: row.Token, Version: row.Version}
	s, err := adapter.New(collection)
	if err != nil {
		return err
	}
	early, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(early) != 0 {
		return fmt.Errorf("mongo claimed %d rows before old lease expired", len(early))
	}
	time.Sleep(remaining + 50*time.Millisecond)
	claims, err := s.ClaimDue(ctx, 1, handoffLease)
	if err != nil {
		return err
	}
	if len(claims) != 1 {
		return fmt.Errorf("mongo recovered %d rows, want one", len(claims))
	}
	fresh := claims[0]
	if err := checkLeaseIdentity(fresh, id); err != nil {
		return err
	}
	if fresh.RecordID != old.RecordID || fresh.Token == old.Token || fresh.Version <= old.Version || fresh.Attempts != 2 {
		return fmt.Errorf("mongo lease recovery did not fence old claim: version=%d attempts=%d", fresh.Version, fresh.Attempts)
	}
	if err := s.Confirm(ctx, old); !errors.Is(err, outbox.ErrStaleClaim) {
		return fmt.Errorf("mongo old lease write was not fenced: %v", err)
	}
	if err := s.Confirm(ctx, fresh); err != nil {
		return err
	}
	var final struct {
		State        string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
	}
	if err := collection.FindOne(ctx, bson.M{"message_id": id}).Decode(&final); err != nil {
		return err
	}
	if final.State != "published" || final.Attempts != 2 || final.FailureCount != 0 {
		return fmt.Errorf("mongo lease final state=%s attempts=%d failures=%d", final.State, final.Attempts, final.FailureCount)
	}
	return nil
}
