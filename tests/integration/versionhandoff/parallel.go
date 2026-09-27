package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	adapter "github.com/FangcunMount/reliable-messaging/storage/mongo"
	store "github.com/FangcunMount/reliable-messaging/storage/mysql"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
)

const parallelID = "parallel-claim"

func participant() (string, error) {
	label := os.Getenv("RM_HANDOFF_PARTICIPANT")
	if label != "old" && label != "new" {
		return "", errors.New("released binary participant must be old or new")
	}
	return label, nil
}

func waitForBoth(ctx context.Context, count func(context.Context) (int64, error)) error {
	for {
		n, err := count(ctx)
		if err != nil {
			return err
		}
		if n == 2 {
			return nil
		}
		if n > 2 {
			return fmt.Errorf("parallel barrier has %d participants", n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func mysqlParallelSeed(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE handoff_barrier (participant VARCHAR(8) PRIMARY KEY, claimed BOOLEAN NULL) ENGINE=InnoDB`); err != nil {
		return err
	}
	return appendFact(ctx, db, parallelID)
}

func mysqlParallelClaim(ctx context.Context, db *sql.DB) error {
	label, err := participant()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO handoff_barrier(participant) VALUES (?)", label); err != nil {
		return err
	}
	if err := waitForBoth(ctx, func(ctx context.Context) (int64, error) {
		var count int64
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM handoff_barrier").Scan(&count)
		return count, err
	}); err != nil {
		return err
	}
	s, err := store.New(db)
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 1, 10*time.Second)
	if err != nil {
		return err
	}
	if len(claims) > 1 {
		return fmt.Errorf("%s claimed %d rows", label, len(claims))
	}
	claimed := len(claims) == 1
	if claimed {
		if err := checkLeaseIdentity(claims[0], parallelID); err != nil {
			return err
		}
		if claims[0].Attempts != 1 {
			return fmt.Errorf("%s parallel attempts=%d", label, claims[0].Attempts)
		}
	}
	if _, err := db.ExecContext(ctx, "UPDATE handoff_barrier SET claimed=? WHERE participant=?", claimed, label); err != nil {
		return err
	}
	if claimed {
		// The loser must finish ClaimDue before this winner can confirm. A
		// fixed sleep would not prove that both versions overlapped on the lease.
		if err := waitForBoth(ctx, func(ctx context.Context) (int64, error) {
			var count int64
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM handoff_barrier WHERE claimed IS NOT NULL").Scan(&count)
			return count, err
		}); err != nil {
			return err
		}
		if err := s.Confirm(ctx, claims[0]); err != nil {
			return err
		}
	}
	fmt.Printf("MySQL parallel %s claimed=%t\n", label, claimed)
	return nil
}

func mysqlParallelVerify(ctx context.Context, db *sql.DB) error {
	var participants, winners int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(claimed),0) FROM handoff_barrier").Scan(&participants, &winners); err != nil {
		return err
	}
	var state string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, "SELECT state,attempt_count,failure_count FROM rm_outbox WHERE message_id=?", parallelID).
		Scan(&state, &attempts, &failures); err != nil {
		return err
	}
	if participants != 2 || winners != 1 || state != "published" || attempts != 1 || failures != 0 {
		return fmt.Errorf("mysql parallel participants=%d winners=%d state=%s attempts=%d failures=%d", participants, winners, state, attempts, failures)
	}
	fmt.Println("PASS MySQL released versions concurrently claimed one original intent exactly once")
	return nil
}

func mongoParallelSeed(ctx context.Context, client *driver.Client) error {
	return mongoAppendFact(ctx, client, parallelID)
}

func mongoParallelClaim(ctx context.Context, client *driver.Client) error {
	label, err := participant()
	if err != nil {
		return err
	}
	barrier := client.Database(database).Collection("handoff_barrier")
	if _, err := barrier.InsertOne(ctx, bson.M{"_id": label}); err != nil {
		return err
	}
	if err := waitForBoth(ctx, func(ctx context.Context) (int64, error) {
		return barrier.CountDocuments(ctx, bson.M{})
	}); err != nil {
		return err
	}
	s, err := adapter.New(client.Database(database).Collection("rm_outbox"))
	if err != nil {
		return err
	}
	claims, err := s.ClaimDue(ctx, 1, 10*time.Second)
	if err != nil {
		return err
	}
	if len(claims) > 1 {
		return fmt.Errorf("%s claimed %d Mongo documents", label, len(claims))
	}
	claimed := len(claims) == 1
	if claimed {
		if err := checkLeaseIdentity(claims[0], parallelID); err != nil {
			return err
		}
		if claims[0].Attempts != 1 {
			return fmt.Errorf("%s Mongo parallel attempts=%d", label, claims[0].Attempts)
		}
	}
	if _, err := barrier.UpdateByID(ctx, label, bson.M{"$set": bson.M{"claimed": claimed}}); err != nil {
		return err
	}
	if claimed {
		if err := waitForBoth(ctx, func(ctx context.Context) (int64, error) {
			return barrier.CountDocuments(ctx, bson.M{"claimed": bson.M{"$exists": true}})
		}); err != nil {
			return err
		}
		if err := s.Confirm(ctx, claims[0]); err != nil {
			return err
		}
	}
	fmt.Printf("Mongo parallel %s claimed=%t\n", label, claimed)
	return nil
}

func mongoParallelVerify(ctx context.Context, client *driver.Client) error {
	barrier := client.Database(database).Collection("handoff_barrier")
	participants, err := barrier.CountDocuments(ctx, bson.M{})
	if err != nil {
		return err
	}
	winners, err := barrier.CountDocuments(ctx, bson.M{"claimed": true})
	if err != nil {
		return err
	}
	var final struct {
		State        string `bson:"state"`
		Attempts     int    `bson:"attempt_count"`
		FailureCount int    `bson:"failure_count"`
	}
	if err := client.Database(database).Collection("rm_outbox").FindOne(ctx, bson.M{"message_id": parallelID}).Decode(&final); err != nil {
		return err
	}
	if participants != 2 || winners != 1 || final.State != "published" || final.Attempts != 1 || final.FailureCount != 0 {
		return fmt.Errorf("mongo parallel participants=%d winners=%d state=%s attempts=%d failures=%d", participants, winners, final.State, final.Attempts, final.FailureCount)
	}
	fmt.Println("PASS Mongo released versions concurrently claimed one original intent exactly once")
	return nil
}
