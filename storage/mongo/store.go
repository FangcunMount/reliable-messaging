package mongo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store uses standard proof documents, not qs-server's historical field mapping.
// Collection/client durability, indexes and lifecycle remain host-owned.
type Store struct{ collection *driver.Collection }

var _ outbox.Store = (*Store)(nil)

func New(collection *driver.Collection) (*Store, error) {
	if collection == nil {
		return nil, errors.New("host Mongo collection required")
	}
	return &Store{collection: collection}, nil
}

// Indexes must be installed explicitly by the host, outside business transactions.
// Validate real query plans before service rollout; $$NOW filters can constrain
// index usage differently across supported Mongo versions.
func Indexes() []driver.IndexModel {
	return []driver.IndexModel{
		{Keys: bson.D{{Key: "state", Value: 1}, {Key: "next_attempt_at", Value: 1}}},
		{Keys: bson.D{{Key: "state", Value: 1}, {Key: "lease_until", Value: 1}}},
		{Keys: bson.D{{Key: "next_attempt_at", Value: 1}, {Key: "_id", Value: 1}}, Options: options.Index().SetName("ix_rm_outbox_active_due").SetPartialFilterExpression(bson.M{"state": bson.M{"$in": bson.A{"pending", "retry_wait", "publishing"}}})},
	}
}
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]outbox.Claim, error) {
	if limit < 1 || limit > 1000 || lease < time.Millisecond || lease > 24*time.Hour {
		return nil, errors.New("limit 1..1000 and lease 1ms..24h required")
	}
	claims := make([]outbox.Claim, 0, limit)
	for i := 0; i < limit; i++ {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return claims, err
		}
		token := hex.EncodeToString(entropy[:])
		filter := bson.M{"state": bson.M{"$in": bson.A{"pending", "retry_wait", "publishing"}}, "$expr": bson.M{"$lte": bson.A{"$next_attempt_at", "$$NOW"}}, "$or": bson.A{
			bson.M{"state": bson.M{"$in": bson.A{"pending", "retry_wait"}}},
			bson.M{"state": "publishing", "$expr": bson.M{"$lte": bson.A{"$lease_until", "$$NOW"}}},
		}}
		// The sort key for publishing records is the lease expiry, not their
		// original append time. Both fields use the same server clock instant.
		leaseDue := bson.M{"$dateAdd": bson.M{"startDate": "$$NOW", "unit": "millisecond", "amount": lease.Milliseconds()}}
		update := driver.Pipeline{bson.D{{Key: "$set", Value: bson.M{"state": "publishing", "claim_token": token, "lease_until": leaseDue, "next_attempt_at": leaseDue, "version": bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$version", 0}}, 1}}, "attempt_count": bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$attempt_count", 0}}, 1}}, "updated_at": "$$NOW"}}}}
		var row struct {
			ID            bson.Raw  `bson:"_id"`
			Producer      string    `bson:"producer"`
			MessageID     string    `bson:"message_id"`
			Destination   string    `bson:"destination"`
			EventType     string    `bson:"event_type"`
			SchemaVersion string    `bson:"schema_version"`
			Scope         string    `bson:"scope"`
			ContentType   string    `bson:"content_type"`
			OccurredAt    string    `bson:"occurred_at"`
			Payload       []byte    `bson:"payload"`
			Fingerprint   []byte    `bson:"fingerprint"`
			Version       uint64    `bson:"version"`
			Attempts      uint64    `bson:"attempt_count"`
			FailureCount  uint64    `bson:"failure_count"`
			LeaseUntil    time.Time `bson:"lease_until"`
		}
		err := s.collection.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetSort(bson.D{{Key: "next_attempt_at", Value: 1}, {Key: "_id", Value: 1}}).SetReturnDocument(options.After)).Decode(&row)
		if errors.Is(err, driver.ErrNoDocuments) {
			break
		}
		if err != nil {
			return claims, err
		}
		c := outbox.Claim{RecordID: hex.EncodeToString(row.ID), Token: token, Version: row.Version, Attempts: row.Attempts, FailureCount: row.FailureCount, LeaseUntil: row.LeaseUntil}
		m, err := message.New(message.Input{Producer: row.Producer, ID: row.MessageID, Destination: row.Destination, EventType: row.EventType, SchemaVersion: row.SchemaVersion, Scope: row.Scope, ContentType: row.ContentType, OccurredAt: row.OccurredAt, Payload: row.Payload})
		hash := m.Fingerprint()
		if err != nil || !bytes.Equal(hash[:], row.Fingerprint) {
			if err = s.Quarantine(ctx, c, "invalid_immutable_content"); err != nil {
				return claims, err
			}
			continue
		}
		c.Message = m
		claims = append(claims, c)
	}
	return claims, nil
}
func (s *Store) Confirm(ctx context.Context, c outbox.Claim) error {
	return s.update(ctx, c, bson.M{"state": "published", "last_error_code": ""}, true, 0, false)
}
func (s *Store) Retry(ctx context.Context, c outbox.Claim, delay time.Duration, code string) error {
	if delay <= 0 || code == "" || len(code) > 128 {
		return errors.New("retry requires positive delay and bounded code")
	}
	return s.update(ctx, c, bson.M{"state": "retry_wait", "last_error_code": code}, false, delay, true)
}
func (s *Store) Quarantine(ctx context.Context, c outbox.Claim, code string) error {
	if code == "" || len(code) > 128 {
		return errors.New("bounded quarantine code required")
	}
	return s.update(ctx, c, bson.M{"state": "quarantined", "last_error_code": code}, false, 0, true)
}
func (s *Store) update(ctx context.Context, c outbox.Claim, set bson.M, confirm bool, delay time.Duration, failure bool) error {
	id, err := hex.DecodeString(c.RecordID)
	if err != nil || len(id) == 0 || bson.Raw(id).Validate() != nil || c.Token == "" || c.Version == 0 {
		return outbox.ErrStaleClaim
	}
	filter := bson.M{"_id": bson.Raw(id), "claim_token": c.Token, "version": c.Version, "state": "publishing", "$expr": bson.M{"$gt": bson.A{"$lease_until", "$$NOW"}}}
	// Update pipelines treat strings beginning with '$' as expressions unless
	// wrapped. Error codes are data, never Mongo expressions.
	fields := bson.M{}
	for key, value := range set {
		fields[key] = bson.M{"$literal": value}
	}
	fields["claim_token"] = "$$REMOVE"
	fields["lease_until"] = "$$REMOVE"
	fields["version"] = bson.M{"$add": bson.A{"$version", 1}}
	fields["updated_at"] = "$$NOW"
	if failure {
		fields["failure_count"] = bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$failure_count", 0}}, 1}}
	}
	if confirm {
		fields["transport_confirmed_at"] = "$$NOW"
	}
	if delay > 0 {
		millis := delay / time.Millisecond
		if delay%time.Millisecond != 0 {
			millis++
		}
		fields["next_attempt_at"] = bson.M{"$dateAdd": bson.M{"startDate": "$$NOW", "unit": "millisecond", "amount": int64(millis)}}
	}
	update := driver.Pipeline{bson.D{{Key: "$set", Value: fields}}}
	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("%w: matched %d", outbox.ErrStaleClaim, result.MatchedCount)
	}
	return nil
}
