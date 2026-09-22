// Package mongo contains a provisional original-transaction proof adapter.
// Historical qs-server document mapping and the claim store remain separate.
package mongo

import (
	"bytes"
	"errors"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrTransactionRequired = errors.New("active original Mongo transaction required")

type Appender struct {
	ctx        driver.SessionContext
	collection *driver.Collection
}

// Bind borrows the host callback's active transaction; it never starts, commits
// or retries transactions. Prepare immutable messages outside WithTransaction.
func Bind(ctx driver.SessionContext, collection *driver.Collection) (*Appender, error) {
	if collection == nil || !active(ctx) {
		return nil, ErrTransactionRequired
	}
	return &Appender{ctx: ctx, collection: collection}, nil
}
func active(ctx driver.SessionContext) bool {
	if ctx == nil {
		return false
	}
	// Mongo driver v1 lacks a stable public transaction-state accessor. Keep this
	// pinned-version dependency inside this adapter and prove it with real tests.
	s, ok := driver.SessionFromContext(ctx).(driver.XSession) //nolint:staticcheck // Deliberate v1.17.6 compatibility bridge; no fallback to nontransactional writes.
	return ok && s.ClientSession() != nil && s.ClientSession().TransactionRunning()
}
func (a *Appender) Append(m message.Message, due time.Time) error {
	if a == nil || a.collection == nil || !active(a.ctx) {
		return ErrTransactionRequired
	}
	if !m.Valid() || due.IsZero() {
		return errors.New("valid message and due time required")
	}
	in := m.Input()
	hash := m.Fingerprint()
	identity := bson.D{{Key: "producer", Value: in.Producer}, {Key: "message_id", Value: in.ID}, {Key: "destination", Value: in.Destination}}
	doc := bson.M{"_id": identity, "producer": in.Producer, "message_id": in.ID, "destination": in.Destination, "event_type": in.EventType, "schema_version": in.SchemaVersion, "scope": in.Scope, "content_type": in.ContentType, "occurred_at": in.OccurredAt, "payload": in.Payload, "fingerprint": hash[:], "state": "pending", "next_attempt_at": due.UTC(), "version": int64(0)}
	filter := bson.M{"_id": identity}
	if _, err := a.collection.UpdateOne(a.ctx, filter, bson.M{"$setOnInsert": doc}, options.Update().SetUpsert(true)); err != nil {
		return err
	}
	var existing struct {
		Fingerprint []byte `bson:"fingerprint"`
	}
	if err := a.collection.FindOne(a.ctx, filter).Decode(&existing); err != nil {
		return err
	}
	if !bytes.Equal(existing.Fingerprint, hash[:]) {
		return outbox.ErrConflict
	}
	return nil
}
