// Package domain preserves the JSON event envelope carried inside historical
// broker messages. The host owns concrete event values and business payloads.
package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

const OccurredAtLayout = "2006-01-02T15:04:05.000Z07:00"

// Event is the structural input needed to derive historical message metadata.
// It does not make the SDK the owner of business event types.
type Event interface {
	EventID() string
	EventType() string
	OccurredAt() time.Time
	AggregateType() string
	AggregateID() string
}

// Envelope is the domain JSON nested inside a transport envelope.
type Envelope struct {
	ID            string          `json:"id"`
	EventType     string          `json:"eventType"`
	OccurredAt    time.Time       `json:"occurredAt"`
	AggregateType string          `json:"aggregateType"`
	AggregateID   string          `json:"aggregateID"`
	Data          json.RawMessage `json:"data"`
}

// EncodeEvent preserves the host event's existing JSON representation.
func EncodeEvent(evt Event) ([]byte, error) {
	if evt == nil {
		return nil, fmt.Errorf("domain event is nil")
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}
	return payload, nil
}

// DecodeEnvelope parses the domain JSON without interpreting its business data.
func DecodeEnvelope(payload []byte) (*Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("failed to parse event envelope: %w", err)
	}
	return &envelope, nil
}

// MetadataFromEvent preserves the historical metadata keys and time layout.
func MetadataFromEvent(evt Event, source string) map[string]string {
	if evt == nil {
		return map[string]string{}
	}
	return map[string]string{
		"event_type":     evt.EventType(),
		"aggregate_type": evt.AggregateType(),
		"aggregate_id":   evt.AggregateID(),
		"occurred_at":    evt.OccurredAt().Format(OccurredAtLayout),
		"source":         source,
	}
}
