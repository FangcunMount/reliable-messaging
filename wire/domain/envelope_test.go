package domain

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

type contractEvent struct {
	ID            string         `json:"id"`
	Type          string         `json:"eventType"`
	At            time.Time      `json:"occurredAt"`
	Aggregate     string         `json:"aggregateType"`
	AggregateKey  string         `json:"aggregateID"`
	BusinessValue map[string]any `json:"data"`
}

func (e contractEvent) EventID() string       { return e.ID }
func (e contractEvent) EventType() string     { return e.Type }
func (e contractEvent) OccurredAt() time.Time { return e.At }
func (e contractEvent) AggregateType() string { return e.Aggregate }
func (e contractEvent) AggregateID() string   { return e.AggregateKey }

func TestHistoricalDomainWireAndMetadata(t *testing.T) {
	at := time.Date(2026, 9, 27, 10, 11, 12, 123456789, time.FixedZone("UTC+8", 8*60*60))
	evt := contractEvent{
		ID: "event-1", Type: "assessment.requested", At: at,
		Aggregate: "Assessment", AggregateKey: "42", BusinessValue: map[string]any{"org_id": 501},
	}
	encoded, err := EncodeEvent(evt)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"id":"event-1","eventType":"assessment.requested","occurredAt":"2026-09-27T10:11:12.123456789+08:00","aggregateType":"Assessment","aggregateID":"42","data":{"org_id":501}}`)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("historical domain bytes changed: got %s, want %s", encoded, want)
	}
	decoded, err := DecodeEnvelope(encoded)
	if err != nil || decoded.ID != evt.ID || decoded.EventType != evt.Type || string(decoded.Data) != `{"org_id":501}` {
		t.Fatalf("decoded historical event = %+v, %v", decoded, err)
	}
	metadata := MetadataFromEvent(evt, "api-server")
	if metadata["event_type"] != evt.Type || metadata["aggregate_type"] != evt.Aggregate || metadata["aggregate_id"] != evt.AggregateKey ||
		metadata["occurred_at"] != "2026-09-27T10:11:12.123+08:00" || metadata["source"] != "api-server" {
		t.Fatalf("historical event metadata changed: %+v", metadata)
	}
}

func TestDecodeEnvelopeRejectsInvalidJSON(t *testing.T) {
	if _, err := DecodeEnvelope([]byte("{")); err == nil || !strings.Contains(err.Error(), "failed to parse event envelope") {
		t.Fatalf("invalid JSON error = %v", err)
	}
}
