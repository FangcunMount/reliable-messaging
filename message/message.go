// Package message defines provisional immutable delivery intents.
// The API remains provisional until the M2 dual-storage proofs complete.
package message

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

// Input contains application identity, not a broker-generated delivery ID.
// Scope is explicit (for example scope:global or a host-defined tenant ID).
// Destination is a stable logical route, not an ephemeral physical queue.
type Input struct {
	Producer, ID, Destination                                string
	EventType, SchemaVersion, Scope, ContentType, OccurredAt string
	Payload                                                  []byte
}

// Message owns its payload. Accessors return copies so mutation by publishers
// cannot silently change the next attempt's durable content.
type Message struct {
	input       Input
	fingerprint [32]byte
}

func New(in Input) (Message, error) {
	fields := []struct {
		name, value string
		limit       int
	}{
		{"producer", in.Producer, 128}, {"id", in.ID, 128}, {"destination", in.Destination, 255},
		{"event_type", in.EventType, 255}, {"schema_version", in.SchemaVersion, 128},
		{"scope", in.Scope, 255}, {"content_type", in.ContentType, 255}, {"occurred_at", in.OccurredAt, 64},
	}
	for _, f := range fields {
		if f.value == "" || len(f.value) > f.limit || !utf8.ValidString(f.value) {
			return Message{}, fmt.Errorf("invalid %s: required UTF-8, at most %d bytes", f.name, f.limit)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, in.OccurredAt); err != nil {
		return Message{}, fmt.Errorf("occurred_at: %w", err)
	}
	if len(in.Payload) == 0 {
		return Message{}, errors.New("payload is required")
	}
	in.Payload = append([]byte(nil), in.Payload...)
	h := sha256.New()
	h.Write([]byte("rm-fingerprint-draft-v1\x00"))
	for _, value := range [][]byte{[]byte(in.Producer), []byte(in.ID), []byte(in.Destination), []byte(in.EventType), []byte(in.SchemaVersion), []byte(in.Scope), []byte(in.ContentType), []byte(in.OccurredAt), in.Payload} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write(value)
	}
	m := Message{input: in}
	copy(m.fingerprint[:], h.Sum(nil))
	return m, nil
}

func (m Message) Input() Input {
	in := m.input
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}
func (m Message) Fingerprint() [32]byte { return m.fingerprint }
func (m Message) Valid() bool           { return m.input.Producer != "" }
