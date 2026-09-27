package compatibility

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/FangcunMount/reliable-messaging/wire/legacy"
)

// These are bytes emitted by the exact component-base versions used by IAM
// and qs-server. This test freezes historical input, not SDK decode support.
func TestHistoricalComponentBaseEnvelopeFixtures(t *testing.T) {
	tests := []struct {
		name     string
		sha256   string
		revision int
		checksum bool
	}{
		{"component-base-v0.6.1-envelope.json", "c50be18211392a079b5f27b367aea39d41f1bb06f170024625d4c4e49c2749a7", 0, false},
		{"component-base-v0.6.11-envelope.json", "adcae1ddf3528b939bbab6dd4fb5b6a72ab87485d3139e7ef76e9718af03e397", 2, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := os.ReadFile(filepath.Join("testdata", tc.name))
			if err != nil {
				t.Fatal(err)
			}
			if len(wire) == 0 || wire[len(wire)-1] != '\n' {
				t.Fatal("fixture must end with exactly one repository newline")
			}
			wire = wire[:len(wire)-1] // fixture files end with one repository newline
			sum := sha256.Sum256(wire)
			if got := hex.EncodeToString(sum[:]); got != tc.sha256 {
				t.Fatalf("historical bytes changed: %s", got)
			}
			var envelope struct {
				Type           string            `json:"type"`
				SchemaRevision int               `json:"schema_revision"`
				UUID           string            `json:"uuid"`
				Metadata       map[string]string `json:"metadata"`
				Payload        []byte            `json:"payload"`
				Checksum       string            `json:"checksum"`
			}
			if err := json.Unmarshal(wire, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Type != "component-base.messaging.message.v1" ||
				envelope.SchemaRevision != tc.revision ||
				envelope.UUID != "550e8400-e29b-41d4-a716-446655440000" ||
				envelope.Metadata["source"] != "iam-outbox-relay" ||
				string(envelope.Payload) != `{"event_type":"policy.changed","subject":"青岛店"}` ||
				(envelope.Checksum != "") != tc.checksum {
				t.Fatalf("historical identity or payload changed: %#v", envelope)
			}
		})
	}
}

func TestLegacyEnvelopeCodecMatchesReleasedVersions(t *testing.T) {
	input := legacy.Envelope{
		UUID: "550e8400-e29b-41d4-a716-446655440000",
		Metadata: map[string]string{
			"source": "iam-outbox-relay", "event_type": "policy.changed", "trace_id": "contract-fixture",
		},
		Payload: []byte(`{"event_type":"policy.changed","subject":"青岛店"}`),
	}
	for _, tc := range []struct {
		name     string
		revision legacy.Revision
	}{
		{"component-base-v0.6.1-envelope.json", legacy.Revision1},
		{"component-base-v0.6.11-envelope.json", legacy.Revision2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			golden, err := os.ReadFile(filepath.Join("testdata", tc.name))
			if err != nil {
				t.Fatal(err)
			}
			golden = bytes.TrimSuffix(golden, []byte("\n"))
			wire, err := legacy.Encode(input, tc.revision)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wire, golden) {
				t.Fatalf("revision %d differs from released encoder\ngot:  %s\nwant: %s", tc.revision, wire, golden)
			}
			decoded, recognized, err := legacy.Decode(golden)
			if err != nil || !recognized || decoded.UUID != input.UUID || !bytes.Equal(decoded.Payload, input.Payload) || decoded.Metadata["source"] != input.Metadata["source"] {
				t.Fatalf("historical decode = %#v, %t, %v", decoded, recognized, err)
			}
			originalWire := append([]byte(nil), golden...)
			decoded.Metadata["source"] = "changed"
			decoded.Payload[0] = '!'
			if !bytes.Equal(golden, originalWire) {
				t.Fatal("decoded payload aliases wire input")
			}
		})
	}
	bad, err := os.ReadFile(filepath.Join("testdata", "component-base-v0.6.11-bad-checksum.json"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, recognized, err := legacy.Decode(bad)
	if err == nil || !recognized || decoded.UUID != input.UUID {
		t.Fatalf("corrupt checked revision = %#v, %t, %v", decoded, recognized, err)
	}
}

func TestLegacyFailedHandoffMatchesReleasedNSQRecord(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "component-base-v0.6.11-failed-handoff.json"))
	if err != nil {
		t.Fatal(err)
	}
	golden = bytes.TrimSuffix(golden, []byte("\n"))
	const expectedSHA = "74a96fe3360490c8381924aa7630bb4150a358b2b45b83acb00b944bf4b2aefb"
	sum := sha256.Sum256(golden)
	if hex.EncodeToString(sum[:]) != expectedSHA {
		t.Fatal("historical failed-handoff bytes changed")
	}
	input := legacy.FailedHandoff{
		Topic: "qs.evaluation.lifecycle", Channel: "qs-worker",
		UUID: "550e8400-e29b-41d4-a716-446655440000", TransportMessageID: "0123456789abcdef",
		Metadata: map[string]string{"source": "qs-worker", "event_type": "evaluation.failed"},
		Payload:  []byte(`{"event_type":"evaluation.failed","subject":"青岛店"}`),
		Attempts: 8, Timestamp: 123456789, Cause: "handler failed",
	}
	wire, err := legacy.EncodeFailedHandoff(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, golden) {
		t.Fatalf("failed handoff differs from released encoder\ngot: %s\nwant: %s", wire, golden)
	}
	decoded, err := legacy.DecodeFailedHandoff(golden)
	if err != nil || decoded.UUID != input.UUID || decoded.TransportMessageID != input.TransportMessageID || decoded.Topic != input.Topic || decoded.Channel != input.Channel || decoded.Attempts != input.Attempts || !bytes.Equal(decoded.Payload, input.Payload) {
		t.Fatalf("failed handoff decode = %#v, %v", decoded, err)
	}
	if got := legacy.FailedHandoffTopic(input.Topic, input.Channel); got != "cb.failed.b6abe5a59ae6cb4454f2a271" {
		t.Fatalf("active QS Worker handoff topic changed: %s", got)
	}
}
