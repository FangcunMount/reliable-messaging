package compatibility

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
