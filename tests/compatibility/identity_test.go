// M0 reference checker, not an SDK API or a production migration tool.
package compatibility

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/FangcunMount/reliable-messaging/message"
)

var fields = []string{"producer", "message_id", "destination", "event_type", "schema_version", "tenant", "content_type", "occurred_at", "payload"}

type vector struct {
	Name        string            `json:"name"`
	Message     map[string]string `json:"message"`
	Valid       bool              `json:"valid"`
	Fingerprint string            `json:"expected_fingerprint"`
}

func fingerprint(m map[string]string) string {
	h := sha256.New()
	h.Write([]byte("rm-fingerprint-draft-v1\x00"))
	for _, key := range fields {
		value := []byte(m[key])
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write(value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func valid(m map[string]string) bool {
	for _, key := range fields {
		if m[key] == "" {
			return false
		}
	}
	var wire map[string]json.RawMessage
	if json.Unmarshal([]byte(m["payload"]), &wire) != nil {
		return false
	}
	stringField := func(raw json.RawMessage) string {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return ""
		}
		return value
	}
	switch m["wire_kind"] {
	case "iam-raw":
		var version int64
		return m["tenant"] == "scope:global" &&
			json.Unmarshal(wire["version"], &version) == nil && version > 0
	case "qs-envelope":
		var data map[string]json.RawMessage
		if json.Unmarshal(wire["data"], &data) != nil {
			return false
		}
		return stringField(wire["id"]) == m["message_id"] &&
			stringField(wire["eventType"]) == m["event_type"] &&
			stringField(wire["occurredAt"]) == m["occurred_at"] &&
			string(bytes.TrimSpace(data["org_id"])) == m["tenant"]
	default:
		return false
	}
}

func verifyVectors(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file struct {
		Cases []vector `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		return err
	}
	if len(file.Cases) != 10 {
		return fmt.Errorf("expected 10 vectors, got %d", len(file.Cases))
	}
	seen := make(map[string]string)
	for _, v := range file.Cases {
		got := fingerprint(v.Message)
		if got != v.Fingerprint {
			return fmt.Errorf("%s: fingerprint mismatch", v.Name)
		}
		if valid(v.Message) != v.Valid {
			return fmt.Errorf("%s: validation mismatch", v.Name)
		}
		seen[v.Name] = got
		fmt.Printf("PASS %s (valid=%t)\n", v.Name, v.Valid)
	}
	for _, pair := range [][2]string{{"iam-original", "iam-same-replay"}, {"qs-extension-preserved", "qs-same-replay"}} {
		if seen[pair[0]] != seen[pair[1]] {
			return fmt.Errorf("replay identity changed: %v", pair)
		}
	}
	for _, name := range []string{"iam-content-conflict", "iam-scope-conflict", "iam-format-conflict"} {
		if seen[name] == seen["iam-original"] {
			return fmt.Errorf("conflict hidden: %s", name)
		}
	}
	fmt.Println("PASS 10 vectors and replay/conflict relations")
	return nil
}

func TestIdentityReferenceVectors(t *testing.T) {
	if err := verifyVectors("../../contracts/fixtures/identity-vectors.json"); err != nil {
		t.Fatal(err)
	}
}

// Host wire validation stays outside the generic SDK: invalid host envelopes
// may still be well-formed immutable delivery intents.
func TestSDKIdentityAndPayloadOwnership(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/fixtures/identity-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []vector `json:"cases"`
	}
	if err = json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Cases {
		t.Run(v.Name, func(t *testing.T) {
			x := v.Message
			in := message.Input{Producer: x["producer"], ID: x["message_id"], Destination: x["destination"], EventType: x["event_type"], SchemaVersion: x["schema_version"], Scope: x["tenant"], ContentType: x["content_type"], OccurredAt: x["occurred_at"], Payload: []byte(x["payload"])}
			m, err := message.New(in)
			if x["producer"] == "" {
				if err == nil {
					t.Fatal("missing identity accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			hash := m.Fingerprint()
			if hex.EncodeToString(hash[:]) != v.Fingerprint {
				t.Fatal("SDK differs from contract vector")
			}
			in.Payload[0] = '!'
			out := m.Input()
			out.Payload[0] = '?'
			if string(m.Input().Payload) != x["payload"] || m.Fingerprint() != hash {
				t.Fatal("payload aliased")
			}
		})
	}
}
