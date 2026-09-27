// Package legacy preserves the message envelope already used by component-base
// consumers. It does not define business event payloads or broker identities.
package legacy

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"
)

const envelopeType = "component-base.messaging.message.v1"

// Revision selects the wire bytes emitted by a historical producer.
// Revision1 omits the revision and checksum fields; Revision2 includes both.
type Revision uint8

const (
	Revision1 Revision = 1
	Revision2 Revision = 2
)

// Envelope carries the application identity and payload within the broker body.
// UUID is not an NSQ physical message ID. Callers own the returned fields.
type Envelope struct {
	UUID     string
	Metadata map[string]string
	Payload  []byte
}

type encodedEnvelope struct {
	Type           string            `json:"type"`
	SchemaRevision int               `json:"schema_revision,omitempty"`
	UUID           string            `json:"uuid,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Payload        []byte            `json:"payload"`
	Checksum       string            `json:"checksum,omitempty"`
}

// Encode produces the historical bytes for an explicitly selected revision.
// The caller must select the version expected by its deployed consumers.
func Encode(value Envelope, revision Revision) ([]byte, error) {
	if revision != Revision1 && revision != Revision2 {
		return nil, fmt.Errorf("unsupported legacy envelope revision %d", revision)
	}
	encoded := encodedEnvelope{
		Type: envelopeType, UUID: value.UUID, Metadata: copyMetadata(value.Metadata),
		Payload: append([]byte(nil), value.Payload...),
	}
	if revision == Revision2 {
		encoded.SchemaRevision = int(Revision2)
		encoded.Checksum = checksum(encoded.UUID, encoded.Metadata, encoded.Payload)
	}
	return json.Marshal(encoded)
}

// Decode returns recognized=false for unmarked or invalid raw payloads. Once
// the historical discriminator is recognized, corrupt revisions return an
// error and must not be acknowledged as a successful business message.
// A partial envelope is returned with recognized errors for failure auditing.
func Decode(body []byte) (value Envelope, recognized bool, err error) {
	var probe struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &probe) != nil || probe.Type != envelopeType {
		return Envelope{}, false, nil
	}
	var encoded encodedEnvelope
	if err := json.Unmarshal(body, &encoded); err != nil {
		var recoverable struct {
			UUID     string            `json:"uuid"`
			Metadata map[string]string `json:"metadata"`
		}
		_ = json.Unmarshal(body, &recoverable)
		return Envelope{UUID: recoverable.UUID, Metadata: copyMetadata(recoverable.Metadata), Payload: append([]byte(nil), body...)}, true,
			fmt.Errorf("decode recognized legacy envelope: %w", err)
	}
	value = Envelope{UUID: encoded.UUID, Metadata: copyMetadata(encoded.Metadata), Payload: append([]byte(nil), encoded.Payload...)}
	switch encoded.SchemaRevision {
	case 0, int(Revision1):
		return value, true, nil
	case int(Revision2):
		if encoded.Checksum == "" {
			return value, true, errors.New("legacy envelope checksum is required")
		}
		expected := checksum(encoded.UUID, encoded.Metadata, encoded.Payload)
		if subtle.ConstantTimeCompare([]byte(encoded.Checksum), []byte(expected)) != 1 {
			return value, true, errors.New("legacy envelope checksum mismatch")
		}
		return value, true, nil
	default:
		return value, true, fmt.Errorf("unsupported legacy envelope revision %d", encoded.SchemaRevision)
	}
}

func checksum(uuid string, metadata map[string]string, payload []byte) string {
	digest := sha256.New()
	writePart(digest, []byte(uuid))
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writePart(digest, []byte(key))
		writePart(digest, []byte(metadata[key]))
	}
	writePart(digest, payload)
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func writePart(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func copyMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
