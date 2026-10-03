package protected

import (
	"encoding/json"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
)

const NSQMaxBytes = 262144
const MaxMetadataBytes = 4096
const MaxCauseBytes = 1024

// FitsNSQ checks both normal wire and the worst recognized/invalid failure handoff.
// Oversize bodies require a host-owned immutable reference, not a broker config change.
func FitsNSQ(value legacy.Envelope, topic, channel string) bool {
	meta, err := json.Marshal(value.Metadata)
	if err != nil || len(meta) > MaxMetadataBytes {
		return false
	}
	wire, err := legacy.Encode(value, legacy.Revision2)
	if err != nil {
		return false
	}
	cause := make([]byte, MaxCauseBytes)
	for i := range cause {
		cause[i] = 'x'
	}
	handoff, err := legacy.EncodeFailedHandoff(legacy.FailedHandoff{
		Topic: topic, Channel: channel, UUID: value.UUID, Metadata: value.Metadata, Payload: wire,
		TransportMessageID: "0000000000000000", Attempts: 65535, Timestamp: 9223372036854775807, Cause: string(cause),
	})
	return err == nil && len(wire) <= NSQMaxBytes && len(handoff) <= NSQMaxBytes
}
