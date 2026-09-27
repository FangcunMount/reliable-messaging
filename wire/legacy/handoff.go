package legacy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	failedEnvelopeType = "component-base.messaging.failed.v1"
	// FailedHandoffChannel is the historical NSQ channel for durable failures.
	FailedHandoffChannel = "cb-failed-handler"
)

// FailedHandoff retains both application and physical delivery identities.
// It is a transport failure record, not proof that business work completed.
type FailedHandoff struct {
	Topic              string
	Channel            string
	UUID               string
	TransportMessageID string
	Metadata           map[string]string
	Payload            []byte
	Attempts           int
	Timestamp          int64
	Cause              string
}

type encodedFailedHandoff struct {
	Type               string            `json:"type"`
	Provider           string            `json:"provider"`
	Topic              string            `json:"topic"`
	Channel            string            `json:"channel"`
	UUID               string            `json:"uuid"`
	TransportMessageID string            `json:"transport_message_id,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	Payload            []byte            `json:"payload"`
	Attempts           int               `json:"attempts"`
	Timestamp          int64             `json:"timestamp,omitempty"`
	Cause              string            `json:"cause"`
}

// EncodeFailedHandoff produces the NSQ failure record emitted by
// component-base v0.6.11. A caller must publish it before finishing the
// original message; a failed or unknown publish cannot authorize a FIN.
func EncodeFailedHandoff(value FailedHandoff) ([]byte, error) {
	if value.Topic == "" || value.Channel == "" || value.UUID == "" || value.Attempts < 1 || value.Cause == "" {
		return nil, errors.New("invalid legacy NSQ failed handoff")
	}
	return json.Marshal(encodedFailedHandoff{
		Type: failedEnvelopeType, Provider: "nsq", Topic: value.Topic, Channel: value.Channel,
		UUID: value.UUID, TransportMessageID: value.TransportMessageID,
		Metadata: copyMetadata(value.Metadata), Payload: append([]byte(nil), value.Payload...),
		Attempts: value.Attempts, Timestamp: value.Timestamp, Cause: value.Cause,
	})
}

// DecodeFailedHandoff accepts only the historical NSQ failure envelope.
func DecodeFailedHandoff(body []byte) (FailedHandoff, error) {
	var encoded encodedFailedHandoff
	if err := json.Unmarshal(body, &encoded); err != nil {
		return FailedHandoff{}, fmt.Errorf("decode legacy NSQ failed handoff: %w", err)
	}
	if encoded.Type != failedEnvelopeType || encoded.Provider != "nsq" || encoded.Topic == "" || encoded.Channel == "" || encoded.UUID == "" || encoded.Attempts < 1 || encoded.Cause == "" {
		return FailedHandoff{}, errors.New("invalid legacy NSQ failed handoff")
	}
	return FailedHandoff{
		Topic: encoded.Topic, Channel: encoded.Channel, UUID: encoded.UUID,
		TransportMessageID: encoded.TransportMessageID, Metadata: copyMetadata(encoded.Metadata),
		Payload: append([]byte(nil), encoded.Payload...), Attempts: encoded.Attempts,
		Timestamp: encoded.Timestamp, Cause: encoded.Cause,
	}, nil
}

// FailedHandoffTopic preserves the historical per-channel NSQ topic name.
func FailedHandoffTopic(topic, channel string) string {
	digest := sha256.Sum256([]byte(topic + "\x00" + channel))
	return "cb.failed." + hex.EncodeToString(digest[:12])
}

// FailedHandoffTopicForGroup preserves the stable shared-handoff naming rule.
func FailedHandoffTopicForGroup(topic, group string) string {
	digest := sha256.Sum256([]byte("group\x00" + topic + "\x00" + group))
	return "cb.failed." + hex.EncodeToString(digest[:12])
}
