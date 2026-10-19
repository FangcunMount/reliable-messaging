package transport

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	NormalMessageType      = "NORMAL"
	TransactionMessageType = "TRANSACTION"
)

func routeField(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func routeDigestV2(domain string, fields ...string) string {
	h := sha256.New()
	h.Write([]byte(domain))
	for _, value := range fields {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(value)))
		h.Write(n[:])
		h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// PhysicalTopicV2 binds the exact logical name, message type, owner and route
// revision. Length-prefixed UTF-8 fields have no delimiter ambiguity. Existing
// V1 resources are never converted or reused for a different message type.
func PhysicalTopicV2(topic, messageType, owner, revision string) (string, error) {
	if !routeField(topic, 255) || !routeField(owner, 128) || !routeField(revision, 128) || (messageType != NormalMessageType && messageType != TransactionMessageType) {
		return "", errors.New("invalid v2 topic identity")
	}
	return "rmq2_t_" + routeDigestV2("rmq-topic-v2\x00", topic, messageType, owner, revision), nil
}

// ConsumerGroupV2 belongs to a LOGICAL subscription. Its physical inputs share
// one consumer and one in-flight budget across types, owners and revisions.
func ConsumerGroupV2(topic, subscription string) (string, error) {
	if !routeField(topic, 255) || !routeField(subscription, 255) {
		return "", errors.New("invalid v2 subscription identity")
	}
	return "rmq2_g_" + routeDigestV2("rmq-group-v2\x00", topic, subscription), nil
}
