package transport

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

// RouteBinding is host-reviewed configuration, never data supplied by a message.
// Version 1 preserves dotted logical names while using legal RocketMQ resources.
type RouteBinding struct {
	Topic         string `json:"topic"`
	Subscription  string `json:"subscription,omitempty"`
	PhysicalTopic string `json:"physical_topic"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
}

func PhysicalTopic(topic string) string { return "rmq_t_" + routeDigest(topic) }
func ConsumerGroup(topic, subscription string) string {
	return "rmq_g_" + routeDigest(topic+"\x00"+subscription)
}
func routeDigest(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:16])
}

// ValidateRoutes fails closed on duplicates, collisions and noncanonical bindings.
func ValidateRoutes(routes []RouteBinding) error {
	if len(routes) == 0 {
		return errors.New("explicit route bindings required")
	}
	seen := map[string]bool{}
	physical := map[string]string{}
	for _, r := range routes {
		if r.Topic == "" || strings.ContainsRune(r.Topic, 0) || strings.ContainsRune(r.Subscription, 0) || r.PhysicalTopic != PhysicalTopic(r.Topic) {
			return errors.New("invalid logical/physical topic binding")
		}
		key := r.Topic + "\x00" + r.Subscription
		if seen[key] {
			return errors.New("duplicate route binding")
		}
		seen[key] = true
		if old, ok := physical[r.PhysicalTopic]; ok && old != r.Topic {
			return errors.New("physical topic collision")
		}
		physical[r.PhysicalTopic] = r.Topic
		if r.Subscription == "" && r.ConsumerGroup != "" || r.Subscription != "" && r.ConsumerGroup != ConsumerGroup(r.Topic, r.Subscription) {
			return errors.New("invalid consumer group binding")
		}
		if r.Subscription != "" {
			if old, ok := physical[r.ConsumerGroup]; ok && old != key {
				return errors.New("consumer group collision")
			}
			physical[r.ConsumerGroup] = key
		}
	}
	return nil
}

// Failure is an original delivery requiring host-owned durable audit before Ack.
// It grants no business replay authorization.
type Failure struct {
	Provider string
	Message  Received
	Cause    string
}
