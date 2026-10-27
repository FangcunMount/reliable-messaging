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
	MessageType   string `json:"message_type,omitempty"`
	Owner         string `json:"owner,omitempty"`
	RouteRevision string `json:"route_revision,omitempty"`
	NamingScheme  string `json:"naming_scheme,omitempty"`
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
	seen := map[[5]string]bool{}
	physical := map[string][4]string{}
	groups := map[string][2]string{}
	for _, r := range routes {
		expected, err := ExpectedPhysicalTopic(r.Topic, r.MessageType, r.Owner, r.RouteRevision, r.NamingScheme)
		if err != nil {
			return err
		}
		group, err := expectedConsumerGroup(r)
		if err != nil {
			return err
		}
		if r.Topic == "" || strings.ContainsRune(r.Topic, 0) || strings.ContainsRune(r.Subscription, 0) || r.PhysicalTopic != expected || r.ConsumerGroup != group {
			return errors.New("invalid canonical route binding")
		}
		key := [5]string{r.Topic, r.Subscription, r.MessageType, r.Owner, r.RouteRevision}
		if seen[key] {
			return errors.New("duplicate route binding")
		}
		seen[key] = true
		identity := [4]string{r.Topic, r.MessageType, r.Owner, r.RouteRevision}
		if old, ok := physical[r.PhysicalTopic]; ok && old != identity {
			return errors.New("physical topic collision")
		}
		physical[r.PhysicalTopic] = identity
		if r.Subscription != "" {
			logical := [2]string{r.Topic, r.Subscription}
			if old, ok := groups[r.ConsumerGroup]; ok && old != logical {
				return errors.New("consumer group collision")
			}
			groups[r.ConsumerGroup] = logical
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
