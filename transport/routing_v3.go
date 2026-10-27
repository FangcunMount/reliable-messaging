package transport

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const ReadableNamingV1 = "readable-v1"

var readableField = regexp.MustCompile(`^[a-z0-9]+([.-][a-z0-9]+)*$`)

func readableName(fields ...string) (string, error) {
	parts := make([]string, len(fields))
	for i, field := range fields {
		if !readableField.MatchString(field) {
			return "", errors.New("readable route requires lowercase single-separated tokens")
		}
		parts[i] = strings.ReplaceAll(field, ".", "-")
	}
	name := strings.Join(parts, "--")
	if len(name) > 64 {
		return "", errors.New("readable resource exceeds 64 bytes")
	}
	return name, nil
}

func PhysicalTopicV3(topic, messageType, owner, revision string) (string, error) {
	var kind string
	switch messageType {
	case NormalMessageType:
		kind = "normal"
	case TransactionMessageType:
		kind = "tx"
	default:
		return "", errors.New("NORMAL or TRANSACTION required")
	}
	return readableName(topic, kind, owner, revision)
}

func ConsumerGroupV3(topic, subscription string) (string, error) {
	return readableName(topic, subscription)
}

// ExpectedPhysicalTopic is a codec selector, never a network/discovery fallback.
func ExpectedPhysicalTopic(topic, messageType, owner, revision, scheme string) (string, error) {
	switch scheme {
	case ReadableNamingV1:
		return PhysicalTopicV3(topic, messageType, owner, revision)
	case "":
		if messageType == "" {
			if owner != "" || revision != "" {
				return "", errors.New("incomplete typed route")
			}
			return PhysicalTopic(topic), nil
		}
		return PhysicalTopicV2(topic, messageType, owner, revision)
	default:
		return "", errors.New("unknown route naming scheme")
	}
}

func expectedConsumerGroup(r RouteBinding) (string, error) {
	if r.Subscription == "" {
		return "", nil
	}
	if r.NamingScheme == ReadableNamingV1 {
		return ConsumerGroupV3(r.Topic, r.Subscription)
	}
	if r.NamingScheme != "" {
		return "", errors.New("unknown route naming scheme")
	}
	if r.MessageType != "" {
		return ConsumerGroupV2(r.Topic, r.Subscription)
	}
	return ConsumerGroup(r.Topic, r.Subscription), nil
}

// RouteKey is a logical publishing identity; no untrusted physical input selects it.
type RouteKey struct{ Topic, MessageType, Owner, RouteRevision string }

func keyFor(r RouteBinding) RouteKey {
	return RouteKey{r.Topic, r.MessageType, r.Owner, r.RouteRevision}
}

// RouteResolver separates current capabilities from exact historical evidence.
// Historical routes never enter publishing or subscription results.
type RouteResolver struct {
	active   []RouteBinding
	recorded map[string]RouteBinding
}

func NewRouteResolver(active, historical []RouteBinding) (*RouteResolver, error) {
	if err := ValidateRoutes(active); err != nil {
		return nil, err
	}
	if len(historical) != 0 {
		if err := ValidateRoutes(historical); err != nil {
			return nil, err
		}
	}
	r := &RouteResolver{active: append([]RouteBinding(nil), active...), recorded: map[string]RouteBinding{}}
	groups := map[string][2]string{}
	logicalGroups := map[[2]string]string{}
	for index, list := range [][]RouteBinding{active, historical} {
		physical := map[string]bool{}
		for _, row := range list {
			if previous, ok := r.recorded[row.PhysicalTopic]; ok && !physical[row.PhysicalTopic] {
				if keyFor(previous) != keyFor(row) || previous.NamingScheme != row.NamingScheme {
					return nil, errors.New("route identity collision")
				}
				return nil, errors.New("active/historical resource overlap")
			}
			if row.ConsumerGroup != "" {
				identity := [2]string{row.Topic, row.Subscription}
				if index == 0 {
					if old, ok := logicalGroups[identity]; ok && old != row.ConsumerGroup {
						return nil, errors.New("logical subscription mixes consumer group schemes")
					}
					logicalGroups[identity] = row.ConsumerGroup
				}
				if old, ok := groups[row.ConsumerGroup]; ok && old != identity {
					return nil, errors.New("consumer group collision")
				}
				groups[row.ConsumerGroup] = identity
			}
			r.recorded[row.PhysicalTopic] = row
			physical[row.PhysicalTopic] = true
		}
	}
	return r, nil
}

func (r *RouteResolver) PublishRoute(key RouteKey) (RouteBinding, error) {
	if r != nil {
		for _, row := range r.active {
			if keyFor(row) == key {
				return row, nil
			}
		}
	}
	return RouteBinding{}, errors.New("publishing route not in active manifest")
}

func (r *RouteResolver) RecordedRoute(key RouteKey, physical string) (RouteBinding, error) {
	if r != nil {
		if row, ok := r.recorded[physical]; ok && keyFor(row) == key {
			return row, nil
		}
	}
	return RouteBinding{}, errors.New("recorded route not in trusted manifest")
}

func (r *RouteResolver) SubscriptionRoutes(topic, subscription string) ([]RouteBinding, error) {
	var out []RouteBinding
	if r != nil {
		for _, row := range r.active {
			if row.Topic == topic && row.Subscription == subscription && row.ConsumerGroup != "" {
				out = append(out, row)
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("subscription not in active manifest")
	}
	return out, nil
}

type RouteManifest struct {
	Version          int            `json:"version"`
	Routes           []RouteBinding `json:"routes"`
	HistoricalRoutes []RouteBinding `json:"historical_routes,omitempty"`
}

func ParseRouteManifest(raw []byte) (RouteManifest, error) {
	var m RouteManifest
	if len(raw) == 0 || len(raw) > 1<<20 || json.Unmarshal(raw, &m) != nil || m.Version < 1 || m.Version > 3 {
		return m, errors.New("unsupported route manifest")
	}
	for _, row := range m.Routes {
		if (m.Version == 1 && (row.MessageType != "" || row.NamingScheme != "")) ||
			(m.Version == 2 && (row.MessageType == "" || row.NamingScheme != "")) ||
			(m.Version == 3 && (row.MessageType == "" || row.NamingScheme != ReadableNamingV1)) {
			return m, errors.New("route codec differs from manifest version")
		}
	}
	if m.Version != 3 && len(m.HistoricalRoutes) != 0 {
		return m, errors.New("historical routes require manifest v3")
	}
	for _, row := range m.HistoricalRoutes {
		if row.MessageType == "" || row.NamingScheme != "" {
			return m, errors.New("historical typed hash route required")
		}
	}
	_, err := NewRouteResolver(m.Routes, m.HistoricalRoutes)
	return m, err
}
