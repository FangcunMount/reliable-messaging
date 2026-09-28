package catalog

import (
	"slices"
	"testing"
)

const sampleCatalogYAML = `
version: "1"
topics:
  assessment:
    name: qs.assessment
events:
  assessment.submitted:
    topic: assessment
    delivery: durable_outbox
    handler: assessment_handler
  assessment.changed:
    topic: assessment
    delivery: best_effort
    handler: assessment_handler
`

func TestCatalogQueriesTopicHandlerSubscriptionAndDelivery(t *testing.T) {
	cfg, err := Parse([]byte(sampleCatalogYAML))
	if err != nil {
		t.Fatalf("Parse sample catalog: %v", err)
	}

	catalog := NewCatalog(cfg)
	topic, ok := catalog.GetTopicForEvent("assessment.submitted")
	if !ok {
		t.Fatalf("GetTopicForEvent not found")
	}
	if topic != "qs.assessment" {
		t.Fatalf("topic = %q, want qs.assessment", topic)
	}

	eventCfg, ok := catalog.GetEventConfig("assessment.submitted")
	if !ok {
		t.Fatalf("GetEventConfig not found")
	}
	if eventCfg.Handler != "assessment_handler" {
		t.Fatalf("handler = %q, want assessment_handler", eventCfg.Handler)
	}
	if delivery, ok := catalog.GetDeliveryClass("assessment.submitted"); !ok || delivery != DeliveryClassDurableOutbox {
		t.Fatalf("delivery = %q ok=%v, want %q", delivery, ok, DeliveryClassDurableOutbox)
	}
	if !catalog.IsDurableOutbox("assessment.submitted") {
		t.Fatalf("assessment.submitted should be durable outbox")
	}
	if catalog.IsDurableOutbox("assessment.changed") {
		t.Fatalf("assessment.changed should be best effort")
	}

	subscriptions := catalog.TopicSubscriptions()
	if len(subscriptions) != 1 {
		t.Fatalf("TopicSubscriptions() len = %d, want 1", len(subscriptions))
	}
	if !slices.Contains(subscriptions[0].EventTypes, "assessment.submitted") {
		t.Fatalf("subscription missing assessment.submitted: %+v", subscriptions[0])
	}
}

func TestParseRejectsDanglingTopicEmptyHandlerAndInvalidDelivery(t *testing.T) {
	t.Run("dangling topic", func(t *testing.T) {
		_, err := Parse([]byte(`
version: "1"
topics:
  known:
    name: known.topic
events:
  sample.created:
    topic: missing
    delivery: best_effort
    handler: sample_handler
`))
		if err == nil {
			t.Fatalf("Parse should reject event referencing unknown topic")
		}
	})

	t.Run("empty handler", func(t *testing.T) {
		_, err := Parse([]byte(`
version: "1"
topics:
  known:
    name: known.topic
events:
  sample.created:
    topic: known
    delivery: best_effort
`))
		if err == nil {
			t.Fatalf("Parse should reject empty handler")
		}
	})

	t.Run("empty delivery", func(t *testing.T) {
		_, err := Parse([]byte(`
version: "1"
topics:
  known:
    name: known.topic
events:
  sample.created:
    topic: known
    handler: sample_handler
`))
		if err == nil {
			t.Fatalf("Parse should reject empty delivery")
		}
	})

	t.Run("invalid delivery", func(t *testing.T) {
		_, err := Parse([]byte(`
version: "1"
topics:
  known:
    name: known.topic
events:
  sample.created:
    topic: known
    delivery: exactly_once
    handler: sample_handler
`))
		if err == nil {
			t.Fatalf("Parse should reject invalid delivery")
		}
	})
}
