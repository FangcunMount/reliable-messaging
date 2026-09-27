package catalog

import (
	"strings"
	"testing"
)

const catalogWithoutHandler = `
version: "1"
topics:
  sample:
    name: sample.topic
events:
  sample.created:
    topic: sample
    delivery: durable_outbox
`

const catalogWithUnusedTopic = `
version: "1"
topics:
  sample:
    name: sample.topic
  unused:
    name: unused.topic
events:
  sample.created:
    topic: sample
    delivery: durable_outbox
    handler: sample_handler
`

func TestParseUsesStrictValidationByDefault(t *testing.T) {
	t.Parallel()

	_, err := Parse([]byte(catalogWithoutHandler))
	if err == nil || !strings.Contains(err.Error(), "empty handler") {
		t.Fatalf("Parse() error = %v, want empty handler validation", err)
	}
}

func TestParseWithOptionsCanSkipHandlerRequirement(t *testing.T) {
	t.Parallel()

	cfg, err := ParseWithOptions([]byte(catalogWithoutHandler), ValidateOptions{
		RequireTopicReferenced: true,
	})
	if err != nil {
		t.Fatalf("ParseWithOptions() error = %v", err)
	}
	if got, ok := cfg.GetDeliveryClass("sample.created"); !ok || got != DeliveryClassDurableOutbox {
		t.Fatalf("delivery = %q, %v; want durable_outbox,true", got, ok)
	}
}

func TestValidateWithOptionsCanSkipUnusedTopicRequirement(t *testing.T) {
	t.Parallel()

	if _, err := Parse([]byte(catalogWithUnusedTopic)); err == nil || !strings.Contains(err.Error(), "has no events") {
		t.Fatalf("Parse() error = %v, want unused topic validation", err)
	}

	if _, err := ParseWithOptions([]byte(catalogWithUnusedTopic), ValidateOptions{RequireHandler: true}); err != nil {
		t.Fatalf("ParseWithOptions() error = %v", err)
	}
}
