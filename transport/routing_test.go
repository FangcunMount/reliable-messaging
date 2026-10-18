package transport

import "testing"

func TestExplicitCanonicalRoutes(t *testing.T) {
	r := RouteBinding{Topic: "qs.ai.events.v1", Subscription: "qs-server.ai-events.v1", PhysicalTopic: PhysicalTopic("qs.ai.events.v1"), ConsumerGroup: ConsumerGroup("qs.ai.events.v1", "qs-server.ai-events.v1")}
	if err := ValidateRoutes([]RouteBinding{r}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRoutes([]RouteBinding{r, r}); err == nil {
		t.Fatal("duplicate admitted")
	}
	r.PhysicalTopic = "qs.ai.events.v1"
	if err := ValidateRoutes([]RouteBinding{r}); err == nil {
		t.Fatal("noncanonical topic admitted")
	}
	r.PhysicalTopic = PhysicalTopic(r.Topic)
	r.ConsumerGroup = ConsumerGroup(r.Topic, "other")
	if err := ValidateRoutes([]RouteBinding{r}); err == nil {
		t.Fatal("cross-subscription group admitted")
	}
}
