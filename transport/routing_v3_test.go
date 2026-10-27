package transport

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func readableVectors(t *testing.T) []RouteBinding {
	t.Helper()
	raw, err := os.ReadFile("../contracts/rocketmq-routing-v3.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version int
		Vectors []RouteBinding
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 3 || len(fixture.Vectors) != 12 {
		t.Fatal("nonempty shared v3 vectors required")
	}
	return fixture.Vectors
}

func TestReadableNamesAndHistoricalResolver(t *testing.T) {
	routes := readableVectors(t)
	if err := ValidateRoutes(routes); err != nil {
		t.Fatal(err)
	}
	old := routes[0]
	old.NamingScheme = ""
	old.PhysicalTopic, _ = PhysicalTopicV2(old.Topic, old.MessageType, old.Owner, old.RouteRevision)
	old.ConsumerGroup, _ = ConsumerGroupV2(old.Topic, old.Subscription)
	resolver, err := NewRouteResolver(routes, []RouteBinding{old})
	if err != nil {
		t.Fatal(err)
	}
	key := RouteKey{old.Topic, old.MessageType, old.Owner, old.RouteRevision}
	current, err := resolver.PublishRoute(key)
	if err != nil || current.PhysicalTopic == old.PhysicalTopic {
		t.Fatal("publisher selected history", err)
	}
	if _, err = resolver.RecordedRoute(key, old.PhysicalTopic); err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.RecordedRoute(key, "unreviewed"); err == nil {
		t.Fatal("unreviewed history admitted")
	}
	inputs, err := resolver.SubscriptionRoutes(routes[5].Topic, "qs-worker")
	if err != nil || len(inputs) != 2 {
		t.Fatal("logical subscription lost shared physical inputs", err)
	}
	if _, err = NewRouteResolver(routes, []RouteBinding{routes[0]}); err == nil {
		t.Fatal("active/history overlap admitted")
	}
	changed := append([]RouteBinding(nil), routes...)
	changed[1].Owner = "qs.mysql"
	if err = ValidateRoutes(changed); err == nil {
		t.Fatal("wrong owner accepted")
	}
}

func TestReadableInvalidNamesAndNormalizationCollisions(t *testing.T) {
	for _, v := range []string{"", "Upper", "a--b", "a..b", "a.-b", "a_b", "a b", "中文", "a\x00b"} {
		if _, err := PhysicalTopicV3(v, NormalMessageType, "qs.mysql", "m3-1"); err == nil {
			t.Fatalf("invalid field %q admitted", v)
		}
	}
	if _, err := ConsumerGroupV3(strings.Repeat("a", 40), strings.Repeat("b", 24)); err == nil {
		t.Fatal("overlength resource admitted")
	}
	if _, err := PhysicalTopicV3("topic", "FIFO", "qs.mysql", "m3-1"); err == nil {
		t.Fatal("unsupported type admitted")
	}
	routes := readableVectors(t)
	alias := routes[0]
	alias.Topic = strings.ReplaceAll(alias.Topic, ".", "-")
	if err := ValidateRoutes(append(routes, alias)); err == nil {
		t.Fatal("normalized topic collision admitted")
	}
	routes = readableVectors(t)
	alias = routes[0]
	alias.Subscription = strings.ReplaceAll(alias.Subscription, ".", "-")
	if err := ValidateRoutes(append(routes, alias)); err == nil {
		t.Fatal("normalized group collision admitted")
	}
}
