package rocketmq

import (
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
)

func TestTypedRouteSelectionAndOneLogicalSubscriber(t *testing.T) {
	group, err := transport.ConsumerGroupV2("logical", "worker")
	if err != nil {
		t.Fatal(err)
	}
	routes := make([]transport.RouteBinding, 0, 3)
	for _, identity := range [][2]string{{transport.NormalMessageType, "iam.mysql"}, {transport.TransactionMessageType, "iam.mysql"}, {transport.TransactionMessageType, "qs.mongo"}} {
		physical, err := transport.PhysicalTopicV2("logical", identity[0], identity[1], "r1")
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, transport.RouteBinding{Topic: "logical", Subscription: "worker", PhysicalTopic: physical, ConsumerGroup: group, MessageType: identity[0], Owner: identity[1], RouteRevision: "r1"})
	}
	config := Config{Endpoint: "127.0.0.1:1", AccessKey: "fixture", SecretKey: "fixture", RequestTimeout: time.Second, MaxInFlight: 2, Routes: routes}
	publisher, err := NewPublisher(config)
	if err != nil {
		t.Fatal(err)
	}
	if publisher.routes["logical"] != routes[0].PhysicalTopic {
		t.Fatal("normal interface selected transaction topic")
	}
	onlyTx := config
	onlyTx.Routes = routes[1:]
	if _, err = NewPublisher(onlyTx); err == nil {
		t.Fatal("normal publisher accepted only transaction routes")
	}
	subscriber, err := NewSubscriber(SubscriberConfig{Config: config, MaxAttempts: 8, InvisibleDuration: 15 * time.Second, HandlerTimeout: time.Second, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	inputs, logical, err := subscriber.subscriptionRoutes("logical", "worker")
	if err != nil || len(inputs) != 3 || logical.ConsumerGroup != group {
		t.Fatal("logical physical inputs split", err)
	}
	if len(subscriber.running) != 0 || cap(subscriber.slots) != 2 {
		t.Fatal("constructor created consumers or amplified budget")
	}
	other, _ := transport.PhysicalTopicV2("logical", transport.NormalMessageType, "iam.mysql", "r2")
	ambiguous := config
	ambiguous.Routes = append(append([]transport.RouteBinding(nil), routes...), transport.RouteBinding{Topic: "logical", PhysicalTopic: other, MessageType: transport.NormalMessageType, Owner: "iam.mysql", RouteRevision: "r2"})
	if _, err = NewPublisher(ambiguous); err == nil {
		t.Fatal("ambiguous active normal revision accepted")
	}
}
