//go:build integration

package integration

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type topologyAlreadyChecked struct{}

func (topologyAlreadyChecked) Verify(context.Context, adapter.Route) transport.Outcome {
	return transport.Confirmed
}

func integrationRoute(exchange, key string) adapter.Route {
	return adapter.Route{Exchange: exchange, ExchangeKind: "direct", RoutingKey: key,
		RequiredQueues: []adapter.RequiredQueue{{Name: "rm.b0.confirm.integration.worker", BindingKey: key, QueueType: "classic"}}}
}

func waitTopology(t *testing.T, ctx context.Context, verifier adapter.TopologyVerifier, route adapter.Route, want transport.Outcome) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		got := verifier.Verify(ctx, route)
		if got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topology outcome = %v, want %v before deadline: %v", got, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestRabbitMQPublisherConfirmReturnAndOriginalWire(t *testing.T) {
	address := os.Getenv("RM_TEST_RABBITMQ_URL")
	if address == "" {
		t.Fatal("isolated RM_TEST_RABBITMQ_URL required")
	}
	conn, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	setup, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	const exchange = "rm.b0.confirm.integration"
	const queue = "rm.b0.confirm.integration.worker"
	if err := setup.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := setup.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueueBind(queue, "worker", exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := setup.QueueDelete(queue, false, false, false); err != nil {
			t.Error(err)
		}
		if err := setup.ExchangeDelete(exchange, false, false); err != nil {
			t.Error(err)
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	p, err := adapter.New(ch, topologyAlreadyChecked{}, map[string]adapter.Route{
		"assessment": integrationRoute(exchange, "worker"),
		"missing":    integrationRoute(exchange, "unbound"),
	})
	if err != nil {
		t.Fatal(err)
	}
	m, err := message.New(message.Input{
		Producer: "integration", ID: "original-b0", Destination: "assessment",
		EventType: "answersheet.submitted", SchemaVersion: "1", Scope: "global",
		ContentType: "application/json", OccurredAt: "2026-09-27T00:00:00+08:00",
		Payload: []byte(`{ "original": true, "unknown": 42 }`),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if got := p.Publish(ctx, m).Outcome; got != transport.Confirmed {
		t.Fatalf("routable persistent publish = %v, want Confirmed", got)
	}
	delivery, ok, err := setup.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("queue delivery: present=%v error=%v", ok, err)
	}
	if delivery.MessageId != "original-b0" || delivery.Type != "answersheet.submitted" ||
		delivery.ContentType != "application/json" || !bytes.Equal(delivery.Body, m.Input().Payload) ||
		delivery.DeliveryMode != amqp.Persistent || delivery.Headers["x-rm-producer"] != "integration" {
		t.Fatalf("AMQP properties or original bytes changed: id=%q type=%q body=%q", delivery.MessageId, delivery.Type, delivery.Body)
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
	missing, err := message.New(message.Input{
		Producer: "integration", ID: "unroutable-b0", Destination: "missing",
		EventType: "answersheet.submitted", SchemaVersion: "1", Scope: "global",
		ContentType: "application/json", OccurredAt: "2026-09-27T00:00:00+08:00", Payload: []byte(`{"id":"unroutable-b0"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Publish(ctx, missing).Outcome; got != transport.Rejected {
		t.Fatalf("mandatory returned publish = %v, want Rejected despite broker confirm", got)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	// A host that forgets confirm mode may still route the bytes. The SDK must
	// not mark that write Confirmed, so a later retry remains possible.
	noConfirm, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	unknownPublisher, err := adapter.New(noConfirm, topologyAlreadyChecked{}, map[string]adapter.Route{
		"assessment": integrationRoute(exchange, "worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := unknownPublisher.Publish(ctx, m).Outcome; got != transport.Unknown {
		t.Fatalf("publish without confirm mode = %v, want Unknown", got)
	}
	if err := unknownPublisher.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noConfirm.Close(); err != nil {
		t.Fatal(err)
	}
	// Reconnecting is host-owned: a fresh channel must re-enter confirm mode
	// and register fresh return/close listeners before publication resumes.
	reconnected, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	if err := reconnected.Confirm(false); err != nil {
		t.Fatal(err)
	}
	retryPublisher, err := adapter.New(reconnected, topologyAlreadyChecked{}, map[string]adapter.Route{
		"assessment": integrationRoute(exchange, "worker"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := retryPublisher.Publish(ctx, m).Outcome; got != transport.Confirmed {
		t.Fatalf("reconnected original-identity publish = %v, want Confirmed", got)
	}
	if err := retryPublisher.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRabbitMQRequiredConsumerBindings(t *testing.T) {
	address, management := os.Getenv("RM_TEST_RABBITMQ_URL"), os.Getenv("RM_TEST_RABBITMQ_HTTP")
	if address == "" || management == "" {
		t.Fatal("isolated RabbitMQ AMQP and management addresses required")
	}
	conn, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	setup, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	const exchange = "rm.b0.topology.integration"
	const workerQueue = "rm.b0.topology.integration.worker"
	const hotRankQueue = "rm.b0.topology.integration.hot-rank"
	if err := setup.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, queue := range []string{workerQueue, hotRankQueue} {
			if _, err := setup.QueueDelete(queue, false, false, false); err != nil {
				t.Error(err)
			}
		}
		if err := setup.ExchangeDelete(exchange, false, false); err != nil {
			t.Error(err)
		}
	}()
	for _, queue := range []string{workerQueue, hotRankQueue} {
		if _, err := setup.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
		if err := setup.QueueBind(queue, "submitted", exchange, false, nil); err != nil {
			t.Fatal(err)
		}
	}
	verifier, err := adapter.NewManagementVerifier(management, "/", "rmtest", "rmtest", nil)
	if err != nil {
		t.Fatal(err)
	}
	route := adapter.Route{Exchange: exchange, ExchangeKind: "direct", RoutingKey: "submitted",
		RequiredQueues: []adapter.RequiredQueue{
			{Name: workerQueue, BindingKey: "submitted", QueueType: "classic"},
			{Name: hotRankQueue, BindingKey: "submitted", QueueType: "classic"},
		}}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	p, err := adapter.New(ch, verifier, map[string]adapter.Route{"assessment": route})
	if err != nil {
		t.Fatal(err)
	}
	m, err := message.New(message.Input{
		Producer: "integration", ID: "two-required-groups", Destination: "assessment",
		EventType: "answersheet.submitted", SchemaVersion: "1", Scope: "global",
		ContentType: "application/json", OccurredAt: "2026-09-27T00:00:00+08:00",
		Payload: []byte(`{"id":"two-required-groups"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	waitTopology(t, ctx, verifier, route, transport.Confirmed)
	if got := p.Publish(ctx, m).Outcome; got != transport.Confirmed {
		t.Fatalf("two-group publish = %v, want Confirmed", got)
	}
	for _, queue := range []string{workerQueue, hotRankQueue} {
		delivery, ok, err := setup.Get(queue, false)
		if err != nil || !ok || delivery.MessageId != m.Input().ID {
			t.Fatalf("required group %s: present=%v id=%q error=%v", queue, ok, delivery.MessageId, err)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatal(err)
		}
	}
	if err := setup.QueueUnbind(hotRankQueue, "submitted", exchange, nil); err != nil {
		t.Fatal(err)
	}
	waitTopology(t, ctx, verifier, route, transport.Rejected)
	// Worker remains routable, so mandatory alone would receive a broker ACK.
	// The read-only topology check must reject the missing second group first.
	if got := p.Publish(ctx, m).Outcome; got != transport.Rejected {
		t.Fatalf("missing hot-rank binding = %v, want Rejected", got)
	}
	if _, ok, err := setup.Get(workerQueue, false); err != nil || ok {
		t.Fatalf("rejected publish reached worker queue: present=%v error=%v", ok, err)
	}
	if err := setup.QueueBind(hotRankQueue, "submitted", exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	waitTopology(t, ctx, verifier, route, transport.Confirmed)
	if got := p.Publish(ctx, m).Outcome; got != transport.Confirmed {
		t.Fatalf("restored two-group publish = %v, want Confirmed", got)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}
