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
	p, err := adapter.New(ch, map[string]adapter.Route{
		"assessment": {Exchange: exchange, RoutingKey: "worker"},
		"missing":    {Exchange: exchange, RoutingKey: "unbound"},
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
	unknownPublisher, err := adapter.New(noConfirm, map[string]adapter.Route{
		"assessment": {Exchange: exchange, RoutingKey: "worker"},
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
	retryPublisher, err := adapter.New(reconnected, map[string]adapter.Route{
		"assessment": {Exchange: exchange, RoutingKey: "worker"},
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
