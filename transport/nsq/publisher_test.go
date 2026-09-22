package nsq

import (
	"context"
	"errors"
	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	"sync/atomic"
	"testing"
	"time"
)

type sendFunc func(string, []byte) error

func (f sendFunc) Publish(topic string, body []byte) error { return f(topic, body) }
func fixture(t *testing.T) message.Message {
	t.Helper()
	m, err := message.New(message.Input{Producer: "test", ID: "original", Destination: "route", EventType: "event", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{ "version":1, "unknown":true }`)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func TestWireAndClassification(t *testing.T) {
	m := fixture(t)
	routes := map[string]string{"route": "original-topic"}
	p, err := newPublisher(sendFunc(func(topic string, b []byte) error {
		if topic != "original-topic" || string(b) != string(m.Input().Payload) {
			t.Error("route/wire changed")
		}
		b[0] = '!'
		return nil
	}), routes, 1)
	if err != nil {
		t.Fatal(err)
	}
	routes["route"] = "changed"
	if p.Publish(context.Background(), m).Outcome != transport.Confirmed {
		t.Fatal("not confirmed")
	}
	if m.Input().Payload[0] != '{' {
		t.Fatal("driver mutated message")
	}
	if p.Publish(context.Background(), message.Message{}).Outcome != transport.Rejected {
		t.Fatal("invalid intent accepted")
	}
	if err = p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.Publish(context.Background(), m).Outcome != transport.Unknown {
		t.Fatal("publish after drain")
	}
	p, err = newPublisher(sendFunc(func(string, []byte) error { return errors.New("lost ACK") }), map[string]string{"route": "topic"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Publish(context.Background(), m).Outcome != transport.Unknown {
		t.Fatal("driver error certainty overstated")
	}
	if err = p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestTimeoutRetainsBoundedSlotAndDrain(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	p, err := newPublisher(sendFunc(func(string, []byte) error { calls.Add(1); entered <- struct{}{}; <-release; return nil }), map[string]string{"route": "topic"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	m := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan transport.Result, 1)
	go func() { done <- p.Publish(ctx, m) }()
	<-entered
	cancel()
	if (<-done).Outcome != transport.Unknown {
		t.Fatal("timeout not unknown")
	}
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		result := p.Publish(ctx, m)
		cancel()
		if result.Outcome != transport.Unknown {
			t.Fatal("queued timeout not unknown")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("timed-out sends exceeded in-flight bound")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	if err = p.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain did not wait: %v", err)
	}
	cancel()
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}
