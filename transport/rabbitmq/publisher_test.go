package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	amqp "github.com/rabbitmq/amqp091-go"
)

type sendFunc func(context.Context, Route, message.Input) transport.Outcome

func (f sendFunc) Send(ctx context.Context, route Route, in message.Input) transport.Outcome {
	return f(ctx, route, in)
}

func fixture(t *testing.T) message.Message {
	t.Helper()
	m, err := message.New(message.Input{
		Producer: "fixture", ID: "original-1", Destination: "assessment", EventType: "answersheet.submitted",
		SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-27T00:00:00+08:00",
		Payload: []byte(`{ "id": "original-1", "unknown": true }`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRouteCopyIdentityAndClassification(t *testing.T) {
	m := fixture(t)
	routes := map[string]Route{"assessment": {Exchange: "rm.assessment", RoutingKey: "submitted"}}
	p, err := newPublisher(sendFunc(func(_ context.Context, route Route, in message.Input) transport.Outcome {
		if route.Exchange != "rm.assessment" || route.RoutingKey != "submitted" || in.ID != m.Input().ID ||
			string(in.Payload) != string(m.Input().Payload) {
			t.Error("route or original identity changed")
		}
		in.Payload[0] = '!'
		return transport.Confirmed
	}), routes)
	if err != nil {
		t.Fatal(err)
	}
	routes["assessment"] = Route{Exchange: "changed"}
	if p.Publish(context.Background(), m).Outcome != transport.Confirmed {
		t.Fatal("confirmed send was not confirmed")
	}
	if m.Input().Payload[0] != '{' {
		t.Fatal("driver mutated durable bytes")
	}
	if p.Publish(context.Background(), message.Message{}).Outcome != transport.Rejected {
		t.Fatal("invalid message accepted")
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.Publish(context.Background(), m).Outcome != transport.Unknown {
		t.Fatal("publish after drain must be unknown")
	}
}

func TestTimeoutRetainsChannelSlotUntilDriverFinishes(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	p, err := newPublisher(sendFunc(func(context.Context, Route, message.Input) transport.Outcome {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return transport.Confirmed
	}), map[string]Route{"assessment": {Exchange: "rm.assessment"}})
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
		t.Fatal("timeout did not become unknown")
	}
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		if p.Publish(ctx, m).Outcome != transport.Unknown {
			t.Fatal("queued timeout not unknown")
		}
		cancel()
	}
	if calls.Load() != 1 {
		t.Fatal("driver calls exceeded one borrowed channel slot")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	if err := p.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain ignored real in-flight work: %v", err)
	}
	cancel()
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

type failingChannel struct{ calls atomic.Int32 }

func (c *failingChannel) PublishWithDeferredConfirmWithContext(context.Context, string, string, bool, bool, amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	c.calls.Add(1)
	return nil, errors.New("connection interrupted after write")
}

func TestUncertainDriverErrorPoisonsSession(t *testing.T) {
	ch := &failingChannel{}
	s := &amqpSender{channel: ch, returns: make(chan amqp.Return, 1), closed: make(chan *amqp.Error, 1)}
	in := fixture(t).Input()
	for range 2 {
		if got := s.Send(context.Background(), Route{Exchange: "rm.assessment"}, in); got != transport.Unknown {
			t.Fatalf("driver error = %v, want Unknown", got)
		}
	}
	if ch.calls.Load() != 1 {
		t.Fatal("poisoned channel was reused after uncertain publish")
	}
}
