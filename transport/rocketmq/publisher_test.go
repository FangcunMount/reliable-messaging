package rocketmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/apache/rocketmq-clients/golang/v5"
)

type fakeProducer struct {
	send   func(context.Context, *driver.Message) ([]*driver.SendReceipt, error)
	closed atomic.Int32
}

func (f *fakeProducer) Send(ctx context.Context, m *driver.Message) ([]*driver.SendReceipt, error) {
	return f.send(ctx, m)
}
func (f *fakeProducer) GracefulStop() error { f.closed.Add(1); return nil }
func testConfig() Config {
	return Config{Endpoint: "localhost:8081", AccessKey: "test", SecretKey: "test", RequestTimeout: time.Second, MaxInFlight: 1, Routes: []transport.RouteBinding{{Topic: "qs.ai.events.v1", PhysicalTopic: transport.PhysicalTopic("qs.ai.events.v1")}}}
}
func readyPublisher(t *testing.T, fake producer) *Publisher {
	t.Helper()
	p, err := NewPublisher(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.inner = fake
	p.started = true
	return p
}

func TestPublishPreservesBytesAndTrustedRoute(t *testing.T) {
	body := []byte(`{"ciphertext":"immutable"}`)
	p := readyPublisher(t, &fakeProducer{send: func(_ context.Context, m *driver.Message) ([]*driver.SendReceipt, error) {
		if m.Topic != transport.PhysicalTopic("qs.ai.events.v1") || string(m.Body) != string(body) {
			t.Error("rewritten message or untrusted route")
		}
		m.Body[0] = 'x'
		return []*driver.SendReceipt{{MessageID: "physical-id"}}, nil
	}})
	if p.PublishRaw(context.Background(), "qs.ai.events.v1", body).Outcome != transport.Confirmed {
		t.Fatal("expected broker confirmation")
	}
	if body[0] != '{' {
		t.Fatal("caller bytes mutated")
	}
	if p.PublishRaw(context.Background(), "rmq_t_untrusted", body).Outcome != transport.Rejected {
		t.Fatal("unbound route accepted")
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLostConfirmationRetainsCapacityAndDrain(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	fake := &fakeProducer{send: func(context.Context, *driver.Message) ([]*driver.SendReceipt, error) {
		close(entered)
		<-finish
		return []*driver.SendReceipt{{MessageID: "accepted-after-timeout"}}, nil
	}}
	p := readyPublisher(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan transport.Result, 1)
	go func() { done <- p.PublishRaw(ctx, "qs.ai.events.v1", []byte("body")) }()
	<-entered
	cancel()
	if (<-done).Outcome != transport.Unknown {
		t.Fatal("lost confirmation must remain Unknown")
	}
	if len(p.slots) != 1 {
		t.Fatal("capacity released while driver running")
	}
	drainCtx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if !errors.Is(p.Close(drainCtx), context.DeadlineExceeded) || fake.closed.Load() != 0 {
		t.Fatal("closed active producer or falsely drained")
	}
	close(finish)
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(context.Background()); err != nil || fake.closed.Load() != 1 {
		t.Fatal("driver close is not once-only")
	}
}

func TestMissingReceiptIsUnknown(t *testing.T) {
	p := readyPublisher(t, &fakeProducer{send: func(context.Context, *driver.Message) ([]*driver.SendReceipt, error) { return nil, nil }})
	if p.PublishRaw(context.Background(), "qs.ai.events.v1", []byte("body")).Outcome != transport.Unknown {
		t.Fatal("nil driver error alone is not confirmation")
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type fakeConsumer struct {
	acknowledgements int
	fail             bool
}

func (f *fakeConsumer) Receive(context.Context, int32, time.Duration) ([]*driver.MessageView, error) {
	return nil, nil
}
func (f *fakeConsumer) Ack(context.Context, *driver.MessageView) error {
	f.acknowledgements++
	if f.fail {
		return errors.New("ack reply lost")
	}
	return nil
}
func (f *fakeConsumer) ChangeInvisibleDuration(*driver.MessageView, time.Duration) error { return nil }
func (f *fakeConsumer) GracefulStop() error                                              { return nil }

func TestAckUncertaintyAndSingleSettlement(t *testing.T) {
	f := &fakeConsumer{fail: true}
	d := &delivery{inner: f, ctx: context.Background(), timeout: time.Second, received: transport.Received{Payload: []byte("body"), Wire: []byte("wire"), Metadata: map[string]string{"id": "original"}}}
	if d.Ack() == nil || d.Settled() {
		t.Fatal("lost Ack incorrectly settled")
	}
	f.fail = false
	if d.Ack() != nil || !d.Settled() {
		t.Fatal("successful Ack not settled")
	}
	if d.Ack() != nil || d.Nack(errors.New("late")) != nil || f.acknowledgements != 2 {
		t.Fatal("duplicate settlement sent")
	}
	r := d.Message()
	r.Payload[0] = 'x'
	r.Wire[0] = 'x'
	r.Metadata["id"] = "changed"
	r = d.Message()
	if string(r.Payload) != "body" || string(r.Wire) != "wire" || r.Metadata["id"] != "original" {
		t.Fatal("received data not owned")
	}
}

func TestExhaustedFailureAuditRunsOncePerPhysicalDelivery(t *testing.T) {
	s := &Subscriber{config: SubscriberConfig{MaxAttempts: 8}}
	d := &delivery{received: transport.Received{ID: "original", Payload: []byte("immutable"), Attempts: 9}}
	called := 0
	err := s.dispatch(context.Background(), d, nil,
		func(context.Context, transport.Delivery) error {
			t.Fatal("business handler ran beyond budget")
			return nil
		},
		func(_ context.Context, f transport.Failure) error {
			called++
			f.Message.Payload[0] = 'x'
			return errors.New("durable audit unavailable")
		})
	if err == nil || called != 1 || d.Settled() || string(d.Message().Payload) != "immutable" {
		t.Fatal("failed audit retried inline, settled, or mutated original delivery")
	}
}
