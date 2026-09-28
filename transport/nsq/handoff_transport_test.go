package nsq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
)

type fakeHandoffConsumer struct {
	mu      sync.Mutex
	address string
	err     error
}

func (c *fakeHandoffConsumer) ConnectToNSQD(address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.address = address
	return c.err
}

type fakeHandoffProducer struct {
	mu      sync.Mutex
	topic   string
	body    []byte
	stopped bool
	started chan struct{}
	release chan struct{}
	err     error
}

func (p *fakeHandoffProducer) Publish(topic string, body []byte) error {
	if p.started != nil {
		close(p.started)
	}
	if p.release != nil {
		<-p.release
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topic, p.body = topic, append([]byte(nil), body...)
	return p.err
}
func (p *fakeHandoffProducer) Stop() {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
}

func TestDirectHandoffRequiresReadyAndPreservesOwnedBytes(t *testing.T) {
	consumer, producer := &fakeHandoffConsumer{}, &fakeHandoffProducer{}
	created := 0
	h, err := newDirectHandoff(consumer, func(address string) (handoffProducer, error) {
		created++
		if address != "nsqd:4150" {
			t.Fatalf("wrong source address %q", address)
		}
		return producer, nil
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if result := h.Publish(ctx, "nsqd:4150", "cb.failed.test", []byte("wire")); result.Outcome != transport.Unknown {
		t.Fatalf("unprepared publish = %v", result.Outcome)
	}
	if created != 0 {
		t.Fatal("constructor or unprepared publish opened producer")
	}
	if err := h.Ready(ctx, "nsqd:4150", "cb.failed.test"); err != nil {
		t.Fatal(err)
	}
	if err := h.Ready(ctx, "nsqd:4150", "cb.failed.test"); err != nil {
		t.Fatal(err)
	}
	if created != 1 || consumer.address != "nsqd:4150" {
		t.Fatalf("ready count=%d address=%s", created, consumer.address)
	}
	if result := h.Publish(ctx, "nsqd:4150", "cb.failed.other", []byte("wire")); result.Outcome != transport.Unknown {
		t.Fatalf("wrong failure topic = %v", result.Outcome)
	}
	body := []byte("wire")
	if result := h.Publish(ctx, "nsqd:4150", "cb.failed.test", body); result.Outcome != transport.Confirmed {
		t.Fatalf("confirmed publish = %v", result.Outcome)
	}
	body[0] = 'x'
	if string(producer.body) != "wire" || producer.topic != "cb.failed.test" {
		t.Fatalf("published bytes changed: %q %q", producer.topic, producer.body)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !producer.stopped {
		t.Fatal("owned producer not stopped")
	}
	if result := h.Publish(ctx, "nsqd:4150", "cb.failed.test", []byte("again")); result.Outcome != transport.Unknown {
		t.Fatalf("publish after close = %v", result.Outcome)
	}
}

func TestDirectHandoffTimeoutKeepsSendOwnedUntilDrain(t *testing.T) {
	producer := &fakeHandoffProducer{started: make(chan struct{}), release: make(chan struct{})}
	h, err := newDirectHandoff(&fakeHandoffConsumer{}, func(string) (handoffProducer, error) { return producer, nil }, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Ready(context.Background(), "nsqd:4150", "cb.failed.test"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan transport.Result, 1)
	go func() { result <- h.Publish(ctx, "nsqd:4150", "cb.failed.test", []byte("wire")) }()
	select {
	case <-producer.started:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	cancel()
	if outcome := <-result; outcome.Outcome != transport.Unknown {
		t.Fatalf("timed-out send = %v", outcome.Outcome)
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	if err := h.Close(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close while send active = %v", err)
	}
	producer.mu.Lock()
	stopped := producer.stopped
	producer.mu.Unlock()
	if stopped {
		t.Fatal("producer stopped with an in-flight publish")
	}
	close(producer.release)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := h.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	producer.mu.Lock()
	stopped = producer.stopped
	producer.mu.Unlock()
	if !stopped {
		t.Fatal("producer not stopped after actual send completed")
	}
}
