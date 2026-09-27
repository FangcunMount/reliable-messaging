package nsq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/nsqio/go-nsq"
)

type managedProducerFixture struct {
	publish func(string, []byte) error
	stops   atomic.Int32
}

func (p *managedProducerFixture) Publish(topic string, body []byte) error {
	return p.publish(topic, body)
}
func (p *managedProducerFixture) Ping() error { return nil }
func (p *managedProducerFixture) Stop()       { p.stops.Add(1) }

func TestManagedPublisherRetainsProducerUntilRealSendDrains(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	producer := &managedProducerFixture{publish: func(string, []byte) error {
		close(entered)
		<-release
		return nil
	}}
	managed, err := newManagedPublisher(producer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	published := make(chan transport.Result, 1)
	go func() { published <- managed.PublishRaw(context.Background(), "events", []byte("original")) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	if err := managed.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close before send drained = %v", err)
	}
	cancel()
	if producer.stops.Load() != 0 {
		t.Fatal("producer stopped while send was still in flight")
	}
	if got := managed.PublishRaw(context.Background(), "events", []byte("new")).Outcome; got != transport.Unknown {
		t.Fatalf("publication after close admission = %v", got)
	}
	close(release)
	if got := (<-published).Outcome; got != transport.Confirmed {
		t.Fatalf("admitted publication = %v", got)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := managed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := managed.Close(ctx); err != nil || producer.stops.Load() != 1 {
		t.Fatalf("retryable idempotent close: err=%v stops=%d", err, producer.stops.Load())
	}
}

func TestManagedPublisherRejectsInvalidConfigBeforeConnecting(t *testing.T) {
	if _, err := NewManagedPublisher(ManagedPublisherConfig{Address: "bad", MaxInFlight: 1}); err == nil {
		t.Fatal("invalid NSQD address accepted")
	}
	config := driver.NewConfig()
	config.DialTimeout = 0
	if _, err := NewManagedPublisher(ManagedPublisherConfig{Address: "127.0.0.1:4150", Driver: config, MaxInFlight: 1}); err == nil {
		t.Fatal("unbounded dial timeout accepted")
	}
	producer := &managedProducerFixture{publish: func(string, []byte) error { return nil }}
	if _, err := newManagedPublisher(producer, map[string]string{"route": "invalid topic"}, 1); err == nil {
		t.Fatal("invalid route accepted")
	}
	if producer.stops.Load() != 0 {
		t.Fatal("failed internal construction took ownership of borrowed test producer")
	}
}
