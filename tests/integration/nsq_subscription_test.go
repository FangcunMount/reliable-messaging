//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

type subscriptionHandoff struct {
	consumer  *driver.Consumer
	producer  *driver.Producer
	mu        sync.Mutex
	publishes int
}

func (h *subscriptionHandoff) Ready(_ context.Context, address, _ string) error {
	err := h.consumer.ConnectToNSQD(address)
	if errors.Is(err, driver.ErrAlreadyConnected) {
		return nil
	}
	return err
}

func (h *subscriptionHandoff) Publish(_ context.Context, _ string, topic string, body []byte) transport.Result {
	if err := h.producer.Publish(topic, body); err != nil {
		return transport.Result{Outcome: transport.Unknown}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.publishes++
	if h.publishes == 1 {
		// Model a lost confirmation after the real broker accepted the handoff.
		return transport.Result{Outcome: transport.Unknown}
	}
	return transport.Result{Outcome: transport.Confirmed}
}

func TestNSQSubscriptionTerminalHandoffLostConfirmation(t *testing.T) {
	address := os.Getenv("RM_TEST_NSQ_TCP")
	if address == "" {
		t.Fatal("isolated NSQ address required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-subscription-%d", time.Now().UnixNano())
	const channel = "business"
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.MaxInFlight = 1
	handoff := &subscriptionHandoff{}
	var mu sync.Mutex
	businessCalls := 0
	failed := make(chan legacy.FailedHandoff, 4)
	s, err := adapter.NewSubscription(adapter.SubscriptionConfig{
		Topic: topic, Channel: channel, MaxAttempts: 2,
		Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
		Handler: func(_ context.Context, delivery transport.Delivery) error {
			mu.Lock()
			businessCalls++
			mu.Unlock()
			if delivery.Message().ID != "application-uuid" {
				return errors.New("application identity changed")
			}
			return errors.New("business failure")
		},
		FailedHandler: func(_ context.Context, record legacy.FailedHandoff) error {
			failed <- record
			return nil
		},
		Handoff: handoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	handoff.producer = producer
	failureConsumer, err := driver.NewConsumer(s.FailureTopic(), s.FailureChannel(), s.ConsumerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	failureConsumer.SetLogger(nil, driver.LogLevelError)
	failureConsumer.AddHandler(s.FailureHandler(ctx))
	handoff.consumer = failureConsumer
	if err := failureConsumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer stopNSQConsumer(t, failureConsumer)
	businessConsumer, err := driver.NewConsumer(topic, channel, s.ConsumerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	businessConsumer.SetLogger(nil, driver.LogLevelError)
	businessConsumer.AddHandler(s.BusinessHandler(ctx))
	if err := businessConsumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer stopNSQConsumer(t, businessConsumer)
	body, err := legacy.Encode(legacy.Envelope{UUID: "application-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case record := <-failed:
			if record.UUID != "application-uuid" || record.TransportMessageID == "" || record.Topic != topic || record.Channel != channel || record.Cause != "business failure" {
				t.Fatalf("handoff identity or cause changed: %+v", record)
			}
		case <-ctx.Done():
			t.Fatal("missing failure handoff after lost confirmation: ", ctx.Err())
		}
	}
	mu.Lock()
	calls := businessCalls
	mu.Unlock()
	handoff.mu.Lock()
	publishes := handoff.publishes
	handoff.mu.Unlock()
	if calls != 2 || publishes != 2 {
		t.Fatalf("business re-executed after exhaustion or handoff lost: business=%d handoffs=%d", calls, publishes)
	}
}

func stopNSQConsumer(t *testing.T, consumer *driver.Consumer) {
	t.Helper()
	consumer.Stop()
	select {
	case <-consumer.StopChan:
	case <-time.After(5 * time.Second):
		t.Error("NSQ consumer did not stop")
	}
}
