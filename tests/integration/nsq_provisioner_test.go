//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

func TestNSQProvisionedFirstMessageBeforeConsumer(t *testing.T) {
	address, httpAddress := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || httpAddress == "" {
		t.Fatal("isolated NSQ addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-first-%d", time.Now().UnixNano())
	const channel = "business"
	provisioner, err := adapter.NewProvisioner(&http.Client{Timeout: 3 * time.Second}, []string{httpAddress})
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioner.EnsureChannel(ctx, topic, channel); err != nil {
		t.Fatal(err)
	}
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	publisher, err := adapter.New(producer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	body, err := legacy.Encode(legacy.Envelope{UUID: "first-application-uuid", Payload: []byte("original")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if result := publisher.PublishRaw(ctx, topic, body); result.Outcome != transport.Confirmed {
		t.Fatalf("first publish outcome=%d", result.Outcome)
	}
	if err := publisher.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	// No business consumer existed when the first message was published.
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		NSQDAddresses: []string{address}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 2, Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	if err := subscriber.Subscribe(ctx, topic, channel, func(_ context.Context, delivery transport.Delivery) error {
		if delivery.Message().ID != "first-application-uuid" {
			return errors.New("first message lost application identity")
		}
		received <- append([]byte(nil), delivery.Message().Payload...)
		return nil
	}, func(context.Context, legacy.FailedHandoff) error {
		return errors.New("first message unexpectedly failed")
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := subscriber.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case actual := <-received:
		if !bytes.Equal(actual, []byte("original")) {
			t.Fatal("first message payload changed")
		}
	case <-ctx.Done():
		t.Fatal("first message not delivered after consumer startup: ", ctx.Err())
	}
}
