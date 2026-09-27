//go:build integration

package nsq

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

func TestManagedPublisherOwnsRealNSQConnection(t *testing.T) {
	address, endpoint := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || endpoint == "" {
		t.Fatal("isolated NSQ TCP and HTTP addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-managed-publisher-%d", time.Now().UnixNano())
	provisioner, err := NewProvisioner(&http.Client{Timeout: time.Second}, []string{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioner.EnsureChannel(ctx, topic, "business"); err != nil {
		t.Fatal(err)
	}
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	consumer, err := driver.NewConsumer(topic, "business", cfg)
	if err != nil {
		t.Fatal(err)
	}
	consumer.SetLogger(nil, driver.LogLevelError)
	received := make(chan []byte, 1)
	consumer.AddHandler(driver.HandlerFunc(func(raw *driver.Message) error {
		received <- bytes.Clone(raw.Body)
		return nil
	}))
	if err := consumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer func() { consumer.Stop(); <-consumer.StopChan }()
	managed, err := NewManagedPublisher(ManagedPublisherConfig{Address: address, Driver: cfg, MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := managed.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	body, err := legacy.Encode(legacy.Envelope{UUID: "managed-original-id", Payload: []byte("original-payload")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if got := managed.PublishRaw(ctx, topic, body).Outcome; got != transport.Confirmed {
		t.Fatalf("managed NSQ publish = %v", got)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, body) {
			t.Fatal("managed publisher changed original wire bytes")
		}
	case <-ctx.Done():
		t.Fatal("managed publication not delivered: ", ctx.Err())
	}
	if err := managed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := managed.PublishRaw(ctx, topic, body).Outcome; got != transport.Unknown {
		t.Fatalf("publish after managed close = %v", got)
	}
}
