package nsq

import (
	"context"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

func TestNewSubscriberCopiesConfigurationAndDisablesDriverCutoff(t *testing.T) {
	config := driver.NewConfig()
	config.MaxAttempts = 5
	addresses := []string{"127.0.0.1:4150"}
	s, err := NewSubscriber(SubscriberConfig{NSQDAddresses: addresses, Driver: config, MaxInFlight: 2, MaxAttempts: 8})
	if err != nil {
		t.Fatal(err)
	}
	addresses[0] = "127.0.0.1:9999"
	config.MaxAttempts = 12
	if s.config.NSQDAddresses[0] != "127.0.0.1:4150" || s.config.Driver.MaxAttempts != 0 || s.config.Driver.MaxInFlight != 2 {
		t.Fatalf("subscriber inherited mutable config or driver cutoff: %+v", s.config)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Subscribe(context.Background(), "topic", "channel", func(context.Context, transport.Delivery) error { return nil }, func(context.Context, legacy.FailedHandoff) error { return nil }); err == nil {
		t.Fatal("closed subscriber accepted registration")
	}
}

func TestSubscriptionWaitIdleTracksActiveHandler(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s := testSubscription(t, func(context.Context, transport.Delivery) error {
		close(started)
		<-release
		return nil
	}, func(context.Context, legacy.FailedHandoff) error { return nil }, &handoffProbe{result: transport.Result{Outcome: transport.Confirmed}})
	raw, _ := rawMessage([]byte("raw"), 1)
	done := make(chan error, 1)
	go func() { done <- s.HandleBusiness(context.Background(), raw) }()
	<-started
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	if err := s.WaitIdle(shortCtx); err != context.DeadlineExceeded {
		t.Fatalf("active handler was reported idle: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
}
