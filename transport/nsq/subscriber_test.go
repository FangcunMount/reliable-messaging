package nsq

import (
	"context"
	"errors"
	"net"
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

func TestSubscriberCloseDeadlineDuringStalledRegistration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release
		conn.Close()
	}()
	config := driver.NewConfig()
	config.ReadTimeout = 2 * time.Second
	config.WriteTimeout = 2 * time.Second
	config.DialTimeout = time.Second
	config.HeartbeatInterval = time.Second
	s, err := NewSubscriber(SubscriberConfig{
		NSQDAddresses: []string{listener.Addr().String()}, Driver: config, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscribed := make(chan error, 1)
	go func() {
		subscribed <- s.Subscribe(context.Background(), "slow-topic", "slow-channel",
			func(context.Context, transport.Delivery) error { return nil },
			func(context.Context, legacy.FailedHandoff) error { return nil })
	}()
	select {
	case <-accepted:
	case err := <-subscribed:
		close(release)
		t.Fatalf("registration failed before connecting: %v", err)
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("subscriber did not start the blocked NSQ connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close during stalled registration = %v, want deadline", err)
		}
	case <-time.After(300 * time.Millisecond):
		close(release)
		<-subscribed
		<-closed
		t.Fatal("Close ignored its deadline while Subscribe held the lock")
	}
	close(release)
	if err := <-subscribed; err == nil {
		t.Fatal("registration completed after Close stopped admission")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
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
