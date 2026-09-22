// Executable SDK lifecycle reference. The store/publisher below are test doubles;
// the separate MySQL integration proves persistence and transaction behavior.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/relay"
	"github.com/FangcunMount/reliable-messaging/transport"
)

type exampleStore struct {
	message            message.Message
	claimed, confirmed bool
}

func (s *exampleStore) ClaimDue(context.Context, int, time.Duration) ([]outbox.Claim, error) {
	if s.claimed {
		return nil, nil
	}
	s.claimed = true
	return []outbox.Claim{{Message: s.message}}, nil
}
func (s *exampleStore) Confirm(context.Context, outbox.Claim) error                  { s.confirmed = true; return nil }
func (s *exampleStore) Retry(context.Context, outbox.Claim, time.Time, string) error { return nil }
func (s *exampleStore) Quarantine(context.Context, outbox.Claim, string) error       { return nil }

type examplePublisher struct{ started, release chan struct{} }

func (p examplePublisher) Publish(ctx context.Context, _ message.Message) transport.Result {
	close(p.started)
	select {
	case <-p.release:
		return transport.Result{Outcome: transport.Confirmed}
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
}
func main() {
	m, err := message.New(message.Input{Producer: "example", ID: "1", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{"version":1}`)})
	if err != nil {
		panic(err)
	}
	store := &exampleStore{message: m}
	publisher := examplePublisher{make(chan struct{}), make(chan struct{})}
	r, err := relay.New(store, publisher, relay.Config{Concurrency: 1, PollInterval: time.Millisecond, Lease: 3 * time.Second, PublishTimeout: time.Second, WriteTimeout: time.Second, Retry: func(outbox.Claim, transport.Outcome) relay.RetryDecision {
		return relay.RetryDecision{Delay: time.Second}
	}, Observe: func(relay.Event) {}})
	if err != nil {
		panic(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-publisher.started
	stop() // Stop admission before closing host resources.
	close(publisher.release)
	if err = <-done; err != nil {
		panic(err)
	} // Drain before closing DB / broker.
	if !store.confirmed {
		panic("admitted delivery was not drained")
	}
	fmt.Println("PASS SDK Relay explicit start, stop admission, drain, then host resource close")
}
