//go:build redis_integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Real Redis proves wire interoperability with the frozen v0.7.0 primitive,
// error recovery and borrowed client ownership; this is not durability proof.
func TestRealRedisFrozenWireAndCancellation(t *testing.T) {
	addr := os.Getenv("RM_SIGNAL_REDIS_ADDR")
	if addr == "" {
		t.Fatal("RM_SIGNAL_REDIS_ADDR required; real integration must not skip")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	channel := fmt.Sprintf("rm-m604-signal-%d", time.Now().UnixNano())
	decoded := make(chan testSignal, 1)
	invalid := make(chan error, 1)
	s := NewSignaler[testSignal](client, Options{Channel: channel, ErrorHandler: func(err error) { invalid <- err }})
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- s.Watch(watchCtx, func(_ context.Context, v testSignal) { decoded <- v }) }()
	waitForSubscriber(t, client, channel)
	if err := client.Publish(ctx, channel, "invalid-json").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-invalid:
	case <-ctx.Done():
		t.Fatal("decode failure not surfaced")
	}
	const frozen = `{"assessment_id":"assess-1","status":"completed"}`
	if err := client.Publish(ctx, channel, frozen).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-decoded:
		if got.AssessmentID != "assess-1" || got.Status != "completed" {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal("frozen old wire was not received")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("watch did not stop")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal("Watch closed host client:", err)
	}
	// The old JSON subscriber primitive must also accept the SDK's exact bytes.
	sub := client.Subscribe(ctx, channel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Notify(ctx, testSignal{AssessmentID: "assess-1", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Payload != frozen {
		t.Fatalf("published bytes changed: %s", msg.Payload)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	// TCP close is asynchronous at the server; wait for its bounded observable cleanup.
	for {
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		if err != nil {
			t.Fatal(err)
		}
		if counts[channel] == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("owned subscription leaked", counts)
		}
		time.Sleep(time.Millisecond)
	}
	if err := s.Notify(ctx, testSignal{AssessmentID: "no-subscriber"}); err != nil {
		t.Fatal(err)
	}
}
