//go:build integration

package nsq

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

type rotatingRealHandoffProducer struct {
	mu      sync.Mutex
	first   *driver.Producer
	next    *driver.Producer
	sends   int
	stopped bool
}

func (p *rotatingRealHandoffProducer) Publish(topic string, body []byte) error {
	p.mu.Lock()
	p.sends++
	producer := p.next
	if p.sends == 1 {
		producer = p.first
	}
	p.mu.Unlock()
	return producer.Publish(topic, body)
}

func (p *rotatingRealHandoffProducer) Stop() {
	p.first.Stop()
	p.next.Stop()
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
}

func TestDirectHandoffRealDisconnectRetainsIdentityAndDrains(t *testing.T) {
	address := os.Getenv("RM_TEST_NSQ_TCP")
	if address == "" {
		t.Fatal("isolated NSQ address required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sourceTopic := fmt.Sprintf("rm-direct-handoff-%d", time.Now().UnixNano())
	failureTopic := legacy.FailedHandoffTopic(sourceTopic, "business")
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.MaxInFlight = 1
	cfg.MaxAttempts = 0
	failureConsumer, err := driver.NewConsumer(failureTopic, legacy.FailedHandoffChannel, cfg)
	if err != nil {
		t.Fatal(err)
	}
	failureConsumer.SetLogger(nil, driver.LogLevelError)
	type physicalFailure struct {
		id   string
		body []byte
	}
	received := make(chan physicalFailure, 2)
	failureConsumer.AddHandler(driver.HandlerFunc(func(raw *driver.Message) error {
		received <- physicalFailure{id: string(raw.ID[:]), body: bytes.Clone(raw.Body)}
		return nil
	}))
	proxyAddress, proxyDone, closeProxy := startRealHandoffAckDropProxy(t, address)
	defer closeProxy()
	first, err := driver.NewProducer(proxyAddress, cfg)
	if err != nil {
		t.Fatal(err)
	}
	first.SetLogger(nil, driver.LogLevelError)
	next, err := driver.NewProducer(address, cfg)
	if err != nil {
		first.Stop()
		t.Fatal(err)
	}
	next.SetLogger(nil, driver.LogLevelError)
	producer := &rotatingRealHandoffProducer{first: first, next: next}
	handoff, err := newDirectHandoff(failureConsumer, func(source string) (handoffProducer, error) {
		if source != address {
			return nil, fmt.Errorf("unexpected source NSQD %q", source)
		}
		return producer, nil
	}, 1)
	if err != nil {
		producer.Stop()
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := handoff.Close(closeCtx); err != nil {
			t.Error(err)
		}
		failureConsumer.Stop()
		select {
		case <-failureConsumer.StopChan:
		case <-time.After(5 * time.Second):
			t.Error("failure consumer did not stop")
		}
	}()
	if err := handoff.Ready(ctx, address, failureTopic); err != nil {
		t.Fatal(err)
	}
	body, err := legacy.EncodeFailedHandoff(legacy.FailedHandoff{
		Topic: sourceTopic, Channel: "business", UUID: "same-application-uuid",
		TransportMessageID: "original-nsq-physical-id", Payload: []byte("original-payload"),
		Attempts: 1, Cause: "business failure",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := handoff.Publish(ctx, address, failureTopic, body).Outcome; got != transport.Unknown {
		t.Fatalf("lost broker confirmation = %v, want Unknown", got)
	}
	select {
	case err := <-proxyDone:
		if err != nil {
			t.Fatalf("broker did not accept first handoff: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first handoff did not reach broker: ", ctx.Err())
	}
	if got := handoff.Publish(ctx, address, failureTopic, body).Outcome; got != transport.Confirmed {
		t.Fatalf("recovered handoff = %v, want Confirmed", got)
	}
	seen := make(map[string]struct{}, 2)
	for i := 0; i < 2; i++ {
		select {
		case item := <-received:
			if !bytes.Equal(item.body, body) {
				t.Fatal("handoff wire changed after unknown confirmation")
			}
			decoded, err := legacy.DecodeFailedHandoff(item.body)
			if err != nil || decoded.UUID != "same-application-uuid" || decoded.TransportMessageID != "original-nsq-physical-id" ||
				decoded.Topic != sourceTopic || decoded.Channel != "business" || decoded.Cause != "business failure" || string(decoded.Payload) != "original-payload" {
				t.Fatalf("failure identity changed: %+v err=%v", decoded, err)
			}
			seen[item.id] = struct{}{}
		case <-ctx.Done():
			t.Fatal("missing physical handoff: ", ctx.Err())
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected two physical handoffs, got %d", len(seen))
	}
	if err := handoff.Close(ctx); err != nil {
		t.Fatal(err)
	}
	producer.mu.Lock()
	sends, stopped := producer.sends, producer.stopped
	producer.mu.Unlock()
	if sends != 2 || !stopped || handoff.Publish(ctx, address, failureTopic, body).Outcome != transport.Unknown {
		t.Fatalf("handoff lifecycle changed: sends=%d stopped=%t", sends, stopped)
	}
}

// The proxy forwards a real PUB but closes before returning its OK frame.
func startRealHandoffAckDropProxy(t *testing.T, address string) (string, <-chan error, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		defer listener.Close()
		downstream, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer downstream.Close()
		_ = downstream.SetDeadline(time.Now().Add(5 * time.Second))
		upstream, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			done <- err
			return
		}
		defer upstream.Close()
		_ = upstream.SetDeadline(time.Now().Add(5 * time.Second))
		copied := make(chan struct{})
		go func() { defer close(copied); _, _ = io.Copy(upstream, downstream) }()
		defer func() { downstream.Close(); upstream.Close(); <-copied }()
		for {
			var header [4]byte
			if _, err = io.ReadFull(upstream, header[:]); err != nil {
				done <- err
				return
			}
			n := binary.BigEndian.Uint32(header[:])
			if n < 4 || n > 1024*1024 {
				done <- fmt.Errorf("unexpected NSQ frame length %d", n)
				return
			}
			frame := make([]byte, n)
			if _, err = io.ReadFull(upstream, frame); err != nil {
				done <- err
				return
			}
			if binary.BigEndian.Uint32(frame[:4]) == 0 && string(frame[4:]) == "OK" {
				done <- nil
				return
			}
			if _, err = downstream.Write(append(header[:], frame...)); err != nil {
				done <- err
				return
			}
		}
	}()
	return listener.Addr().String(), done, func() { _ = listener.Close() }
}

var _ handoffProducer = (*rotatingRealHandoffProducer)(nil)
