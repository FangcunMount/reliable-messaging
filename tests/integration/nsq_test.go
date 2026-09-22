//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	driver "github.com/nsqio/go-nsq"
)

func TestNSQLostConfirmationPreservesWire(t *testing.T) {
	address, httpAddress := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || httpAddress == "" {
		t.Fatal("isolated NSQ addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic := "rm-sdk-lost-confirm"
	client := &http.Client{Timeout: 3 * time.Second}
	for _, path := range []string{"/topic/create?topic=" + topic, "/channel/create?topic=" + topic + "&channel=proof"} {
		req, err := http.NewRequestWithContext(ctx, "POST", httpAddress+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("setup status %d for %s", response.StatusCode, path)
		}
	}
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	consumer, err := driver.NewConsumer(topic, "proof", cfg)
	if err != nil {
		t.Fatal(err)
	}
	consumer.SetLogger(nil, driver.LogLevelError)
	received := make(chan []byte, 4)
	consumer.AddHandler(driver.HandlerFunc(func(m *driver.Message) error { received <- append([]byte(nil), m.Body...); return nil }))
	if err = consumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer func() {
		consumer.Stop()
		select {
		case <-consumer.StopChan:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	}()
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxyDone := make(chan error, 1)
	// The pinned driver always negotiates IDENTIFY features (JSON response).
	// Drop the subsequent PUB OK frame after NSQ has accepted the raw bytes.
	go func() {
		downstream, err := proxy.Accept()
		if err != nil {
			proxyDone <- err
			return
		}
		defer downstream.Close()
		_ = downstream.SetDeadline(time.Now().Add(5 * time.Second))
		upstream, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			proxyDone <- err
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
				proxyDone <- err
				return
			}
			n := binary.BigEndian.Uint32(header[:])
			if n < 4 || n > 1024*1024 {
				proxyDone <- fmt.Errorf("unexpected frame %d", n)
				return
			}
			frame := make([]byte, n)
			if _, err = io.ReadFull(upstream, frame); err != nil {
				proxyDone <- err
				return
			}
			if binary.BigEndian.Uint32(frame[:4]) == 0 && string(frame[4:]) == "OK" {
				proxyDone <- nil
				return
			}
			if _, err = downstream.Write(append(header[:], frame...)); err != nil {
				proxyDone <- err
				return
			}
		}
	}()
	producer, err := driver.NewProducer(proxy.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	p, err := adapter.New(producer, map[string]string{"events": topic}, 1)
	if err != nil {
		t.Fatal(err)
	}
	m, err := message.New(message.Input{Producer: "test", ID: "same-business-id", Destination: "events", EventType: "created", SchemaVersion: "1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: []byte(`{ "id":"same-business-id", "extension": {"keep":true} }`)})
	if err != nil {
		t.Fatal(err)
	}
	if p.Publish(ctx, m).Outcome != transport.Unknown {
		t.Fatal("lost confirmation must be unknown")
	}
	select {
	case err = <-proxyDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	direct, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	direct.SetLogger(nil, driver.LogLevelError)
	defer direct.Stop()
	retry, err := adapter.New(direct, map[string]string{"events": topic}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Publish(ctx, m).Outcome != transport.Confirmed {
		t.Fatal("retry not confirmed")
	}
	if err = retry.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case body := <-received:
			if string(body) != string(m.Input().Payload) {
				t.Fatal("wire changed across lost ACK/retry")
			}
		case <-ctx.Done():
			t.Fatal("missing physical delivery", ctx.Err())
		}
	}
	// This proves duplicate physical delivery with preserved wire, not host
	// consumer idempotency or fsync durability. Those have separate acceptance.
}
