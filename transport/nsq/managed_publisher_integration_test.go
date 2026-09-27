//go:build integration

package nsq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
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

func TestManagedPublisherInterruptsRealUnconfirmedSend(t *testing.T) {
	address := os.Getenv("RM_TEST_NSQ_TCP")
	if address == "" {
		t.Fatal("isolated NSQ TCP address required")
	}
	proxyAddress, accepted, closeProxy := startManagedAckHoldProxy(t, address)
	defer closeProxy()
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = time.Second
	managed, err := NewManagedPublisher(ManagedPublisherConfig{Address: proxyAddress, Driver: cfg, MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := managed.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	topic := fmt.Sprintf("rm-managed-interrupt-%d", time.Now().UnixNano())
	published := make(chan transport.Result, 1)
	go func() { published <- managed.PublishRaw(context.Background(), topic, []byte("same-original-id")) }()
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal("broker did not accept unconfirmed PUB: ", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not accept managed publication")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if err := managed.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("graceful close settled unconfirmed publication: %v", err)
	}
	cancel()
	managed.Interrupt()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := managed.Close(ctx); err != nil {
		t.Fatal("interrupted producer did not drain: ", err)
	}
	select {
	case result := <-published:
		if result.Outcome != transport.Unknown {
			t.Fatalf("interrupted publication = %v", result.Outcome)
		}
	case <-ctx.Done():
		t.Fatal("interrupted publication did not finish: ", ctx.Err())
	}
}

// Ping writes a NOP without a broker response. The first OK proves nsqd
// accepted PUB, but is held until SDK Interrupt closes the producer socket.
func startManagedAckHoldProxy(t *testing.T, address string) (string, <-chan error, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	go func() {
		defer listener.Close()
		downstream, err := listener.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer downstream.Close()
		_ = downstream.SetDeadline(time.Now().Add(15 * time.Second))
		upstream, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			accepted <- err
			return
		}
		defer upstream.Close()
		_ = upstream.SetDeadline(time.Now().Add(15 * time.Second))
		copied := make(chan struct{})
		go func() { defer close(copied); _, _ = io.Copy(upstream, downstream) }()
		defer func() { downstream.Close(); upstream.Close(); <-copied }()
		for {
			var header [4]byte
			if _, err = io.ReadFull(upstream, header[:]); err != nil {
				accepted <- err
				return
			}
			n := binary.BigEndian.Uint32(header[:])
			if n < 4 || n > 1024*1024 {
				accepted <- fmt.Errorf("unexpected NSQ frame length %d", n)
				return
			}
			frame := make([]byte, n)
			if _, err = io.ReadFull(upstream, frame); err != nil {
				accepted <- err
				return
			}
			if binary.BigEndian.Uint32(frame[:4]) == 0 && string(frame[4:]) == "OK" {
				accepted <- nil
				select {
				case <-copied:
				case <-release:
				}
				return
			}
			if _, err = downstream.Write(append(header[:], frame...)); err != nil {
				accepted <- err
				return
			}
		}
	}()
	return listener.Addr().String(), accepted, func() {
		once.Do(func() { close(release); _ = listener.Close() })
	}
}
