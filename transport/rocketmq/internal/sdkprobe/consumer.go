package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/transport/rocketmq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
)

// Deliberate JSON whitespace proves the FIRST wire bytes survive persistence,
// producer RPCs and public SDK consumer decoding without reserialization.
func fixtureWire(id string) []byte {
	body, err := legacy.Encode(legacy.Envelope{UUID: id, Payload: []byte("sdk:" + id), Metadata: map[string]string{"fixture": "m2-sdk", "unicode": "测评 <&>"}}, legacy.Revision2)
	if err != nil {
		panic("fixture envelope")
	}
	return append(append([]byte{'\n'}, body...), '\n')
}
func observerRoutes(c config) []transport.RouteBinding {
	if len(c.Inputs) > 0 {
		return c.Inputs
	}
	return []transport.RouteBinding{{Topic: "sdk.logical", Subscription: c.Subscription, PhysicalTopic: c.Topic, ConsumerGroup: c.Group, MessageType: transport.TransactionMessageType, Owner: c.Owner, RouteRevision: c.Revision}}
}
func consume(c config, tlsConfig *tls.Config, shared bool) error {
	routes := observerRoutes(c)
	subscriber, err := rocketmq.NewSubscriber(rocketmq.SubscriberConfig{Config: rocketmq.Config{Endpoint: c.Endpoint, AccessKey: c.AccessKey, SecretKey: c.SecretKey, TLS: tlsConfig, Routes: routes, RequestTimeout: 5 * time.Second, MaxInFlight: 2}, MaxAttempts: 8, InvisibleDuration: 15 * time.Second, HandlerTimeout: 60 * time.Second, RetryDelay: time.Second})
	if err != nil {
		return err
	}
	release, stop := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	if !shared {
		releaseOnce.Do(func() { close(release) })
	}
	var active, maximum atomic.Int32
	handler := func(ctx context.Context, delivery transport.Delivery) error {
		received := delivery.Message()
		if received.ID == "" || received.Topic != "sdk.logical" || !bytes.Equal(received.Wire, fixtureWire(received.ID)) || string(received.Payload) != "sdk:"+received.ID {
			return errors.New("public SDK original wire or ID mismatch")
		}
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous; previous = maximum.Load() {
			if maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		if shared {
			emit("handler_enter", received.ID, strconv.Itoa(int(current)))
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
		emit("received", received.ID, "exact_wire_public_sdk_acknowledged")
		return nil
	}
	failed := func(context.Context, transport.Failure) error {
		return errors.New("unexpected public SDK durable failure path")
	}
	if err = subscriber.Subscribe(context.Background(), "sdk.logical", c.Subscription, handler, failed); err != nil {
		return err
	}
	emit("ready", "", "public_sdk_consumer")
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if scanner.Text() == "release" {
				releaseOnce.Do(func() { close(release) })
			}
			if scanner.Text() == "stop" {
				break
			}
		}
		close(stop)
	}()
	select {
	case <-stop:
	case err = <-subscriber.Errors():
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	closeErr := subscriber.Close(ctx)
	emit("max_inflight", "", strconv.Itoa(int(maximum.Load())))
	if closeErr == nil {
		emit("closed", "", "consumer_drained")
	}
	return errors.Join(err, closeErr)
}
func normal(c config, tlsConfig *tls.Config, id string) error {
	publisher, err := rocketmq.NewPublisher(rocketmq.Config{Endpoint: c.Endpoint, AccessKey: c.AccessKey, SecretKey: c.SecretKey, TLS: tlsConfig, Routes: []transport.RouteBinding{{Topic: "sdk.logical", PhysicalTopic: c.Topic, MessageType: transport.NormalMessageType, Owner: c.Owner, RouteRevision: c.Revision}}, RequestTimeout: 5 * time.Second, MaxInFlight: 2})
	if err != nil {
		return err
	}
	if err = publisher.Start(); err != nil {
		return err
	}
	count := c.Branches
	if count == 0 {
		count = 1
	}
	for i := 0; i < count; i++ {
		messageID := id
		if count > 1 {
			messageID = fmt.Sprintf("%s-b%d", id, i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := publisher.PublishRaw(ctx, "sdk.logical", fixtureWire(messageID))
		cancel()
		if result.Outcome != transport.Confirmed {
			return errors.New("normal fixture publish unconfirmed")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = publisher.Close(ctx); err == nil {
		emit("closed", id, "normal_drained")
	}
	return err
}
