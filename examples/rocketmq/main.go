// Command rocketmq probes an explicitly provisioned disposable ordinary-message
// route. No provisioning, production business effects or implicit queue reset.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/transport/rocketmq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "publish", "publish or consume")
	id := flag.String("id", "go-probe", "original application id")
	subscription := flag.String("subscription", "m1-go", "preprovisioned subscription")
	flag.Parse()
	file, err := os.ReadFile(os.Getenv("ROCKETMQ_CREDENTIALS_FILE"))
	if err != nil {
		return err
	}
	var accounts map[string]struct{ Username, Password string }
	if err = json.Unmarshal(file, &accounts); err != nil {
		return err
	}
	account := accounts[os.Getenv("ROCKETMQ_ACCOUNT")]
	if account.Username == "" {
		return errors.New("explicit test account required")
	}
	ca, err := os.ReadFile(os.Getenv("ROCKETMQ_CA_FILE"))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid CA")
	}
	topic := "m1.interop"
	c := rocketmq.Config{Endpoint: "127.0.0.1:8081", AccessKey: account.Username, SecretKey: account.Password, RequestTimeout: 5 * time.Second, MaxInFlight: 16, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, Routes: []transport.RouteBinding{{Topic: topic, Subscription: *subscription, PhysicalTopic: transport.PhysicalTopic(topic), ConsumerGroup: transport.ConsumerGroup(topic, *subscription)}}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if *mode == "publish" {
		p, err := rocketmq.NewPublisher(c)
		if err != nil {
			return err
		}
		if err = p.Start(); err != nil {
			return err
		}
		body, err := legacy.Encode(legacy.Envelope{UUID: *id, Payload: []byte("immutable interop payload"), Metadata: map[string]string{"from": "go"}}, legacy.Revision2)
		if err != nil {
			return err
		}
		result := p.PublishRaw(ctx, topic, body)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		if err = p.Close(closeCtx); err != nil {
			return err
		}
		if result.Outcome != transport.Confirmed {
			return fmt.Errorf("publish outcome %d", result.Outcome)
		}
		fmt.Println("broker_confirmed", *id)
		return nil
	}
	if *mode != "consume" {
		return errors.New("unknown mode")
	}
	s, err := rocketmq.NewSubscriber(rocketmq.SubscriberConfig{Config: c, MaxAttempts: 8, InvisibleDuration: 30 * time.Second, HandlerTimeout: time.Minute, RetryDelay: 10 * time.Second})
	if err != nil {
		return err
	}
	done := make(chan string, 16)
	if err = s.Subscribe(ctx, topic, *subscription, func(_ context.Context, d transport.Delivery) error {
		m := d.Message()
		if m.Topic != topic || m.PhysicalTopic != transport.PhysicalTopic(topic) || string(m.Payload) != "immutable interop payload" {
			return errors.New("interop wire/route mismatch")
		}
		if err := d.Ack(); err != nil {
			return err
		}
		if m.ID == *id {
			done <- m.ID
		}
		return nil
	}, func(context.Context, transport.Failure) error {
		return errors.New("probe cannot durably accept failures")
	}); err != nil {
		return err
	}
	select {
	case got := <-done:
		fmt.Println("received_and_acknowledged", got)
	case <-ctx.Done():
		err = ctx.Err()
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer closeCancel()
	return errors.Join(err, s.Close(closeCtx))
}
