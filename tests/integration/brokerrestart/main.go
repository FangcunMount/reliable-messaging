// Command brokerrestart is an isolated two-phase integration fixture. The shell
// harness must restart its own nsqd container between seed and recover.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	sdknsq "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/nsqio/go-nsq"
)

const topic = "rm-restart"
const channel = "rm-restart"

type manifest struct {
	Bodies map[string]string `json:"bodies"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 3 {
		return errors.New("require seed|recover and manifest path")
	}
	address, httpAddress := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || httpAddress == "" {
		return errors.New("isolated NSQ addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config := nsq.NewConfig()
	config.ReadTimeout = 3 * time.Second
	config.HeartbeatInterval = time.Second
	config.WriteTimeout = time.Second
	switch os.Args[1] {
	case "seed":
		return seed(ctx, address, httpAddress, os.Args[2], config)
	case "recover":
		return recoverMessages(ctx, address, os.Args[2], config)
	default:
		return errors.New("unknown phase")
	}
}
func seed(ctx context.Context, address, httpAddress, path string, config *nsq.Config) error {
	client := &http.Client{Timeout: 3 * time.Second}
	for _, endpoint := range []string{"/topic/create?topic=" + topic, "/channel/create?topic=" + topic + "&channel=" + channel} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpAddress+endpoint, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("create subscription: %d", response.StatusCode)
		}
	}
	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return err
	}
	defer producer.Stop()
	producer.SetLogger(nil, nsq.LogLevelError)
	publisher, err := sdknsq.New(producer, map[string]string{"restart": topic}, 1)
	if err != nil {
		return err
	}
	m := manifest{Bodies: map[string]string{}}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("original-%02d", i)
		body := []byte(fmt.Sprintf(`{"id":%q,"extension":{"preserve":true}}`, id))
		hash := sha256.Sum256(body)
		m.Bodies[hex.EncodeToString(hash[:])] = string(body)
		intent, err := message.New(message.Input{Producer: "restart-fixture", ID: id, Destination: "restart", EventType: "created", SchemaVersion: "v1", Scope: "global", ContentType: "application/json", OccurredAt: "2026-09-22T00:00:00Z", Payload: body})
		if err != nil {
			return err
		}
		if result := publisher.Publish(ctx, intent); result.Outcome != transport.Confirmed {
			return fmt.Errorf("unconfirmed publish %s: %v", id, result)
		}
	}
	if err := publisher.Drain(ctx); err != nil {
		return err
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path, encoded, 0600); err != nil {
		return err
	}
	fmt.Println("PASS seeded 10 broker-confirmed messages on a precreated durable channel")
	return nil
}
func recoverMessages(ctx context.Context, address, path string, config *nsq.Config) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		return err
	}
	if len(m.Bodies) != 10 {
		return errors.New("incomplete restart manifest")
	}
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	consumer.SetLogger(nil, nsq.LogLevelError)
	bodies := make(chan []byte, 32)
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		select {
		case bodies <- append([]byte(nil), msg.Body...):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	defer consumer.Stop()
	if err = consumer.ConnectToNSQD(address); err != nil {
		return err
	}
	seen := map[string]bool{}
	for len(seen) < len(m.Bodies) {
		select {
		case body := <-bodies:
			hash := sha256.Sum256(body)
			key := hex.EncodeToString(hash[:])
			expected, ok := m.Bodies[key]
			if !ok || expected != string(body) {
				return errors.New("unexpected or changed body after restart")
			}
			seen[key] = true
		case <-ctx.Done():
			return fmt.Errorf("restart recovered %d/%d original messages: %w", len(seen), len(m.Bodies), ctx.Err())
		}
	}
	consumer.Stop()
	select {
	case <-consumer.StopChan:
	case <-ctx.Done():
		return fmt.Errorf("consumer drain: %w", ctx.Err())
	}
	fmt.Println("PASS after real broker restart: 10/10 original message identities and bytes consumed")
	return nil
}
