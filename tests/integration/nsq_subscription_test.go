//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

type subscriptionHandoff struct {
	direct    *adapter.DirectHandoff
	mu        sync.Mutex
	publishes int
}

func (h *subscriptionHandoff) Ready(ctx context.Context, address, topic string) error {
	return h.direct.Ready(ctx, address, topic)
}

func (h *subscriptionHandoff) Publish(ctx context.Context, address, topic string, body []byte) transport.Result {
	result := h.direct.Publish(ctx, address, topic, body)
	if result.Outcome != transport.Confirmed {
		return result
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.publishes++
	if h.publishes == 1 {
		// Model a lost confirmation after the real broker accepted the handoff.
		return transport.Result{Outcome: transport.Unknown}
	}
	return transport.Result{Outcome: transport.Confirmed}
}

func TestNSQSubscriptionTerminalHandoffLostConfirmation(t *testing.T) {
	address := os.Getenv("RM_TEST_NSQ_TCP")
	if address == "" {
		t.Fatal("isolated NSQ address required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-subscription-%d", time.Now().UnixNano())
	const channel = "business"
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.MaxInFlight = 1
	failureConfig := *cfg
	failureConfig.MaxAttempts = 0
	failureConsumer, err := driver.NewConsumer(legacy.FailedHandoffTopic(topic, channel), legacy.FailedHandoffChannel, &failureConfig)
	if err != nil {
		t.Fatal(err)
	}
	failureConsumer.SetLogger(nil, driver.LogLevelError)
	directHandoff, err := adapter.NewDirectHandoff(failureConsumer, cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := directHandoff.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	handoff := &subscriptionHandoff{direct: directHandoff}
	var mu sync.Mutex
	businessCalls := 0
	failed := make(chan legacy.FailedHandoff, 4)
	s, err := adapter.NewSubscription(adapter.SubscriptionConfig{
		Topic: topic, Channel: channel, MaxAttempts: 2,
		Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
		Handler: func(_ context.Context, delivery transport.Delivery) error {
			mu.Lock()
			businessCalls++
			mu.Unlock()
			if delivery.Message().ID != "application-uuid" {
				return errors.New("application identity changed")
			}
			return errors.New("business failure")
		},
		FailedHandler: func(_ context.Context, record legacy.FailedHandoff) error {
			failed <- record
			return nil
		},
		Handoff: handoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	failureConsumer.AddHandler(s.FailureHandler(ctx))
	if err := failureConsumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer stopNSQConsumer(t, failureConsumer)
	businessConsumer, err := driver.NewConsumer(topic, channel, s.ConsumerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	businessConsumer.SetLogger(nil, driver.LogLevelError)
	businessConsumer.AddHandler(s.BusinessHandler(ctx))
	if err := businessConsumer.ConnectToNSQD(address); err != nil {
		t.Fatal(err)
	}
	defer stopNSQConsumer(t, businessConsumer)
	body, err := legacy.Encode(legacy.Envelope{UUID: "application-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case record := <-failed:
			if record.UUID != "application-uuid" || record.TransportMessageID == "" || record.Topic != topic || record.Channel != channel || record.Cause != "business failure" {
				t.Fatalf("handoff identity or cause changed: %+v", record)
			}
		case <-ctx.Done():
			t.Fatal("missing failure handoff after lost confirmation: ", ctx.Err())
		}
	}
	mu.Lock()
	calls := businessCalls
	mu.Unlock()
	// The failure consumer may receive the second broker publish before the
	// publishing goroutine has returned and updated this test probe.
	var publishes int
	for {
		handoff.mu.Lock()
		publishes = handoff.publishes
		handoff.mu.Unlock()
		if publishes >= 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("second handoff publish did not complete: business=%d handoffs=%d: %v", calls, publishes, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if calls != 2 || publishes != 2 {
		t.Fatalf("business re-executed after exhaustion or handoff lost: business=%d handoffs=%d", calls, publishes)
	}
}

func TestNSQSubscriberOwnsConsumersAndTerminalHandoff(t *testing.T) {
	address := os.Getenv("RM_TEST_NSQ_TCP")
	if address == "" {
		t.Fatal("isolated NSQ address required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-runtime-%d", time.Now().UnixNano())
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.MaxInFlight = 1
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		NSQDAddresses: []string{address}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan legacy.FailedHandoff, 1)
	setupCtx, stopSetup := context.WithTimeout(ctx, 5*time.Second)
	if err := subscriber.Subscribe(setupCtx, topic, "business", func(deliveryCtx context.Context, delivery transport.Delivery) error {
		if err := deliveryCtx.Err(); err != nil {
			return fmt.Errorf("registration timeout leaked into delivery: %w", err)
		}
		if delivery.Message().ID != "runtime-uuid" {
			return errors.New("application identity changed")
		}
		return errors.New("expected business failure")
	}, func(_ context.Context, record legacy.FailedHandoff) error {
		failed <- record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stopSetup() // registration context is not the delivery lifetime
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	body, err := legacy.Encode(legacy.Envelope{UUID: "runtime-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-failed:
		if record.UUID != "runtime-uuid" || record.TransportMessageID == "" || record.Cause != "expected business failure" {
			t.Fatalf("runtime handoff lost evidence: %+v", record)
		}
	case <-ctx.Done():
		t.Fatal("runtime handoff missing: ", ctx.Err())
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := subscriber.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Subscribe(ctx, topic, "another", func(context.Context, transport.Delivery) error { return nil }, func(context.Context, legacy.FailedHandoff) error { return nil }); err == nil {
		t.Fatal("closed subscriber accepted a new subscription")
	}
}

func TestNSQSubscriberLookupdTopology(t *testing.T) {
	address, lookupd, nsqdHTTP := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_LOOKUPD"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || lookupd == "" || nsqdHTTP == "" {
		t.Fatal("isolated NSQ and lookupd addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-lookupd-%d", time.Now().UnixNano())
	client := &http.Client{Timeout: 2 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, nsqdHTTP+"/topic/create?topic="+url.QueryEscape(topic), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create topic status %s", response.Status)
	}
	for {
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lookupd+"/lookup?topic="+url.QueryEscape(topic), nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = client.Do(request)
		if err == nil {
			var found struct {
				Producers []json.RawMessage `json:"producers"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&found)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && len(found.Producers) > 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("lookupd did not register topic %s: %v", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.MaxInFlight = 1
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		LookupdAddresses: []string{lookupd}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan legacy.FailedHandoff, 1)
	if err := subscriber.Subscribe(ctx, topic, "business", func(context.Context, transport.Delivery) error {
		return errors.New("lookupd business failure")
	}, func(_ context.Context, record legacy.FailedHandoff) error {
		failed <- record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	body, err := legacy.Encode(legacy.Envelope{UUID: "lookupd-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-failed:
		if record.UUID != "lookupd-uuid" || record.TransportMessageID == "" || record.Cause != "lookupd business failure" {
			t.Fatalf("lookupd handoff lost evidence: %+v", record)
		}
	case <-ctx.Done():
		t.Fatal("lookupd handoff missing: ", ctx.Err())
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := subscriber.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

// A lookupd HTTP outage must not settle an already connected broker delivery
// early. The same Subscriber also has to resume polling after discovery returns.
// This does not prove discovery of a new nsqd while lookupd is unavailable.
func TestNSQSubscriberLookupdOutageWithConnectedBroker(t *testing.T) {
	address, lookupd, nsqdHTTP := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_LOOKUPD"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || lookupd == "" || nsqdHTTP == "" {
		t.Fatal("isolated NSQ and lookupd addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-lookupd-outage-%d", time.Now().UnixNano())
	client := &http.Client{Timeout: 2 * time.Second}
	provisioner, err := adapter.NewProvisioner(client, []string{nsqdHTTP})
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioner.EnsureChannel(ctx, topic, "business"); err != nil {
		t.Fatal(err)
	}
	waitForNSQSource(t, ctx, client, lookupd, topic, "nsqd", 4150)

	var available atomic.Bool
	available.Store(true)
	var rejected, successful atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !available.Load() {
			rejected.Add(1)
			http.Error(w, "lookupd temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		upstream, err := http.NewRequestWithContext(request.Context(), request.Method, "http://"+lookupd+request.URL.RequestURI(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		upstream.Header = request.Header.Clone() // preserve NSQ's version negotiation
		response, err := client.Do(upstream)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		for name, values := range response.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		if response.StatusCode == http.StatusOK {
			successful.Add(1)
		}
	}))
	defer proxy.Close()
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.LookupdPollInterval = 250 * time.Millisecond
	cfg.LookupdPollJitter = 0
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		LookupdAddresses: []string{strings.TrimPrefix(proxy.URL, "http://")}, Driver: cfg,
		MaxInFlight: 1, MaxAttempts: 1,
		Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan legacy.FailedHandoff, 4)
	if err := subscriber.Subscribe(ctx, topic, "business", func(context.Context, transport.Delivery) error {
		return errors.New("dynamic node business failure")
	}, func(_ context.Context, record legacy.FailedHandoff) error {
		failed <- record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// ConnectToNSQLookupds starts asynchronous discovery. The outage should
	// begin only after both broker channels have actual consumer clients.
	waitForNSQChannelClient(t, ctx, client, nsqdHTTP, topic, "business")
	waitForNSQChannelClient(t, ctx, client, nsqdHTTP, legacy.FailedHandoffTopic(topic, "business"), legacy.FailedHandoffChannel)
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		if err := subscriber.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()

	available.Store(false)
	waitForNSQCondition(t, ctx, "lookupd poll during outage", func() bool { return rejected.Load() > 0 })
	firstID := publishAndAwaitNSQFailure(t, ctx, cfg, address, topic, "lookupd-down-uuid", "", failed)
	if firstID == "" {
		t.Fatal("lookupd outage lost source transport identity")
	}
	beforeRecovery := successful.Load()
	available.Store(true)
	waitForNSQCondition(t, ctx, "lookupd polling recovery", func() bool { return successful.Load() > beforeRecovery })
	secondID := publishAndAwaitNSQFailure(t, ctx, cfg, address, topic, "lookupd-recovered-uuid", "lookupd-down-uuid", failed)
	if secondID == "" || secondID == firstID {
		t.Fatal("lookupd recovery did not preserve distinct physical deliveries")
	}
}

func TestNSQSubscriberBeforeTopicRegistration(t *testing.T) {
	address, lookupd, nsqdHTTP := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_LOOKUPD"), os.Getenv("RM_TEST_NSQ_HTTP")
	if address == "" || lookupd == "" || nsqdHTTP == "" {
		t.Fatal("isolated NSQ and lookupd addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	topic := fmt.Sprintf("rm-late-topic-%d", time.Now().UnixNano())
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lookupd+"/nodes", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			var found struct {
				Producers []json.RawMessage `json:"producers"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&found)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && len(found.Producers) > 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("lookupd did not register any nsqd: ", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lookupd+"/lookup?topic="+url.QueryEscape(topic), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("late topic already exists in lookupd: %s", response.Status)
	}
	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.LookupdPollInterval = 100 * time.Millisecond
	cfg.LookupdPollJitter = 0
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		LookupdAddresses: []string{lookupd}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan legacy.FailedHandoff, 1)
	if err := subscriber.Subscribe(ctx, topic, "business", func(context.Context, transport.Delivery) error {
		return errors.New("late topic failure")
	}, func(_ context.Context, record legacy.FailedHandoff) error {
		failed <- record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, nsqdHTTP+"/topic/create?topic="+url.QueryEscape(topic), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create late topic status %s", response.Status)
	}
	for {
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, nsqdHTTP+"/stats?format=json", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = client.Do(request)
		if err == nil {
			var stats struct {
				Topics []struct {
					Name     string `json:"topic_name"`
					Channels []struct {
						Name    string            `json:"channel_name"`
						Clients []json.RawMessage `json:"clients"`
					} `json:"channels"`
				} `json:"topics"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&stats)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil {
				for _, current := range stats.Topics {
					if current.Name == topic {
						for _, channel := range current.Channels {
							if channel.Name == "business" && len(channel.Clients) > 0 {
								goto connected
							}
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("business consumer did not discover late topic: ", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
connected:
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	body, err := legacy.Encode(legacy.Envelope{UUID: "late-topic-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-failed:
		if record.UUID != "late-topic-uuid" || record.TransportMessageID == "" || record.Cause != "late topic failure" {
			t.Fatalf("late topic handoff lost evidence: %+v", record)
		}
	case <-ctx.Done():
		t.Fatal("late topic handoff missing: ", ctx.Err())
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := subscriber.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func stopNSQConsumer(t *testing.T, consumer *driver.Consumer) {
	t.Helper()
	consumer.Stop()
	select {
	case <-consumer.StopChan:
	case <-time.After(5 * time.Second):
		t.Error("NSQ consumer did not stop")
	}
}
