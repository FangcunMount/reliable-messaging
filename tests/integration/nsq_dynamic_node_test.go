//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

// This test runs in the disposable MySQL container. It starts an isolated
// second nsqd as a child process, so the subscriber stays alive across a real
// node kill and restart. The harness supplies the binary from its pinned image.
func TestNSQSubscriberLateNodeKillAndRejoin(t *testing.T) {
	primary, primaryHTTP, lookupd, binary := os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP"), os.Getenv("RM_TEST_NSQ_LOOKUPD"), os.Getenv("RM_TEST_NSQD_BINARY")
	if primary == "" || primaryHTTP == "" || lookupd == "" || binary == "" {
		t.Fatal("isolated NSQ, lookupd and pinned nsqd binary required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	topic := fmt.Sprintf("rm-node-rejoin-%d", time.Now().UnixNano())
	const channel = "business"
	httpClient := &http.Client{Timeout: 2 * time.Second}
	primaryProvisioner, err := adapter.NewProvisioner(httpClient, []string{primaryHTTP})
	if err != nil {
		t.Fatal(err)
	}
	if err := primaryProvisioner.EnsureChannel(ctx, topic, channel); err != nil {
		t.Fatal(err)
	}
	waitForNSQSource(t, ctx, httpClient, lookupd, topic, "nsqd", 4150)

	cfg := driver.NewConfig()
	cfg.HeartbeatInterval = time.Second
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.LookupdPollInterval = 250 * time.Millisecond
	cfg.LookupdPollJitter = 0
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		LookupdAddresses: []string{lookupd}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, Retry: adapter.Backoff{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan legacy.FailedHandoff, 4)
	if err := subscriber.Subscribe(ctx, topic, channel, func(_ context.Context, delivery transport.Delivery) error {
		if delivery.Message().ID == "" || delivery.Message().TransportID == "" {
			return errors.New("missing application or physical identity")
		}
		return errors.New("dynamic node business failure")
	}, func(_ context.Context, record legacy.FailedHandoff) error {
		failed <- record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		if err := subscriber.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()

	dataDir := filepath.Join(t.TempDir(), "peer-data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	peerHTTP := "http://127.0.0.1:4251"
	peerProvisioner, err := adapter.NewProvisioner(httpClient, []string{peerHTTP})
	if err != nil {
		t.Fatal(err)
	}
	var peer *exec.Cmd
	stopPeer := func() {
		if peer != nil {
			_ = peer.Process.Kill()
			_ = peer.Wait()
			peer = nil
		}
	}
	defer stopPeer()
	startPeer := func() {
		logPath := filepath.Join(t.TempDir(), "nsqd-peer.log")
		peer = exec.Command(binary,
			"--tcp-address=0.0.0.0:4250", "--http-address=0.0.0.0:4251",
			"--broadcast-address=mysql", "--lookupd-tcp-address=nsqlookupd:4160",
			"--data-path="+dataDir, "--mem-queue-size=0", "--sync-every=1",
		)
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		peer.Stdout, peer.Stderr = logFile, logFile
		if err := peer.Start(); err != nil {
			logFile.Close()
			t.Fatal(err)
		}
		logFile.Close()
		bootCtx, bootCancel := context.WithTimeout(ctx, 8*time.Second)
		defer bootCancel()
		waitForNSQPing(t, bootCtx, httpClient, peerHTTP, logPath)
		if err := peerProvisioner.EnsureChannel(ctx, topic, channel); err != nil {
			t.Fatal(err)
		}
		waitForNSQSource(t, ctx, httpClient, lookupd, topic, "mysql", 4250)
		waitForNSQChannelClient(t, ctx, httpClient, peerHTTP, topic, channel)
	}

	startPeer()
	firstTransportID := publishAndAwaitNSQFailure(t, ctx, cfg, "127.0.0.1:4250", topic, "late-node-uuid", "", failed)
	stopPeer() // a real TCP disconnect while Subscriber and lookupd keep running
	startPeer()
	secondTransportID := publishAndAwaitNSQFailure(t, ctx, cfg, "127.0.0.1:4250", topic, "rejoined-node-uuid", "late-node-uuid", failed)
	if firstTransportID == secondTransportID {
		t.Fatal("two physical deliveries shared a transport identity")
	}
}

func publishAndAwaitNSQFailure(t *testing.T, ctx context.Context, cfg *driver.Config, address, topic, uuid, priorUUID string, failed <-chan legacy.FailedHandoff) string {
	t.Helper()
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	body, err := legacy.Encode(legacy.Envelope{UUID: uuid, Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case record := <-failed:
			if record.Topic != topic || record.Channel != "business" || record.TransportMessageID == "" || record.Cause != "dynamic node business failure" {
				t.Fatalf("dynamic handoff changed identity or cause: %+v", record)
			}
			if record.UUID == priorUUID {
				// A broker kill may replay a previously delivered physical
				// failure record. Host audit must deduplicate it.
				t.Logf("observed permitted replay of %s after node restart", priorUUID)
				continue
			}
			if record.UUID != uuid {
				t.Fatalf("unexpected application identity after node restart: %q", record.UUID)
			}
			return record.TransportMessageID
		case <-ctx.Done():
			t.Fatal("dynamic node handoff did not settle: ", ctx.Err())
			return ""
		}
	}
}

func waitForNSQPing(t *testing.T, ctx context.Context, client *http.Client, endpoint, logPath string) {
	t.Helper()
	for {
		response, err := client.Get(endpoint + "/ping")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			log, _ := os.ReadFile(logPath)
			t.Fatalf("peer NSQ did not start: %v; log: %s", ctx.Err(), log)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForNSQSource(t *testing.T, ctx context.Context, client *http.Client, lookupd, topic, host string, port int) {
	t.Helper()
	waitForNSQCondition(t, ctx, "lookupd source "+host, func() bool {
		response, err := client.Get("http://" + lookupd + "/lookup?topic=" + url.QueryEscape(topic))
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		var result struct {
			Producers []struct {
				BroadcastAddress string `json:"broadcast_address"`
				TCPPort          int    `json:"tcp_port"`
			} `json:"producers"`
		}
		if json.NewDecoder(response.Body).Decode(&result) != nil {
			return false
		}
		for _, source := range result.Producers {
			if source.BroadcastAddress == host && source.TCPPort == port {
				return true
			}
		}
		return false
	})
}

func waitForNSQChannelClient(t *testing.T, ctx context.Context, client *http.Client, endpoint, topic, channel string) {
	t.Helper()
	waitForNSQCondition(t, ctx, "business consumer on peer", func() bool {
		response, err := client.Get(endpoint + "/stats?format=json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		var result struct {
			Topics []struct {
				Name     string `json:"topic_name"`
				Channels []struct {
					Name    string            `json:"channel_name"`
					Clients []json.RawMessage `json:"clients"`
				} `json:"channels"`
			} `json:"topics"`
		}
		if json.NewDecoder(response.Body).Decode(&result) != nil {
			return false
		}
		for _, currentTopic := range result.Topics {
			if currentTopic.Name == topic {
				for _, currentChannel := range currentTopic.Channels {
					if currentChannel.Name == channel && len(currentChannel.Clients) > 0 {
						return true
					}
				}
			}
		}
		return false
	})
}

func waitForNSQCondition(t *testing.T, ctx context.Context, what string, ready func() bool) {
	t.Helper()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
