// quorumproof is an isolated broker primitive experiment. It is not an SDK
// adapter or evidence that IAM/QS business messages have been processed.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	sdknsq "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/nsqio/go-nsq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	queueName  = "rm.b0.quorum.node-loss"
	originalID = "b0-confirmed-before-leader-loss"
	nsqTopic   = "rm-b0-comparison"
)

var originalBody = []byte(`{"identity":"b0-confirmed-before-leader-loss"}`)

type queueStatus struct {
	Type    string   `json:"type"`
	Leader  string   `json:"leader"`
	Members []string `json:"members"`
	Online  []string `json:"online"`
}

type nodeStatus struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
}

func getJSON(ctx context.Context, path string, value any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:15672"+path, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("rmtest", "rmtest")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("management %s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(value)
}

func waitNodes(ctx context.Context, expected int) error {
	for {
		var nodes []nodeStatus
		if err := getJSON(ctx, "/api/nodes", &nodes); err == nil {
			running := 0
			for _, node := range nodes {
				if node.Running {
					running++
				}
			}
			if len(nodes) == 3 && running == expected {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for 3 cluster members and %d running: %w", expected, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func waitQueue(ctx context.Context, leader string, online int) error {
	for {
		var queue queueStatus
		if err := getJSON(ctx, "/api/queues/%2F/"+queueName, &queue); err == nil {
			sort.Strings(queue.Members)
			leaderMatches := queue.Leader == leader || leader == "survivor" &&
				(queue.Leader == "rabbit@rabbitmq2" || queue.Leader == "rabbit@rabbitmq3")
			if queue.Type == "quorum" && leaderMatches && len(queue.Members) == 3 &&
				queue.Members[0] == "rabbit@rabbitmq1" && queue.Members[1] == "rabbit@rabbitmq2" &&
				queue.Members[2] == "rabbit@rabbitmq3" && len(queue.Online) == online {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for quorum queue leader %s with %d online members: %w", leader, online, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func publish(ctx context.Context, channel *amqp.Channel, id string, body []byte) error {
	if err := channel.Confirm(false); err != nil {
		return err
	}
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", queueName, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, MessageId: id, ContentType: "application/json", Body: body,
	})
	if err != nil || confirmation == nil {
		return fmt.Errorf("publish: confirmation=%v error=%w", confirmation, err)
	}
	select {
	case <-confirmation.Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	if !confirmation.Acked() {
		return errors.New("broker rejected publication")
	}
	select {
	case returned := <-returns:
		return fmt.Errorf("message %s was returned as unroutable", returned.MessageId)
	default:
	}
	return nil
}

func getAndAck(ctx context.Context, channel *amqp.Channel, id string, body []byte) error {
	for {
		delivery, found, err := channel.Get(queueName, false)
		if err != nil {
			return err
		}
		if found {
			if delivery.MessageId != id || !bytes.Equal(delivery.Body, body) || delivery.DeliveryMode != amqp.Persistent {
				return fmt.Errorf("delivery changed original identity, bytes or persistence mode")
			}
			return delivery.Ack(false)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func waitNSQDepth(ctx context.Context, expected int64) error {
	client := &http.Client{Timeout: 3 * time.Second}
	stable := 0
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://nsqd:4151/stats?format=json", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		var stats struct {
			Topics []struct {
				Name     string `json:"topic_name"`
				Depth    *int64 `json:"depth"`
				Backend  *int64 `json:"backend_depth"`
				Channels []struct {
					Name     string `json:"channel_name"`
					Depth    *int64 `json:"depth"`
					Backend  *int64 `json:"backend_depth"`
					InFlight *int64 `json:"in_flight_count"`
					Deferred *int64 `json:"deferred_count"`
				} `json:"channels"`
			} `json:"topics"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&stats)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil {
			return fmt.Errorf("NSQ stats: HTTP %d decode %v", resp.StatusCode, decodeErr)
		}
		matched := false
		for _, topic := range stats.Topics {
			if topic.Name != nsqTopic || topic.Depth == nil || topic.Backend == nil || *topic.Depth != 0 || *topic.Backend != 0 {
				continue
			}
			for _, channel := range topic.Channels {
				if channel.Name == nsqTopic && channel.Depth != nil && channel.Backend != nil &&
					channel.InFlight != nil && channel.Deferred != nil && *channel.Depth == expected &&
					*channel.Backend == 0 && *channel.InFlight == 0 && *channel.Deferred == 0 {
					matched = true
				}
			}
		}
		if matched {
			stable++
			if stable >= 3 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("NSQ channel expected depth %d not stable: %w", expected, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func seedNSQ(ctx context.Context) error {
	for _, path := range []string{
		"/topic/create?topic=" + nsqTopic,
		"/channel/create?topic=" + nsqTopic + "&channel=" + nsqTopic,
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://nsqd:4151"+path, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("create NSQ channel: HTTP %d", resp.StatusCode)
		}
	}
	config := nsq.NewConfig()
	config.ReadTimeout = 3 * time.Second
	config.HeartbeatInterval = time.Second
	config.WriteTimeout = time.Second
	producer, err := nsq.NewProducer("nsqd:4150", config)
	if err != nil {
		return err
	}
	defer producer.Stop()
	producer.SetLogger(nil, nsq.LogLevelError)
	publisher, err := sdknsq.New(producer, map[string]string{"assessment": nsqTopic}, 1)
	if err != nil {
		return err
	}
	intent, err := message.New(message.Input{
		Producer: "b0-comparison", ID: originalID, Destination: "assessment",
		EventType: "answersheet.submitted", SchemaVersion: "1", Scope: "global",
		ContentType: "application/json", OccurredAt: "2026-09-27T00:00:00+08:00", Payload: originalBody,
	})
	if err != nil {
		return err
	}
	if outcome := publisher.Publish(ctx, intent).Outcome; outcome != transport.Confirmed {
		return fmt.Errorf("NSQ SDK outcome %v, want Confirmed", outcome)
	}
	if err := publisher.Drain(ctx); err != nil {
		return err
	}
	return waitNSQDepth(ctx, 1)
}

func run(phase string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch phase {
	case "seed":
		if err := waitNodes(ctx, 3); err != nil {
			return err
		}
	case "recover":
		if err := waitNodes(ctx, 2); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown phase %q", phase)
	}
	conn, err := amqp.Dial("amqp://rmtest:rmtest@127.0.0.1:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if phase == "seed" {
		if _, err := channel.QueueDeclare(queueName, true, false, false, false, amqp.Table{
			"x-queue-type": "quorum", "x-quorum-initial-group-size": int32(3), "x-queue-leader-locator": "client-local",
		}); err != nil {
			return err
		}
		if err := waitQueue(ctx, "rabbit@rabbitmq1", 3); err != nil {
			return err
		}
		if err := publish(ctx, channel, originalID, originalBody); err != nil {
			return err
		}
		if err := seedNSQ(ctx); err != nil {
			return err
		}
		fmt.Println("PASS same original ID/body confirmed by RabbitMQ quorum and NSQ; NSQ copy observed only in channel memory")
		return nil
	}
	if err := waitQueue(ctx, "survivor", 2); err != nil {
		return err
	}
	if err := getAndAck(ctx, channel, originalID, originalBody); err != nil {
		return err
	}
	if err := waitNSQDepth(ctx, 0); err != nil {
		return err
	}
	const afterID = "b0-after-leader-loss"
	afterBody := []byte(`{"identity":"b0-after-leader-loss"}`)
	if err := publish(ctx, channel, afterID, afterBody); err != nil {
		return err
	}
	if err := getAndAck(ctx, channel, afterID, afterBody); err != nil {
		return err
	}
	fmt.Println("PASS same original survived RabbitMQ leader SIGKILL; NSQ confirmed in-memory copy was lost after nsqd SIGKILL; RabbitMQ majority accepted a new message")
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: quorumproof seed|recover")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
