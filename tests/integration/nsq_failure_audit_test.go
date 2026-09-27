//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	mysqlDriver "github.com/go-sql-driver/mysql"
	driver "github.com/nsqio/go-nsq"
)

func TestNSQSubscriberDurableFailureAudit(t *testing.T) {
	dsn, address, nsqdHTTP := os.Getenv("RM_TEST_MYSQL_DSN"), os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if dsn == "" || address == "" || nsqdHTTP == "" {
		t.Fatal("isolated MySQL and NSQ addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("rm-failure-audit-%d", time.Now().UnixNano())
	const channel = "business"
	firstWriteError := make(chan error, 1)
	persisted := make(chan legacy.FailedHandoff, 1)
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.MaxInFlight = 1
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		NSQDAddresses: []string{address}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, Retry: adapter.Backoff{BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Subscribe(ctx, topic, channel, func(context.Context, transport.Delivery) error {
		return errors.New("business failed before audit")
	}, func(ctx context.Context, record legacy.FailedHandoff) error {
		_, err := db.ExecContext(ctx, `INSERT INTO rm_failure_audit_test
  (source_topic,source_channel,application_id,source_transport_id,cause,payload)
VALUES (?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE source_transport_id=source_transport_id`,
			record.Topic, record.Channel, record.UUID, record.TransportMessageID, record.Cause, record.Payload)
		if err != nil {
			select {
			case firstWriteError <- err:
			default:
			}
			return err
		}
		select {
		case persisted <- record:
		default:
		}
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
	body, err := legacy.Encode(legacy.Envelope{UUID: "audited-uuid", Payload: []byte("original event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstWriteError:
		var mysqlErr *mysqlDriver.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1146 {
			t.Fatalf("first audit write error = %v, want missing-table MySQL 1146", err)
		}
	case <-ctx.Done():
		t.Fatal("failed audit callback was not reached: ", ctx.Err())
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE rm_failure_audit_test (
  source_topic VARCHAR(128) NOT NULL,
  source_channel VARCHAR(128) NOT NULL,
  application_id VARCHAR(128) NOT NULL,
  source_transport_id VARCHAR(64) NOT NULL,
  cause VARCHAR(255) NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (source_topic,source_channel,application_id,source_transport_id)
)`); err != nil {
		t.Fatal(err)
	}
	var record legacy.FailedHandoff
	select {
	case record = <-persisted:
	case <-ctx.Done():
		t.Fatal("failure handoff did not retry after MySQL recovery: ", ctx.Err())
	}
	var count int
	var applicationID, transportID, cause string
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MIN(application_id),MIN(source_transport_id),MIN(cause),MIN(payload)
FROM rm_failure_audit_test WHERE source_topic=? AND source_channel=?`, topic, channel).
		Scan(&count, &applicationID, &transportID, &cause, &payload); err != nil {
		t.Fatal(err)
	}
	if count != 1 || applicationID != "audited-uuid" || transportID == "" || transportID != record.TransportMessageID || cause != "business failed before audit" || string(payload) != "original event" {
		t.Fatalf("durable audit row count=%d app=%q transport=%q cause=%q payload=%q", count, applicationID, transportID, cause, payload)
	}
	client := &http.Client{Timeout: time.Second}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, nsqdHTTP+"/stats?format=json", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var stats struct {
			Topics []struct {
				Name     string `json:"topic_name"`
				Channels []struct {
					Name     string `json:"channel_name"`
					Depth    int64  `json:"depth"`
					InFlight int64  `json:"in_flight_count"`
					Deferred int64  `json:"deferred_count"`
					Requeues int64  `json:"requeue_count"`
				} `json:"channels"`
			} `json:"topics"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&stats)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			t.Fatalf("NSQ stats status=%s decode=%v", response.Status, decodeErr)
		}
		settled := false
		for _, current := range stats.Topics {
			if current.Name != legacy.FailedHandoffTopic(topic, channel) {
				continue
			}
			for _, queue := range current.Channels {
				if queue.Name == legacy.FailedHandoffChannel && queue.Requeues >= 1 && queue.Depth == 0 && queue.InFlight == 0 && queue.Deferred == 0 {
					settled = true
				}
			}
		}
		if settled {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("failure handoff did not settle after durable audit: ", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := subscriber.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}
