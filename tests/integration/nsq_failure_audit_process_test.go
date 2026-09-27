//go:build integration

package integration

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	adapter "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	mysqlDriver "github.com/go-sql-driver/mysql"
	driver "github.com/nsqio/go-nsq"
)

const processAuditTable = "rm_failure_process_audit"

// A terminated first process must leave the failed handoff on NSQ until a
// replacement process can persist it. The two business channels deliberately
// differ, as IAM's ephemeral instance channels do in production.
func TestNSQFailureAuditAcrossProcessRestart(t *testing.T) {
	dsn, address, nsqdHTTP := os.Getenv("RM_TEST_MYSQL_DSN"), os.Getenv("RM_TEST_NSQ_TCP"), os.Getenv("RM_TEST_NSQ_HTTP")
	if dsn == "" || address == "" || nsqdHTTP == "" {
		t.Fatal("isolated MySQL and NSQ addresses required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+processAuditTable); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+processAuditTable); err != nil {
			t.Error(err)
		}
	}()

	topic := fmt.Sprintf("rm-process-audit-%d", time.Now().UnixNano())
	const group = "iam-process-audit"
	const firstChannel = "iam-process-one#ephemeral"
	const replacementChannel = "iam-process-two#ephemeral"
	failureTopic := legacy.FailedHandoffTopicForGroup(topic, group)
	first := startNSQAuditProcess(t, ctx, "first", topic, firstChannel)
	defer first.killAndWait()
	first.awaitMarker(t, ctx, "RM_PROCESS_AUDIT_READY")
	client := &http.Client{Timeout: 2 * time.Second}
	waitForNSQChannelClient(t, ctx, client, nsqdHTTP, topic, firstChannel)
	waitForNSQChannelClient(t, ctx, client, nsqdHTTP, failureTopic, legacy.FailedHandoffChannel)

	cfg := processAuditNSQConfig()
	producer, err := driver.NewProducer(address, cfg)
	if err != nil {
		t.Fatal(err)
	}
	producer.SetLogger(nil, driver.LogLevelError)
	defer producer.Stop()
	const applicationID = "process-restart-original-uuid"
	const payload = "original business event"
	body, err := legacy.Encode(legacy.Envelope{UUID: applicationID, Payload: []byte(payload)}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(topic, body); err != nil {
		t.Fatal(err)
	}
	first.awaitMarker(t, ctx, "RM_PROCESS_AUDIT_DB_FAILED")
	if err := first.killAndWait(); err == nil || !wasSIGKILL(err) {
		t.Fatalf("first audit process did not exit by SIGKILL: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+processAuditTable+` (
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

	replacement := startNSQAuditProcess(t, ctx, "replacement", topic, replacementChannel)
	defer replacement.killAndWait()
	replacement.awaitMarker(t, ctx, "RM_PROCESS_AUDIT_READY")
	waitForNSQChannelClient(t, ctx, client, nsqdHTTP, failureTopic, legacy.FailedHandoffChannel)
	replacement.awaitMarker(t, ctx, "RM_PROCESS_AUDIT_PERSISTED")

	var count int
	var gotApplicationID, gotChannel, gotTransportID, gotCause string
	var gotPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MIN(application_id),MIN(source_channel),MIN(source_transport_id),MIN(cause),MIN(payload)
FROM `+processAuditTable+` WHERE source_topic=?`, topic).
		Scan(&count, &gotApplicationID, &gotChannel, &gotTransportID, &gotCause, &gotPayload); err != nil {
		t.Fatal(err)
	}
	if count != 1 || gotApplicationID != applicationID || gotChannel != firstChannel || gotTransportID == "" ||
		gotCause != "business failure before durable audit" || string(gotPayload) != payload {
		t.Fatalf("replacement audit changed identity or payload: count=%d application=%q source_channel=%q transport=%q cause=%q payload=%q",
			count, gotApplicationID, gotChannel, gotTransportID, gotCause, gotPayload)
	}
	waitForNSQCondition(t, ctx, "replacement audit acknowledgment", func() bool {
		return processAuditFailureChannelSettled(client, nsqdHTTP, failureTopic)
	})
}

// TestNSQFailureAuditProcessHelper is run only in child test binaries.
func TestNSQFailureAuditProcessHelper(t *testing.T) {
	role := os.Getenv("RM_PROCESS_AUDIT_ROLE")
	if role == "" {
		return
	}
	if role != "first" && role != "replacement" {
		t.Fatalf("invalid audit process role %q", role)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", os.Getenv("RM_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := processAuditNSQConfig()
	subscriber, err := adapter.NewSubscriber(adapter.SubscriberConfig{
		NSQDAddresses: []string{os.Getenv("RM_TEST_NSQ_TCP")}, Driver: cfg, MaxInFlight: 1,
		MaxAttempts: 1, FailedHandoffGroup: "iam-process-audit",
		Retry: adapter.Backoff{BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := subscriber.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	topic, channel := os.Getenv("RM_PROCESS_AUDIT_TOPIC"), os.Getenv("RM_PROCESS_AUDIT_CHANNEL")
	if err := subscriber.Subscribe(ctx, topic, channel, func(context.Context, transport.Delivery) error {
		if role == "replacement" {
			fmt.Println("RM_PROCESS_AUDIT_UNEXPECTED_BUSINESS")
		}
		return errors.New("business failure before durable audit")
	}, func(ctx context.Context, record legacy.FailedHandoff) error {
		_, err := db.ExecContext(ctx, `INSERT INTO `+processAuditTable+`
  (source_topic,source_channel,application_id,source_transport_id,cause,payload)
VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE source_transport_id=source_transport_id`,
			record.Topic, record.Channel, record.UUID, record.TransportMessageID, record.Cause, record.Payload)
		if err != nil {
			var mysqlErr *mysqlDriver.MySQLError
			if role == "first" && errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
				fmt.Println("RM_PROCESS_AUDIT_DB_FAILED")
			}
			return err
		}
		fmt.Println("RM_PROCESS_AUDIT_PERSISTED")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fmt.Println("RM_PROCESS_AUDIT_READY")
	<-ctx.Done()
}

type nsqAuditProcess struct {
	cmd     *exec.Cmd
	lines   <-chan string
	done    chan struct{}
	exitErr error
}

func startNSQAuditProcess(t *testing.T, ctx context.Context, role, topic, channel string) *nsqAuditProcess {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNSQFailureAuditProcessHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "RM_PROCESS_AUDIT_ROLE="+role, "RM_PROCESS_AUDIT_TOPIC="+topic, "RM_PROCESS_AUDIT_CHANNEL="+channel)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 128)
	process := &nsqAuditProcess{cmd: cmd, lines: lines, done: make(chan struct{})}
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			default:
			}
		}
		process.exitErr = cmd.Wait()
		close(process.done)
	}()
	return process
}

func (p *nsqAuditProcess) awaitMarker(t *testing.T, ctx context.Context, marker string) {
	t.Helper()
	var output []string
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				<-p.done
				t.Fatalf("audit process exited before %s: %v, output=%s", marker, p.exitErr, strings.Join(output, " | "))
			}
			if line == marker {
				return
			}
			output = append(output, line)
		case <-ctx.Done():
			t.Fatalf("audit process did not reach %s: %v, output=%s", marker, ctx.Err(), strings.Join(output, " | "))
		}
	}
}

func (p *nsqAuditProcess) killAndWait() error {
	_ = p.cmd.Process.Kill()
	<-p.done
	return p.exitErr
}

func wasSIGKILL(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signal() == syscall.SIGKILL
}

func processAuditNSQConfig() *driver.Config {
	cfg := driver.NewConfig()
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.MsgTimeout = 3 * time.Second
	cfg.MaxInFlight = 1
	return cfg
}

func processAuditFailureChannelSettled(client *http.Client, endpoint, topic string) bool {
	response, err := client.Get(endpoint + "/stats?format=json")
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	var stats struct {
		Topics []struct {
			Name     string `json:"topic_name"`
			Channels []struct {
				Name     string `json:"channel_name"`
				Depth    int64  `json:"depth"`
				InFlight int64  `json:"in_flight_count"`
				Deferred int64  `json:"deferred_count"`
			} `json:"channels"`
		} `json:"topics"`
	}
	if json.NewDecoder(response.Body).Decode(&stats) != nil {
		return false
	}
	for _, currentTopic := range stats.Topics {
		if currentTopic.Name == topic {
			for _, channel := range currentTopic.Channels {
				if channel.Name == legacy.FailedHandoffChannel {
					return channel.Depth == 0 && channel.InFlight == 0 && channel.Deferred == 0
				}
			}
		}
	}
	return false
}
