// Disposable upstream-client feasibility probe, not the SDK transaction API.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type configuration struct {
	Endpoint, AccessKey, SecretKey, CA, Topic, Group, Owner, DSN string
}

var output sync.Mutex

func emit(event, id, decision string) {
	output.Lock()
	defer output.Unlock()
	b, _ := json.Marshal(map[string]string{"event": event, "id": id, "decision": decision})
	fmt.Println("TXPROBE " + string(b))
}

func digest(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

func resolution(db *sql.DB, owner, topic string, msg *driver.MessageView) driver.TransactionResolution {
	p := msg.GetProperties()
	id := p["probe_id"]
	if p["owner"] != owner || msg.GetTopic() != topic || p["wire_hash"] != digest(msg.GetBody()) {
		emit("checked", id, "unknown_identity")
		return driver.UNKNOWN
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var state string
	err := db.QueryRowContext(ctx, "SELECT state FROM witness WHERE id=? AND owner=? AND wire_hash=?", id, owner, p["wire_hash"]).Scan(&state)
	if err != nil {
		reason := "unknown_database"
		if errors.Is(err, sql.ErrNoRows) {
			reason = "unknown_missing"
		}
		emit("checked", id, reason)
		return driver.UNKNOWN
	}
	switch state {
	case "COMMITTED":
		emit("checked", id, "commit")
		return driver.COMMIT
	case "ABORTED":
		emit("checked", id, "rollback")
		return driver.ROLLBACK
	default:
		emit("checked", id, "unknown_prepared")
		return driver.UNKNOWN
	}
}

func main() {
	if err := run(); err != nil {
		// Driver errors can contain signed request metadata: do not print them.
		emit("failed", "", "operation_failed")
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config", "", "private fixture configuration")
	mode := flag.String("mode", "send", "send, recover or consume")
	id := flag.String("id", "", "fixture identity")
	flag.Parse()
	var c configuration
	b, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &c); err != nil {
		return err
	}
	ca, err := os.ReadFile(c.CA)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid CA")
	}
	conf := &driver.Config{Endpoint: c.Endpoint, ConsumerGroup: c.Group, Credentials: &credentials.SessionCredentials{AccessKey: c.AccessKey, AccessSecret: c.SecretKey}}
	var discardCommitResponse atomic.Bool
	interceptor := func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoke(ctx, method, req, reply, conn, opts...)
		if strings.HasSuffix(method, "/EndTransaction") && err == nil && discardCommitResponse.CompareAndSwap(true, false) {
			return status.Error(codes.Unavailable, "test discarded real EndTransaction response")
		}
		return err
	}
	client := func(config *driver.Config, opts ...driver.ClientOption) (driver.Client, error) {
		return driver.NewClient(config, append(opts, driver.WithQueryRouteTimeout(5*time.Second), driver.WithConnOptions(driver.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}), driver.WithDialOptions(grpc.WithUnaryInterceptor(interceptor))))...)
	}
	if *mode == "consume" {
		consumer, err := driver.NewSimpleConsumer(conf, driver.WithClientFuncForSimpleConsumer(client), driver.WithSimpleAwaitDuration(time.Second), driver.WithSimpleSubscriptionExpressions(map[string]*driver.FilterExpression{c.Topic: driver.SUB_ALL}))
		if err != nil {
			return err
		}
		consumer.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(5 * time.Second)
		if err = consumer.Start(); err != nil {
			return err
		}
		defer consumer.GracefulStop()
		emit("ready", "", "consumer")
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			messages, err := consumer.Receive(ctx, 16, 15*time.Second)
			cancel()
			if err != nil {
				var status *driver.ErrRpcStatus
				if errors.As(err, &status) && status.Code == 40401 {
					continue
				}
				return err
			}
			for _, msg := range messages {
				p := msg.GetProperties()
				if p["wire_hash"] != digest(msg.GetBody()) || string(msg.GetBody()) != "tx:"+p["probe_id"] {
					return errors.New("wire mismatch")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := consumer.Ack(ctx, msg)
				cancel()
				if err != nil {
					return err
				}
				emit("received", p["probe_id"], "acknowledged")
			}
		}
	}
	db, err := sql.Open("mysql", c.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	producer, err := driver.NewProducer(conf, driver.WithClientFunc(client), driver.WithTopics(c.Topic), driver.WithMaxAttempts(1), driver.WithTransactionChecker(&driver.TransactionChecker{Check: func(msg *driver.MessageView) driver.TransactionResolution {
		return resolution(db, c.Owner, c.Topic, msg)
	}}))
	if err != nil {
		return err
	}
	producer.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(5 * time.Second)
	if err = producer.Start(); err != nil {
		return err
	}
	defer producer.GracefulStop()
	emit("ready", "", "producer")
	if *mode == "recover" {
		bufio.NewScanner(os.Stdin).Scan()
		return nil
	}
	if *mode != "send" || *id == "" {
		return errors.New("invalid mode")
	}
	body := []byte("tx:" + *id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = db.ExecContext(ctx, "INSERT INTO witness(id,owner,wire_hash,state) VALUES(?,?,?,'PREPARED')", *id, c.Owner, digest(body))
	cancel()
	if err != nil {
		return err
	}
	tx := producer.BeginTransaction()
	m := &driver.Message{Topic: c.Topic, Body: body}
	m.SetKeys(*id)
	m.AddProperty("probe_id", *id)
	m.AddProperty("owner", c.Owner)
	m.AddProperty("wire_hash", digest(body))
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	receipts, err := producer.SendWithTransaction(ctx, m, tx)
	cancel()
	if err != nil || len(receipts) != 1 || receipts[0].MessageID == "" {
		return errors.New("half not confirmed")
	}
	emit("half", *id, "confirmed")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return errors.New("missing decision")
	}
	action := scanner.Text()
	if action == "crash-prepared" {
		os.Exit(23)
	}
	state := "COMMITTED"
	if action == "rollback" {
		state = "ABORTED"
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	local, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer local.Rollback()
	if state == "COMMITTED" {
		if _, err = local.ExecContext(ctx, "INSERT INTO effects(id) VALUES(?)", *id); err != nil {
			return err
		}
	}
	result, err := local.ExecContext(ctx, "UPDATE witness SET state=? WHERE id=? AND owner=? AND wire_hash=? AND state='PREPARED'", state, *id, c.Owner, digest(body))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("witness CAS failed")
	}
	if err = local.Commit(); err != nil {
		return err
	}
	emit("local", *id, state)
	if action == "crash-committed" {
		os.Exit(23)
	}
	if action == "lost-commit-response" {
		discardCommitResponse.Store(true)
		if tx.Commit() == nil {
			return errors.New("response-loss fault did not fire")
		}
		emit("finalization_unknown", *id, "response_discarded_after_real_rpc")
		os.Exit(24)
	}
	if action == "rollback" {
		err = tx.RollBack()
	} else if action == "commit" {
		err = tx.Commit()
	} else {
		return errors.New("invalid action")
	}
	if err != nil {
		return err
	}
	emit("finalized", *id, state)
	return nil
}
