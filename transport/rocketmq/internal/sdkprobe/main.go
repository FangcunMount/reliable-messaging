// Disposable acceptance executable. Producers use the PUBLIC SDK exclusively;
// the private pinned client is used only to observe/ack original wire bytes.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	mongostore "github.com/FangcunMount/reliable-messaging/storage/transaction/mongo"
	mysqlstore "github.com/FangcunMount/reliable-messaging/storage/transaction/mysql"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"github.com/FangcunMount/reliable-messaging/transport/rocketmq"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type config struct {
	Endpoint, AccessKey, SecretKey, CA, Topic, Group, Owner, DSN, MongoURI, Storage string
	Branches                                                                        int
}

var output sync.Mutex

func emit(event, id, decision string) {
	output.Lock()
	defer output.Unlock()
	b, _ := json.Marshal(map[string]string{"event": event, "id": id, "decision": decision})
	fmt.Println("TXPROBE " + string(b))
}
func main() {
	if err := run(); err != nil {
		emit("failed", "", "sdk_operation_failed")
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "", "private disposable fixture configuration")
	mode := flag.String("mode", "send", "init, send, crash-committed, crash-prepared, recover, consume")
	id := flag.String("id", "", "synthetic identity")
	flag.Parse()
	raw, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var c config
	if err = json.Unmarshal(raw, &c); err != nil {
		return err
	}
	host, _, splitErr := net.SplitHostPort(c.Endpoint)
	if splitErr != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || (c.Storage != "mysql" && c.Storage != "mongo") || (c.Storage == "mysql" && !strings.Contains(c.DSN, "@tcp(127.0.0.1:")) || (c.Storage == "mongo" && !strings.HasPrefix(c.MongoURI, "mongodb://127.0.0.1:")) {
		return errors.New("owned loopback fixture required")
	}
	ca, err := os.ReadFile(c.CA)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("CA")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if *mode == "consume" {
		return consume(c, tlsConfig)
	}
	ctx := context.Background()
	binding := transaction.Binding{Owner: c.Owner, StoreID: "sdk-disposable", WriterEpoch: "sdk-epoch-1"}
	var store transaction.PreparedStore
	var execute transaction.LocalExecutor
	command := bufio.NewScanner(os.Stdin)
	gate := func() string {
		emit("half", *id, "all_prepared")
		if !command.Scan() {
			return "cancel"
		}
		return command.Text()
	}
	if c.Storage == "mongo" {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(c.MongoURI).SetServerSelectionTimeout(2*time.Second).SetHeartbeatInterval(500*time.Millisecond))
		if err != nil {
			return err
		}
		defer client.Disconnect(ctx)
		db := client.Database("sdkprobe", options.Database().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Majority()).SetWriteConcern(writeconcern.Majority()))
		if *mode == "init" {
			if err = mongostore.InstallSchema(ctx, db); err != nil {
				return err
			}
			_, err = db.Collection("rm_tx_binding").InsertOne(ctx, bson.M{"_id": binding.Owner, "store_id": binding.StoreID, "writer_epoch": binding.WriterEpoch, "schema_version": 1})
			if err != nil {
				return err
			}
			err = db.CreateCollection(ctx, "sdk_business")
			if err == nil {
				emit("installed", "", "mongo")
			}
			return err
		}
		s, err := mongostore.New(db, binding)
		if err != nil {
			return err
		}
		store = s
		execute = func(localCtx context.Context, e transaction.Execution) (transaction.LocalOutcome, error) {
			action := gate()
			if *mode == "crash-prepared" {
				os.Exit(22)
			}
			if action != "commit" {
				return transaction.LocalAborted, nil
			}
			session, err := client.StartSession()
			if err != nil {
				return transaction.LocalAborted, err
			}
			defer session.EndSession(ctx)
			// The driver may retry THIS PURE DATABASE callback; no new messages, IDs,
			// signatures, network sends or model calls are generated within it.
			_, err = session.WithTransaction(localCtx, func(sc mongo.SessionContext) (any, error) {
				guard, err := s.Bind(sc, e)
				if err != nil {
					return nil, err
				}
				if _, err = db.Collection("sdk_business").InsertOne(sc, bson.M{"_id": *id, "value": 1}); err != nil {
					return nil, err
				}
				return nil, guard.CommitWitness(sc)
			}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
			if err != nil {
				return transaction.LocalUnknown, err
			}
			if *mode == "crash-committed" {
				os.Exit(23)
			}
			return transaction.LocalCommitted, nil
		}
	} else {
		db, err := sql.Open("mysql", c.DSN)
		if err != nil {
			return err
		}
		defer db.Close()
		db.SetMaxOpenConns(12)
		if *mode == "init" {
			for _, stmt := range strings.Split(mysqlstore.Schema, ";") {
				if strings.TrimSpace(stmt) != "" {
					if _, err = db.ExecContext(ctx, stmt); err != nil {
						return err
					}
				}
			}
			if _, err = db.ExecContext(ctx, "INSERT INTO rm_tx_binding VALUES(?,?,?,1)", binding.Owner, binding.StoreID, binding.WriterEpoch); err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "CREATE TABLE sdk_business(id VARBINARY(128) PRIMARY KEY,value INT NOT NULL) ENGINE=InnoDB")
			if err == nil {
				emit("installed", "", "mysql")
			}
			return err
		}
		s, err := mysqlstore.New(db, binding)
		if err != nil {
			return err
		}
		store = s
		execute = func(localCtx context.Context, e transaction.Execution) (transaction.LocalOutcome, error) {
			action := gate()
			if *mode == "crash-prepared" {
				os.Exit(22)
			}
			if action != "commit" {
				return transaction.LocalAborted, nil
			}
			tx, err := db.BeginTx(localCtx, nil)
			if err != nil {
				return transaction.LocalAborted, err
			}
			defer tx.Rollback()
			guard, err := s.Bind(localCtx, tx, e)
			if err != nil {
				return transaction.LocalAborted, err
			}
			if _, err = tx.ExecContext(localCtx, "INSERT INTO sdk_business VALUES(?,1)", *id); err != nil {
				return transaction.LocalAborted, err
			}
			if err = guard.CommitWitness(localCtx); err != nil {
				return transaction.LocalAborted, err
			}
			if err = tx.Commit(); err != nil {
				return transaction.LocalUnknown, err
			}
			if *mode == "crash-committed" {
				os.Exit(23)
			}
			return transaction.LocalCommitted, nil
		}
	}
	limits := transaction.DefaultLimits()
	limits.Lease = 15 * time.Second
	limits.ReconcileInterval = 200 * time.Millisecond
	limits.RPCTimeout = 5 * time.Second
	limits.CheckTimeout = 2 * time.Second
	sdkConfig := rocketmq.TransactionConfig{Endpoint: c.Endpoint, AccessKey: c.AccessKey, SecretKey: c.SecretKey, TLS: tlsConfig, Owner: c.Owner, Routes: []rocketmq.TransactionRoute{{LogicalTopic: "sdk.logical", PhysicalTopic: c.Topic, Revision: "sdk-v2"}}, Limits: limits}
	sender, err := rocketmq.NewTransactionalSender(sdkConfig, store)
	if err != nil {
		return err
	}
	if err = sender.Start(ctx); err != nil {
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if closeErr := sender.Close(closeCtx); closeErr != nil {
			return closeErr
		}
		if !sender.Stats().Stopped {
			return errors.New("failed startup still owns resources")
		}
		emit("closed", *id, "failed_start_drained")
		return err
	}
	if *mode == "isolation" {
		other, otherErr := rocketmq.NewTransactionalSender(sdkConfig, store)
		if otherErr != nil {
			return otherErr
		}
		if otherErr = other.Start(ctx); otherErr != nil {
			return otherErr
		}
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		otherErr = sender.Close(closeCtx)
		cancel()
		if otherErr != nil {
			return otherErr
		}
		sender = other
		emit("isolated", *id, "first_closed_second_ready")
	}
	closeSender := func() error {
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := sender.Close(closeCtx); err != nil {
			return err
		}
		emit("closed", *id, "drained")
		return nil
	}
	emit("ready", *id, "public_sdk")
	if *mode == "recover" {
		done := make(chan struct{})
		go func() {
			for command.Scan() {
				if command.Text() == "stop" {
					break
				}
			}
			close(done)
		}()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var last rocketmq.TransactionStats
		for {
			select {
			case <-done:
				return closeSender()
			case <-ticker.C:
				current := sender.Stats()
				if current.CheckCommit > last.CheckCommit {
					emit("checked", "", "commit")
				}
				if current.CheckRollback > last.CheckRollback {
					emit("checked", "", "rollback")
				}
				if current.CheckUnknown > last.CheckUnknown {
					emit("checked", "", "unknown")
				}
				last = current
			}
		}
	}

	count := c.Branches
	if count == 0 {
		count = 1
	}
	if count < 1 || count > 16 {
		return errors.New("branch budget")
	}
	branches := make([]transaction.BranchInput, 0, count)
	for i := 0; i < count; i++ {
		messageID := *id
		if count > 1 {
			messageID = fmt.Sprintf("%s-b%d", *id, i)
		}
		m, err := message.New(message.Input{Producer: "sdkprobe", ID: messageID, Destination: "sdk.logical", EventType: "created", SchemaVersion: "1", Scope: "fixture", ContentType: "application/octet-stream", OccurredAt: "2026-10-19T00:00:00.000000123Z", Payload: []byte("sdk:" + messageID)})
		if err != nil {
			return err
		}
		branches = append(branches, transaction.BranchInput{ID: fmt.Sprintf("branch-%d", i), PhysicalTopic: c.Topic, RouteRevision: "sdk-v2", Message: m, Wire: []byte("sdk:" + messageID)})
	}
	p, err := transaction.NewPlan(transaction.PlanInput{Owner: c.Owner, Scope: "fixture", OperationID: "op-" + *id, IdempotencyKey: *id, InputHash: sha256.Sum256([]byte(*id)), Branches: branches})
	if err != nil {
		return err
	}
	reservation, err := sender.Reserve(ctx, transaction.RequestFor(p), p)
	if err != nil || !reservation.Acquired {
		return errors.New("fixture reservation failed")
	}
	result, err := sender.Execute(ctx, reservation.Execution, execute)
	if err != nil {
		return err
	}
	if len(result.Branches) != count {
		return errors.New("half result")
	}
	want := transaction.FinalCommitConfirmed
	if result.Local == transaction.LocalAborted {
		want = transaction.FinalRollbackConfirmed
	}
	for _, b := range result.Branches {
		if b.Half != transaction.HalfPrepared || b.Final != want || b.BrokerMessageID == "" || b.BrokerTransactionID == "" {
			return errors.New("branch result")
		}
	}
	emit("finalized", *id, string(result.Local))
	return closeSender()
}
func consume(c config, tlsConfig *tls.Config) error {
	factory := func(cfg *driver.Config, opts ...driver.ClientOption) (driver.Client, error) {
		return driver.NewClient(cfg, append(opts, driver.WithQueryRouteTimeout(5*time.Second), driver.WithConnOptions(driver.WithTLSConfig(tlsConfig)))...)
	}
	consumer, err := driver.NewSimpleConsumer(&driver.Config{Endpoint: c.Endpoint, ConsumerGroup: c.Group, Credentials: &credentials.SessionCredentials{AccessKey: c.AccessKey, AccessSecret: c.SecretKey}}, driver.WithClientFuncForSimpleConsumer(factory), driver.WithSimpleAwaitDuration(time.Second), driver.WithSimpleSubscriptionExpressions(map[string]*driver.FilterExpression{c.Topic: driver.SUB_ALL}))
	if err != nil {
		return err
	}
	consumer.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(5 * time.Second)
	if err = consumer.Start(); err != nil {
		return err
	}
	defer consumer.GracefulStop()
	emit("ready", "", "observer")
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
		for _, m := range messages {
			ref, err := transaction.DecodeReference([]byte(m.GetProperties()[rocketmq.TransactionReferenceProperty]))
			if err != nil || ref.PhysicalTopic != c.Topic || ref.Owner != c.Owner || sha256.Sum256(m.GetBody()) != ref.WireHash || string(m.GetBody()) != "sdk:"+ref.MessageID {
				return errors.New("original wire mismatch")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = consumer.Ack(ctx, m)
			cancel()
			if err != nil {
				return err
			}
			emit("received", ref.MessageID, "exact_wire_acknowledged")
		}
	}
}
