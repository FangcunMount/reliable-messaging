// Disposable acceptance executable. Producers and consumers use PUBLIC SDK
// APIs; no production endpoints, schemas or business data are accepted.
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
	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/transport/rocketmq"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type config struct {
	Revision                                                                                      string                      `json:"route_revision"`
	OwnerRoutes                                                                                   []rocketmq.TransactionRoute `json:"owner_routes"`
	Endpoint, AccessKey, SecretKey, CA, Topic, Group, Owner, DSN, MongoURI, Storage, Subscription string
	Inputs                                                                                        []transport.RouteBinding
	Branches                                                                                      int
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
		var rpcStatus *driver.ErrRpcStatus
		if errors.As(err, &rpcStatus) && (rpcStatus.Code == 40100 || rpcStatus.Code == 40300) {
			emit("broker_denied", "", fmt.Sprint(rpcStatus.Code))
		}
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
	if c.Revision == "" {
		c.Revision = "sdk-v2"
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
	if *mode == "consume" || *mode == "shared-consume" {
		return consume(c, tlsConfig, *mode == "shared-consume")
	}
	if *mode == "normal" {
		return normal(c, tlsConfig, *id)
	}
	ctx := context.Background()
	binding := transaction.Binding{Owner: c.Owner, StoreID: "sdk-disposable", WriterEpoch: "sdk-epoch-1"}
	var store transaction.PreparedStore
	var execute transaction.LocalExecutor
	command := bufio.NewScanner(os.Stdin)
	localCalls := 0
	gate := func() string {
		localCalls++
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
	routes := c.OwnerRoutes
	if len(routes) == 0 {
		routes = []rocketmq.TransactionRoute{{LogicalTopic: "sdk.logical", PhysicalTopic: c.Topic, Revision: c.Revision}}
	}
	sdkConfig := rocketmq.TransactionConfig{Endpoint: c.Endpoint, AccessKey: c.AccessKey, SecretKey: c.SecretKey, TLS: tlsConfig, Owner: c.Owner, Routes: routes, Limits: limits}
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
		branches = append(branches, transaction.BranchInput{ID: fmt.Sprintf("branch-%d", i), PhysicalTopic: c.Topic, RouteRevision: c.Revision, Message: m, Wire: fixtureWire(messageID)})
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
	if strings.HasPrefix(*mode, "fault-") {
		emitResult(*id, result, localCalls)
		valid := false
		switch *mode {
		case "fault-half":
			unknown := false
			for _, branch := range result.Branches {
				unknown = unknown || branch.Half == transaction.HalfUnknown
			}
			valid = result.Local == transaction.LocalNotStarted && localCalls == 0 && unknown
		case "fault-final":
			unknown := false
			for _, branch := range result.Branches {
				unknown = unknown || branch.Final == transaction.FinalUnknown
			}
			valid = result.Local == transaction.LocalCommitted && localCalls == 1 && unknown
		case "fault-local-committed":
			valid = result.Local == transaction.LocalCommitted && localCalls == 1
		case "fault-local-unknown":
			valid = result.Local == transaction.LocalUnknown && localCalls == 1
			for _, branch := range result.Branches {
				valid = valid && branch.Final == transaction.FinalNotAttempted
			}
		}
		if !valid {
			return errors.Join(errors.New("expected real fault outcome not established"), closeSender())
		}
		if closeErr := closeSender(); closeErr != nil {
			return closeErr
		}
		emit("fault_observed", *id, *mode)
		return nil
	}
	if err != nil {
		return errors.Join(err, closeSender())
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

func emitResult(id string, result transaction.Result, calls int) {
	branches := make([]map[string]string, 0, len(result.Branches))
	for _, b := range result.Branches {
		branches = append(branches, map[string]string{"message_id": b.Reference.MessageID, "half": string(b.Half), "final": string(b.Final), "error_code": b.ErrorCode, "broker_message_id": b.BrokerMessageID, "broker_transaction_id": b.BrokerTransactionID})
	}
	raw, _ := json.Marshal(map[string]any{"local": result.Local, "local_calls": calls, "branches": branches})
	emit("result", id, string(raw))
}
