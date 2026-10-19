// Bounded, disposable LOOPBACK-ONLY packet fault fixture. It never logs wire,
// SQL, BSON, credentials or message bodies. Not a production component.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type control struct {
	Action, Boundary, Method string
	Nth                      int
}
type injector struct {
	mu                               sync.Mutex
	kind, upstream, boundary, method string
	nth, seen, dropped, acks         int
	sessions                         map[string]bson.Raw
}

var printMu sync.Mutex

func emit(event string, fields map[string]any) {
	printMu.Lock()
	defer printMu.Unlock()
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = event
	raw, _ := json.Marshal(fields)
	fmt.Println("FAULTPROXY " + string(raw))
}
func loopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	return err == nil && ip != nil && ip.IsLoopback()
}
func main() {
	if err := run(); err != nil {
		emit("failed", nil)
		os.Exit(1)
	}
}
func run() error {
	kind := flag.String("kind", "", "mysql, mongo or rocketmq")
	upstream := flag.String("upstream", "", "owned loopback endpoint")
	listen := flag.String("listen", "127.0.0.1:0", "loopback listener")
	cert := flag.String("cert", "", "disposable TLS certificate")
	key := flag.String("key", "", "disposable TLS key")
	flag.Parse()
	if !loopback(*upstream) || !loopback(*listen) {
		return errors.New("loopback fixture required")
	}
	if *kind != "mysql" && *kind != "mongo" && *kind != "rocketmq" {
		return errors.New("unknown fixture protocol")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fixture := &injector{kind: *kind, upstream: *upstream, sessions: map[string]bson.Raw{}}
	var closeFixture func() error
	if *kind == "rocketmq" {
		closeFixture, err = fixture.startRPC(listener, *cert, *key)
	} else {
		closeFixture = fixture.startPackets(listener)
	}
	if err != nil {
		listener.Close()
		return err
	}
	defer closeFixture()
	emit("ready", map[string]any{"address": listener.Addr().String(), "kind": *kind})
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var c control
		if json.Unmarshal(scanner.Bytes(), &c) != nil {
			return errors.New("invalid fixture control")
		}
		switch c.Action {
		case "arm":
			if c.Boundary != "before" && c.Boundary != "after" {
				return errors.New("invalid boundary")
			}
			if c.Nth < 1 || c.Nth > 16 {
				return errors.New("invalid bounded ordinal")
			}
			fixture.mu.Lock()
			fixture.boundary, fixture.method, fixture.nth = c.Boundary, c.Method, c.Nth
			fixture.seen, fixture.dropped, fixture.acks = 0, 0, 0
			fixture.sessions = map[string]bson.Raw{}
			fixture.mu.Unlock()
			emit("armed", nil)
		case "abort-blocked":
			if err = fixture.abortMongo(); err != nil {
				return err
			}
			emit("sessions_aborted", nil)
		case "stats":
			fixture.mu.Lock()
			emit("stats", map[string]any{"seen": fixture.seen, "dropped": fixture.dropped, "backend_commit_acks": fixture.acks})
			fixture.mu.Unlock()
		case "stop":
			return closeFixture()
		default:
			return errors.New("unknown fixture action")
		}
	}
	return closeFixture()
}
func (f *injector) target(method string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.boundary == "" || (f.method != "" && f.method != method) {
		return ""
	}
	f.seen++
	if f.seen != f.nth {
		return ""
	}
	return f.boundary
}
func (f *injector) injected(boundary string, ack bool) {
	f.mu.Lock()
	f.dropped++
	if ack {
		f.acks++
	}
	count := f.dropped
	f.mu.Unlock()
	emit("injected", map[string]any{"kind": f.kind, "boundary": boundary, "backend_ack": ack, "dropped": count})
}
func (f *injector) abortMongo() error {
	if f.kind != "mongo" {
		return errors.New("not Mongo fixture")
	}
	f.mu.Lock()
	sessions := make(bson.A, 0, len(f.sessions))
	for _, raw := range f.sessions {
		sessions = append(sessions, raw)
	}
	f.mu.Unlock()
	if len(sessions) == 0 {
		return errors.New("no exact blocked sessions")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+f.upstream+"/?replicaSet=sdk-proof&directConnection=true").SetServerSelectionTimeout(2*time.Second).SetMaxPoolSize(2))
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	return client.Database("admin").RunCommand(ctx, bson.D{{Key: "killSessions", Value: sessions}}).Err()
}
func sessionKey(raw bson.Raw, number int64) string {
	data, _ := bson.Marshal(bson.D{{Key: "lsid", Value: raw}, {Key: "txnNumber", Value: number}})
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
