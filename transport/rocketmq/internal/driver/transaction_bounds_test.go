package golang

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type boundedTransactionServer struct {
	routeServer
	completed    chan struct{}
	transactions atomic.Int32
	endpoint     *v2.Endpoints
}

func (s *boundedTransactionServer) QueryRoute(_ context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	s.calls.Add(1)
	return &v2.QueryRouteResponse{Status: &v2.Status{Code: v2.Code_OK}, MessageQueues: []*v2.MessageQueue{{Topic: request.Topic, Id: 0, Permission: v2.Permission_READ_WRITE, Broker: &v2.Broker{Name: "fixture", Id: 0, Endpoints: s.endpoint}}}}, nil
}
func (s *boundedTransactionServer) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func (s *boundedTransactionServer) EndTransaction(ctx context.Context, _ *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	s.transactions.Add(1)
	<-ctx.Done()
	s.completed <- struct{}{}
	return nil, ctx.Err()
}

func TestTransactionContextReachesActualTLSCommitAndRollback(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	cert := certificate.TLS.Certificates[0]
	certificate.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := utils.ParseTarget(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	service := &boundedTransactionServer{completed: make(chan struct{}, 2), endpoint: endpoint}
	v2.RegisterMessagingServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	inner, err := NewProducer(&Config{Endpoint: listener.Addr().String()}, WithTopics("topic"), WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
		return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	p := inner.(*defaultProducer)
	manager := p.cli.newClientManager()
	p.cli.clientManager = manager
	defer manager.shutdown()
	p.pSetting.requestTimeout = 2 * time.Second
	if _, err = manager.QueryRoute(context.Background(), endpoint, &v2.QueryRouteRequest{}, time.Second); err != nil {
		t.Fatal(err)
	}
	for _, rollback := range []bool{false, true} {
		tx := NewTransactionImpl(p)
		tx.messageSendReceiptMap.Store("message", []interface{}{&PublishingMessage{msg: &Message{Topic: "topic", Body: []byte("wire")}}, &SendReceipt{Endpoints: endpoint, MessageID: "message", TransactionId: "transaction"}})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		start := time.Now()
		if rollback {
			err = tx.RollBackContext(ctx)
		} else {
			err = tx.CommitContext(ctx)
		}
		cancel()
		if err == nil || time.Since(start) > 300*time.Millisecond {
			t.Fatal("actual RPC ignored caller budget", err)
		}
		select {
		case <-service.completed:
		case <-time.After(time.Second):
			t.Fatal("real server RPC still active after cancellation")
		}
	}
	if service.transactions.Load() != 2 {
		t.Fatal("both real RPCs were not exercised")
	}
	// A trusted route connection that never provides initial settings must also
	// stop at the caller's startup budget and join its real scheduled workers.
	startup, err := NewProducer(&Config{Endpoint: listener.Addr().String()}, WithTopics("topic"), WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
		return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = startup.(ContextProducer).StartContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 300*time.Millisecond {
		t.Fatal("startup ignored caller budget", err)
	}
	if _, err = startup.(*defaultProducer).cli.clientManager.(*defaultClientManager).getRpcClient(endpoint); err == nil {
		t.Fatal("failed startup retained live manager")
	}
}

func TestRecoveryAdmissionPrecedesGoroutineAndMalformedCommandFailsClosed(t *testing.T) {
	var called atomic.Int32
	queue := make(chan func(), 1)
	checker := &TransactionChecker{CheckContext: func(context.Context, *MessageView) TransactionResolution { called.Add(1); return UNKNOWN }, Dispatch: func(work func()) bool {
		select {
		case queue <- work:
			return true
		default:
			return false
		}
	}}
	inner, err := NewProducer(&Config{Endpoint: "127.0.0.1:1"}, WithTopics("topic"), WithTransactionChecker(checker))
	if err != nil {
		t.Fatal(err)
	}
	p := inner.(*defaultProducer)
	body := []byte("wire")
	digest := crc32.ChecksumIEEE(body)
	command := &v2.RecoverOrphanedTransactionCommand{TransactionId: "transaction", Message: &v2.Message{Topic: &v2.Resource{Name: "topic"}, Body: body, SystemProperties: &v2.SystemProperties{MessageId: "message", BodyDigest: &v2.Digest{Type: v2.DigestType_CRC32, Checksum: fmt.Sprintf("%X", digest)}, BodyEncoding: v2.Encoding_IDENTITY}}}
	for i := 0; i < 1000; i++ {
		if err = p.onRecoverOrphanedTransactionCommand(nil, command); err != nil {
			t.Fatal(err)
		}
	}
	if called.Load() != 0 || len(queue) != 1 {
		t.Fatal("recovery bypassed bounded pre-dispatch admission")
	}
	if err = p.onRecoverOrphanedTransactionCommand(nil, nil); err == nil {
		t.Fatal("malformed recovery accepted")
	}
}

func TestManagerShutdownInterruptsInitialSleepsAndJoinsWorkers(t *testing.T) {
	manager := NewDefaultClientManager()
	manager.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY = time.Hour
	manager.opts.HEART_BEAT_INITIAL_DELAY = time.Hour
	manager.opts.LOG_STATS_INITIAL_DELAY = time.Hour
	manager.opts.SYNC_SETTINGS_DELAY = time.Hour
	manager.startUp()
	done := make(chan struct{})
	go func() { manager.shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not join its real scheduled workers")
	}
	endpoint, err := utils.ParseTarget("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.getRpcClient(endpoint); err == nil {
		t.Fatal("closed manager created a new RPC connection")
	}
}

func TestTransactionContextAlsoBoundsColdTLSConnection(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "https://")
	inner, err := NewProducer(&Config{Endpoint: address}, WithTopics("topic"), WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
		return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12})))...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	p := inner.(*defaultProducer)
	manager := p.cli.newClientManager()
	defer manager.shutdown()
	p.cli.clientManager = manager
	p.pSetting.requestTimeout = 2 * time.Second
	endpoint, err := utils.ParseTarget(address)
	if err != nil {
		t.Fatal(err)
	}
	tx := NewTransactionImpl(p)
	tx.messageSendReceiptMap.Store("message", []interface{}{&PublishingMessage{msg: &Message{Topic: "topic", Body: []byte("wire")}}, &SendReceipt{Endpoints: endpoint, MessageID: "message", TransactionId: "transaction"}})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err = tx.CommitContext(ctx); err == nil || time.Since(start) > 300*time.Millisecond {
		t.Fatal("cold dial ignored transaction caller context", err)
	}
}

// Server-side retry settings must never authorize another half-message send.
type oneAttemptTransactionServer struct {
	boundedTransactionServer
	sends atomic.Int32
}

func (s *oneAttemptTransactionServer) SendMessage(context.Context, *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	s.sends.Add(1)
	return nil, status.Error(codes.Unavailable, "synthetic lost half reply")
}
func TestTransactionSendDoesNotRetryAfterServerRetryOverride(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	cert := certificate.TLS.Certificates[0]
	certificate.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := utils.ParseTarget(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	service := &oneAttemptTransactionServer{boundedTransactionServer: boundedTransactionServer{endpoint: endpoint}}
	v2.RegisterMessagingServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	inner, err := NewProducer(&Config{Endpoint: listener.Addr().String()}, WithTopics("topic"), WithMaxAttempts(1), WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
		return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	p := inner.(*defaultProducer)
	manager := p.cli.newClientManager()
	p.cli.clientManager = manager
	defer manager.shutdown()
	p.cli.on.Store(true)
	if err = p.pSetting.applySettingsCommand(&v2.Settings{BackoffPolicy: &v2.RetryPolicy{MaxAttempts: 5}, PubSub: &v2.Settings_Publishing{Publishing: &v2.Publishing{MaxBodySize: 4 * 1024 * 1024, ValidateMessageType: false}}}); err != nil {
		t.Fatal(err)
	}
	p.pSetting.requestTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = p.SendWithTransaction(ctx, &Message{Topic: "topic", Body: []byte("synthetic")}, p.BeginTransaction())
	if err == nil || service.sends.Load() != 1 {
		t.Fatalf("actual half RPC attempts=%d error=%v", service.sends.Load(), err)
	}
}

func TestWaitingForAbsentObserverIsInterruptibleOnShutdown(t *testing.T) {
	cli := &defaultClient{done: make(chan struct{})}
	session := &defaultClientSession{cli: cli}
	returned := make(chan struct{})
	go func() { session._acquire_observer(); close(returned) }()
	// Closing before or during the wait must interrupt it; neither schedule is
	// allowed to retain the historical uninterruptible one-second sleep.
	close(cli.done)
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("absent observer prevented actual session drain")
	}
}
