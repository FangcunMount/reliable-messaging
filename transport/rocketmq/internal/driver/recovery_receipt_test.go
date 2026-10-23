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
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type recoveryReceiptServer struct {
	boundedTransactionServer
	code       v2.Code
	rpcError   bool
	rpcSeen    chan struct{}
	rpcRelease chan struct{}
}

func (s *recoveryReceiptServer) EndTransaction(context.Context, *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	close(s.rpcSeen)
	<-s.rpcRelease
	if s.rpcError {
		return nil, errors.New("controlled response loss")
	}
	return &v2.EndTransactionResponse{Status: &v2.Status{Code: s.code}}, nil
}

func TestRecoveryHookRunsAfterActualTLSRPCAndInsideAdmittedWork(t *testing.T) {
	for _, tc := range []struct {
		name                string
		resolution          TransactionResolution
		code                v2.Code
		rpcError, wantError bool
	}{
		{"commit acknowledged", COMMIT, v2.Code_OK, false, false},
		{"rollback acknowledged", ROLLBACK, v2.Code_OK, false, false},
		{"positive decision but rejected RPC", COMMIT, v2.Code_FORBIDDEN, false, true},
		{"positive decision but lost response", COMMIT, v2.Code_OK, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			service := &recoveryReceiptServer{code: tc.code, rpcError: tc.rpcError, rpcSeen: make(chan struct{}), rpcRelease: make(chan struct{})}
			service.endpoint = endpoint
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
			v2.RegisterMessagingServiceServer(server, service)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			leaf, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AddCert(leaf)
			queue := make(chan func(), 1)
			hookEntered, hookRelease, workDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			checker := &TransactionChecker{CheckContext: func(context.Context, *MessageView) TransactionResolution { return tc.resolution }, Dispatch: func(work func()) bool { queue <- work; return true }, AfterRecovery: func(message *MessageView, resolution TransactionResolution, rpcErr error) {
				if message.GetMessageId() != "message" || resolution != tc.resolution || (rpcErr != nil) != tc.wantError {
					t.Error("actual result lost at recovery hook")
				}
				close(hookEntered)
				<-hookRelease
			}}
			inner, err := NewProducer(&Config{Endpoint: listener.Addr().String()}, WithTopics("topic"), WithTransactionChecker(checker), WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
				return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
			}))
			if err != nil {
				t.Fatal(err)
			}
			producer := inner.(*defaultProducer)
			manager := producer.cli.newClientManager()
			producer.cli.clientManager = manager
			t.Cleanup(manager.shutdown)
			producer.pSetting.requestTimeout = time.Second
			if _, err = manager.QueryRoute(context.Background(), endpoint, &v2.QueryRouteRequest{}, time.Second); err != nil {
				t.Fatal(err)
			}
			body := []byte("wire")
			command := &v2.RecoverOrphanedTransactionCommand{TransactionId: "transaction", Message: &v2.Message{Topic: &v2.Resource{Name: "topic"}, Body: body, SystemProperties: &v2.SystemProperties{MessageId: "message", BodyDigest: &v2.Digest{Type: v2.DigestType_CRC32, Checksum: fmt.Sprintf("%X", crc32.ChecksumIEEE(body))}, BodyEncoding: v2.Encoding_IDENTITY}}}
			if err = producer.onRecoverOrphanedTransactionCommand(endpoint, command); err != nil {
				t.Fatal(err)
			}
			work := <-queue
			go func() { defer close(workDone); work() }()
			select {
			case <-service.rpcSeen:
			case <-time.After(2 * time.Second):
				t.Fatal("actual RPC did not arrive")
			}
			select {
			case <-hookEntered:
				t.Fatal("decision alone invoked confirmation hook")
			default:
			}
			close(service.rpcRelease)
			select {
			case <-hookEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("actual RPC result not observed")
			}
			select {
			case <-workDone:
				t.Fatal("worker released before confirmation audit")
			default:
			}
			close(hookRelease)
			select {
			case <-workDone:
			case <-time.After(2 * time.Second):
				t.Fatal("admitted worker did not drain")
			}
		})
	}
}
