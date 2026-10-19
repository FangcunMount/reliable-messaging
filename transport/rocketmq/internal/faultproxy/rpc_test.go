package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type acknowledgedServer struct {
	v2.UnimplementedMessagingServiceServer
	sent atomic.Int32
}

func (s *acknowledgedServer) SendMessage(context.Context, *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	s.sent.Add(1)
	return &v2.SendMessageResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}
func TestRPCFaultActuallyClosesTLSConnectionAfterBackendAck(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	cert := certificate.TLS.Certificates[0]
	certificate.Close()
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	service := &acknowledgedServer{}
	v2.RegisterMessagingServiceServer(server, service)
	go server.Serve(backend)
	defer server.Stop()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &injector{kind: "rocketmq", upstream: backend.Addr().String(), boundary: "after", method: "SendMessage", nth: 1}
	closeProxy, err := fixture.startRPC(listener, certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeProxy()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = v2.NewMessagingServiceClient(conn).SendMessage(ctx, &v2.SendMessageRequest{})
	if err == nil || service.sent.Load() != 1 {
		t.Fatal("successful backend reply reached client despite TCP fault", err)
	}
	// The client can observe socket closure BEFORE the server handler has
	// recorded its post-close audit. Join the real handler before inspecting
	// counters; stopping now cannot cause the already-observed RPC error.
	if err = closeProxy(); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.dropped != 1 || fixture.acks != 1 {
		t.Fatal("network injection lacks positive backend acknowledgment")
	}
}
