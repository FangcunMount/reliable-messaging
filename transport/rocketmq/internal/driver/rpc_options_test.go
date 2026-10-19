package golang

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type routeServer struct {
	v2.UnimplementedMessagingServiceServer
	calls atomic.Int32
}

func (s *routeServer) QueryRoute(context.Context, *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	s.calls.Add(1)
	return &v2.QueryRouteResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}

func (s *routeServer) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	for {
		command, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(command); err != nil {
			return err
		}
	}
}

func TestClientRPCUsesConfiguredTLSAndInterceptor(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	cert := certificate.TLS.Certificates[0]
	certificate.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	service := &routeServer{}
	v2.RegisterMessagingServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	endpoint, err := utils.ParseTarget(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		roots      *x509.CertPool
		serverName string
		wantError  bool
	}{
		{"trusted", roots, "", false},
		{"untrusted CA", x509.NewCertPool(), "", true},
		{"wrong server name", roots, "wrong.invalid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var intercepted atomic.Int32
			client, err := NewClient(&Config{Endpoint: listener.Addr().String()}, WithConnOptions(
				WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: tc.roots, ServerName: tc.serverName}),
				WithDialTimeout(150*time.Millisecond),
				WithDialOptions(grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					intercepted.Add(1)
					return invoke(ctx, method, req, reply, connection, opts...)
				}))))
			if err != nil {
				t.Fatal(err)
			}
			manager := client.(*defaultClient).newClientManager()
			defer manager.cleanRpcClient()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = manager.QueryRoute(ctx, endpoint, &v2.QueryRouteRequest{}, time.Second)
			if (err != nil) != tc.wantError {
				t.Fatalf("RPC error = %v, want error %v", err, tc.wantError)
			}
			if !tc.wantError && intercepted.Load() != 1 {
				t.Fatal("client interceptor did not reach real RPC")
			}
			if !tc.wantError {
				stream, err := manager.Telemetry(ctx, endpoint, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if err = stream.Send(&v2.TelemetryCommand{}); err != nil {
					t.Fatal(err)
				}
				if _, err = stream.Recv(); err != nil {
					t.Fatalf("stream canceled before RPC reader completed: %v", err)
				}
				if err = stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-stream.Context().Done():
				case <-time.After(time.Second):
					t.Fatal("stream did not release its deadline on close")
				}
			}
		})
	}
	if service.calls.Load() != 1 {
		t.Fatalf("untrusted TLS reached server; calls=%d", service.calls.Load())
	}
}
