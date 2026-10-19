package golang

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func serverProducerSettings(attempts int32) *v2.Settings {
	return &v2.Settings{
		BackoffPolicy: &v2.RetryPolicy{MaxAttempts: attempts, Strategy: &v2.RetryPolicy_ExponentialBackoff{
			ExponentialBackoff: &v2.ExponentialBackoff{Initial: durationpb.New(time.Millisecond),
				Max: durationpb.New(time.Second), Multiplier: 2}}},
		PubSub: &v2.Settings_Publishing{Publishing: &v2.Publishing{MaxBodySize: 262144}},
	}
}

func settingsProducer(t *testing.T) *defaultProducer {
	t.Helper()
	p, err := NewProducer(&Config{Endpoint: "127.0.0.1:8081"}, WithTopics("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	return p.(*defaultProducer)
}

func TestProducerSettingsCommandOwnsRetrySnapshot(t *testing.T) {
	p := settingsProducer(t)
	command := p.cli.getSettingsCommand()
	before, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	response := serverProducerSettings(5)
	if err = p.pSetting.applySettingsCommand(response); err != nil {
		t.Fatal(err)
	}
	after, err := proto.Marshal(command)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("server update mutated an already captured TelemetryCommand", err)
	}
	current := p.cli.getSettingsCommand()
	currentBytes, err := proto.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	response.GetBackoffPolicy().GetExponentialBackoff().Initial.Seconds = 99
	policy := p.pSetting.GetRetryPolicy()
	policy.MaxAttempts = 99
	policy.GetExponentialBackoff().Initial.Seconds = 98
	unchanged, err := proto.Marshal(current)
	if err != nil || !bytes.Equal(currentBytes, unchanged) || p.getRetryMaxAttempts() != 5 {
		t.Fatal("settings retained a mutable response/getter alias", err)
	}
}

func TestProducerSettingsConcurrentUpdateAndMarshal(t *testing.T) {
	p := settingsProducer(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if err := p.pSetting.applySettingsCommand(serverProducerSettings(int32(i%5 + 1))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			command := p.cli.getSettingsCommand()
			size := proto.Size(command)
			data, err := proto.Marshal(command)
			if err != nil || size != len(data) {
				t.Errorf("unstable settings wire: size=%d bytes=%d err=%v", size, len(data), err)
				return
			}
		}
	}()
	wg.Wait()
}

type settingsSnapshotServer struct {
	boundedTransactionServer
	commands atomic.Int32
}

func (s *settingsSnapshotServer) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		n := s.commands.Add(1)
		if err := stream.Send(&v2.TelemetryCommand{Command: &v2.TelemetryCommand_Settings{
			Settings: serverProducerSettings(n%5 + 1)}}); err != nil {
			return err
		}
	}
}

func TestProducerSettingsSnapshotsReachActualTLSStream(t *testing.T) {
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
	service := &settingsSnapshotServer{boundedTransactionServer: boundedTransactionServer{endpoint: endpoint}}
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
	for iteration := 0; iteration < 4; iteration++ {
		inner, err := NewProducer(&Config{Endpoint: listener.Addr().String()},
			WithTopics("first", "second", "third", "fourth"),
			WithClientFunc(func(c *Config, opts ...ClientOption) (Client, error) {
				return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
			}))
		if err != nil {
			t.Fatal(err)
		}
		p := inner.(*defaultProducer)
		p.SetRequestTimeout(2 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = p.StartContext(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			if err = p.cli.telemeter(listener.Addr().String(), p.cli.getSettingsCommand()); err != nil {
				_ = p.GracefulStop()
				t.Fatal(err)
			}
		}
		if err = p.GracefulStop(); err != nil {
			t.Fatal(err)
		}
	}
	if service.commands.Load() < 200 {
		t.Fatalf("actual telemetry exchanges=%d", service.commands.Load())
	}
}
