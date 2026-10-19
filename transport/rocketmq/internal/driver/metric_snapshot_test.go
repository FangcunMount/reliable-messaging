package golang

import (
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
)

// A broker may resend Metric{On:false} while receive/ACK hooks are running.
// The meter's atomic enabled field does not protect replacement of its pointer.
func TestMeterResetConcurrentWithDeliveryHooks(t *testing.T) {
	client, err := NewClient(&Config{Endpoint: "127.0.0.1:8081"})
	if err != nil {
		t.Fatal(err)
	}
	provider := client.(*defaultClient).clientMeterProvider.(*defaultClientMeterProvider)
	interceptor := NewDefaultMessageMeterInterceptor(provider)
	start := make(chan struct{})
	var work sync.WaitGroup
	for i := 0; i < 4; i++ {
		work.Add(1)
		go func() {
			defer work.Done()
			<-start
			for j := 0; j < 10000; j++ {
				if err := interceptor.doBefore(MessageHookPoints_ACK, nil); err != nil {
					t.Error(err)
					return
				}
				if err := interceptor.doAfter(MessageHookPoints_ACK, nil, time.Millisecond, MessageHookPointsStatus_OK); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	work.Add(1)
	go func() {
		defer work.Done()
		<-start
		for i := 0; i < 1000; i++ {
			provider.Reset(&v2.Metric{On: false})
		}
	}()
	close(start)
	work.Wait()
	if provider.isEnabled() {
		t.Fatal("disabled broker metrics unexpectedly enabled")
	}
}

type meterSnapshotServer struct {
	boundedTransactionServer
	settings         atomic.Int32
	acknowledgements atomic.Int32
}

func (s *meterSnapshotServer) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	settings := proto.Clone(first.GetSettings()).(*v2.Settings)
	settings.Metric = &v2.Metric{On: false}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if err = stream.Send(&v2.TelemetryCommand{Command: &v2.TelemetryCommand_Settings{Settings: settings}}); err != nil {
			return err
		}
		s.settings.Add(1)
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}
func (s *meterSnapshotServer) AckMessage(context.Context, *v2.AckMessageRequest) (*v2.AckMessageResponse, error) {
	s.acknowledgements.Add(1)
	return &v2.AckMessageResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}

// Exercise the real consumer ACK hooks concurrently with repeated metric-off
// settings received on its real TLS telemetry stream, without a live Broker.
func TestMeterResetActualTLSConsumerACK(t *testing.T) {
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
	service := &meterSnapshotServer{boundedTransactionServer: boundedTransactionServer{endpoint: endpoint}}
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
	consumer, err := NewSimpleConsumer(&Config{Endpoint: listener.Addr().String(), ConsumerGroup: "fixture"},
		WithSimpleSubscriptionExpressions(map[string]*FilterExpression{"fixture": SUB_ALL}),
		WithClientFuncForSimpleConsumer(func(c *Config, opts ...ClientOption) (Client, error) {
			return NewClient(c, append(opts, WithConnOptions(WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if consumer.GracefulStop() != nil {
			t.Error("real consumer did not drain")
		}
	})
	if err = consumer.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var work sync.WaitGroup
	for i := 0; i < 4; i++ {
		work.Add(1)
		go func() {
			defer work.Done()
			for j := 0; j < 100; j++ {
				if err := consumer.Ack(ctx, &MessageView{messageId: "fixture", topic: "fixture", ReceiptHandle: "fixture", endpoints: endpoint}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	work.Wait()
	if service.acknowledgements.Load() != 400 || service.settings.Load() < 2 {
		t.Fatal("actual ACK/telemetry exchanges absent")
	}
}
