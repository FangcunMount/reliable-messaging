package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type rpcRelay struct {
	v2.UnimplementedMessagingServiceServer
	fixture     *injector
	client      v2.MessagingServiceClient
	address     *v2.Endpoints
	mu          sync.Mutex
	connections map[string]net.Conn
	streams     sync.WaitGroup
}
type relayListener struct {
	net.Listener
	relay *rpcRelay
}
type relayConn struct {
	net.Conn
	once    sync.Once
	removed func()
}

func (c *relayConn) Close() error {
	var err error
	c.once.Do(func() { err = c.Conn.Close(); c.removed() })
	return err
}
func (l relayListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	key := conn.RemoteAddr().String()
	wrapped := &relayConn{Conn: conn, removed: func() { l.relay.mu.Lock(); delete(l.relay.connections, key); l.relay.mu.Unlock() }}
	l.relay.mu.Lock()
	l.relay.connections[key] = wrapped
	l.relay.mu.Unlock()
	return wrapped, nil
}
func (f *injector) startRPC(listener net.Listener, certPath, keyPath string) (func() error, error) {
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("fixture CA")
	}
	conn, err := grpc.NewClient(f.upstream, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		return nil, err
	}
	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		conn.Close()
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		conn.Close()
		return nil, err
	}
	relay := &rpcRelay{fixture: f, client: v2.NewMessagingServiceClient(conn), connections: map[string]net.Conn{}, address: &v2.Endpoints{Scheme: v2.AddressScheme_IPv4, Addresses: []*v2.Address{{Host: host, Port: int32(port)}}}}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})), grpc.MaxConcurrentStreams(64), grpc.WaitForHandlers(true))
	v2.RegisterMessagingServiceServer(server, relay)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(relayListener{Listener: listener, relay: relay}) }()
	var once sync.Once
	return func() error {
		once.Do(func() { server.Stop(); conn.Close(); <-done; relay.streams.Wait(); emit("closed", nil) })
		return nil
	}, nil
}
func outgoing(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	copied := md.Copy()
	for _, key := range []string{":authority", "content-type", "user-agent"} {
		delete(copied, key)
	}
	return metadata.NewOutgoingContext(ctx, copied)
}
func (r *rpcRelay) disconnect(ctx context.Context, boundary string, ack bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return
	}
	r.mu.Lock()
	conn := r.connections[p.Addr.String()]
	r.mu.Unlock()
	if conn != nil {
		conn.Close()
		r.fixture.injected(boundary, ack)
	}
}
func (r *rpcRelay) QueryRoute(ctx context.Context, req *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	response, err := r.client.QueryRoute(outgoing(ctx), req)
	if err == nil {
		for _, queue := range response.MessageQueues {
			if queue.Broker != nil {
				queue.Broker.Endpoints = r.address
			}
		}
	}
	return response, err
}
func (r *rpcRelay) Heartbeat(ctx context.Context, req *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	return r.client.Heartbeat(outgoing(ctx), req)
}
func (r *rpcRelay) NotifyClientTermination(ctx context.Context, req *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error) {
	return r.client.NotifyClientTermination(outgoing(ctx), req)
}
func (r *rpcRelay) SendMessage(ctx context.Context, req *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	boundary := r.fixture.target("SendMessage")
	if boundary == "before" {
		r.disconnect(ctx, "before", false)
		return nil, context.Canceled
	}
	response, err := r.client.SendMessage(outgoing(ctx), req)
	if boundary == "hold" && err == nil && response.GetStatus().GetCode() == v2.Code_OK {
		// Withhold a real successful half-message reply until the producer's
		// actual RPC ends. The harness can now SIGKILL the process while its
		// second send is genuinely in flight, before it can persist Unknown
		// or start business. No extra goroutine or unbounded wait is created.
		r.fixture.injected("hold", true)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if boundary == "after" && err == nil && response.GetStatus().GetCode() == v2.Code_OK {
		r.disconnect(ctx, "after", true)
	}
	return response, err
}
func (r *rpcRelay) EndTransaction(ctx context.Context, req *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	boundary := r.fixture.target("EndTransaction")
	if boundary == "before" {
		r.disconnect(ctx, "before", false)
		return nil, context.Canceled
	}
	response, err := r.client.EndTransaction(outgoing(ctx), req)
	if boundary == "after" && err == nil && response.GetStatus().GetCode() == v2.Code_OK {
		r.disconnect(ctx, "after", true)
	}
	return response, err
}
func (r *rpcRelay) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	r.streams.Add(1)
	defer r.streams.Done()
	ctx, cancel := context.WithCancel(outgoing(stream.Context()))
	defer cancel()
	backend, err := r.client.Telemetry(ctx)
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	r.streams.Add(1)
	go func() {
		defer r.streams.Done()
		for {
			command, err := stream.Recv()
			if err != nil {
				sent <- err
				return
			}
			if err = backend.Send(command); err != nil {
				sent <- err
				return
			}
		}
	}()
	for {
		command, err := backend.Recv()
		if err != nil {
			return err
		}
		if err = stream.Send(command); err != nil {
			return err
		}
		select {
		case err = <-sent:
			return err
		default:
		}
	}
}
