package rocketmq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type producer interface {
	Send(context.Context, *driver.Message) ([]*driver.SendReceipt, error)
	GracefulStop() error
}

type Publisher struct {
	closeDone         chan struct{}
	config            Config
	routes            map[string]string
	mu                sync.Mutex
	inner             producer
	started, stopping bool
	active            int
	slots             chan struct{}
	drained           chan struct{}
	stop              chan struct{}
	closeOnce         sync.Once
	closeErr          error
}

func NewPublisher(config Config) (*Publisher, error) {
	c, err := validate(config)
	if err != nil {
		return nil, err
	}
	routes := map[string]string{}
	for _, r := range c.Routes {
		routes[r.Topic] = r.PhysicalTopic
	}
	return &Publisher{config: c, routes: routes, slots: make(chan struct{}, c.MaxInFlight), drained: make(chan struct{}), stop: make(chan struct{}), closeDone: make(chan struct{})}, nil
}

// Start is the explicit network/lifecycle boundary. Driver startup and shutdown
// use its configured bounded request timeout; hosts supervise startup failures.
func (p *Publisher) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		return errors.New("RocketMQ publisher stopping")
	}
	if p.started {
		return nil
	}
	topics := make([]string, 0, len(p.routes))
	for _, topic := range p.routes {
		topics = append(topics, topic)
	}
	inner, err := driver.NewProducer(&driver.Config{Endpoint: p.config.Endpoint, Credentials: &credentials.SessionCredentials{AccessKey: p.config.AccessKey, AccessSecret: p.config.SecretKey}}, driver.WithClientFunc(func(c *driver.Config, opts ...driver.ClientOption) (driver.Client, error) {
		return driver.NewClient(c, append(opts, driver.WithQueryRouteTimeout(p.config.RequestTimeout), driver.WithConnOptions(driver.WithTLSConfig(p.config.TLS)))...)
	}), driver.WithTopics(topics...), driver.WithMaxAttempts(1))
	if err != nil {
		return err
	}
	inner.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(p.config.RequestTimeout)
	if err = inner.Start(); err != nil {
		return errors.Join(err, inner.GracefulStop())
	}
	p.inner = inner
	p.started = true
	return nil
}

func (p *Publisher) Publish(ctx context.Context, m message.Message) transport.Result {
	if !m.Valid() {
		return transport.Result{Outcome: transport.Rejected}
	}
	in := m.Input()
	return p.publish(ctx, in.Destination, in.ID, in.Payload)
}

// PublishRaw accepts a logical topic and preserves already encoded bytes.
func (p *Publisher) PublishRaw(ctx context.Context, topic string, body []byte) transport.Result {
	return p.publish(ctx, topic, "", body)
}

func (p *Publisher) publish(ctx context.Context, logical, id string, body []byte) transport.Result {
	topic, ok := p.routes[logical]
	if !ok || len(body) == 0 || len(body) > 262144 {
		return transport.Result{Outcome: transport.Rejected}
	}
	if ctx.Err() != nil {
		return transport.Result{Outcome: transport.Unknown}
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	case <-p.stop:
		return transport.Result{Outcome: transport.Unknown}
	}
	p.mu.Lock()
	if !p.started || p.stopping || ctx.Err() != nil {
		p.mu.Unlock()
		<-p.slots
		return transport.Result{Outcome: transport.Unknown}
	}
	inner := p.inner
	p.active++
	p.mu.Unlock()
	m := &driver.Message{Topic: topic, Body: append([]byte(nil), body...)}
	if id != "" {
		m.SetKeys(id)
	}
	done := make(chan transport.Result, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
		defer cancel()
		receipts, err := inner.Send(callCtx, m)
		outcome := transport.Unknown
		var status *driver.ErrRpcStatus
		if errors.As(err, &status) && (status.Code >= 40000 && status.Code < 40600 || status.Code >= 41300 && status.Code < 41400) {
			outcome = transport.Rejected
		}
		if err == nil && len(receipts) == 1 && receipts[0] != nil && receipts[0].MessageID != "" {
			outcome = transport.Confirmed
		}
		done <- transport.Result{Outcome: outcome}
		<-p.slots
		p.mu.Lock()
		p.active--
		if p.stopping && p.active == 0 {
			close(p.drained)
		}
		p.mu.Unlock()
	}()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
}

func (p *Publisher) Drain(ctx context.Context) error {
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
		close(p.stop)
		if p.active == 0 {
			close(p.drained)
		}
	}
	p.mu.Unlock()
	select {
	case <-p.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) Close(ctx context.Context) error {
	if err := p.Drain(ctx); err != nil {
		return err
	}
	p.startClose()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeDone:
		return p.closeErr
	}
}

// Interrupt explicitly ends admission and asks the owned client to stop while
// sends may still be active. Their results remain Unknown; Close must verify
// both real sends and driver shutdown before a host releases resources.
func (p *Publisher) Interrupt() {
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
		close(p.stop)
		if p.active == 0 {
			close(p.drained)
		}
	}
	p.mu.Unlock()
	p.startClose()
}
func (p *Publisher) startClose() {
	p.closeOnce.Do(func() {
		go func() {
			p.mu.Lock()
			inner := p.inner
			p.mu.Unlock()
			if inner != nil {
				p.closeErr = inner.GracefulStop()
			}
			close(p.closeDone)
		}()
	})
}
