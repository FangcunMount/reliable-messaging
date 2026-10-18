package rocketmq

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type SubscriberConfig struct {
	Config
	MaxAttempts                                   uint16
	InvisibleDuration, HandlerTimeout, RetryDelay time.Duration
	DeliveryContext                               context.Context
}

type consumer interface {
	Receive(context.Context, int32, time.Duration) ([]*driver.MessageView, error)
	Ack(context.Context, *driver.MessageView) error
	ChangeInvisibleDuration(*driver.MessageView, time.Duration) error
	GracefulStop() error
}

type Subscriber struct {
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
	drained      chan struct{}
	drainOnce    sync.Once
	health       map[string]*atomic.Int64
	config       SubscriberConfig
	mu           sync.Mutex
	stopping     bool
	running      map[string]consumer
	ctx          context.Context
	cancel       context.CancelFunc
	pollers      sync.WaitGroup
	handlers     sync.WaitGroup
	slots        chan struct{}
	errors       chan error
}

func NewSubscriber(config SubscriberConfig) (*Subscriber, error) {
	c, err := validate(config.Config)
	if err != nil {
		return nil, err
	}
	config.Config = c
	if config.MaxAttempts == 0 || config.InvisibleDuration < 10*time.Second || config.InvisibleDuration > 12*time.Hour || config.HandlerTimeout <= 0 || config.RetryDelay <= 0 || config.RetryDelay > 12*time.Hour {
		return nil, errors.New("bounded attempts, visibility, handler timeout and retry delay required")
	}
	if config.DeliveryContext == nil {
		config.DeliveryContext = context.Background()
	}
	// No goroutines, network or owned context resources before Subscribe.
	return &Subscriber{shutdownDone: make(chan struct{}), drained: make(chan struct{}), config: config, running: map[string]consumer{}, health: map[string]*atomic.Int64{}, slots: make(chan struct{}, c.MaxInFlight), errors: make(chan error, c.MaxInFlight)}, nil
}

// Errors exposes transport lifecycle/renewal failures for host supervision.
func (s *Subscriber) Errors() <-chan error { return s.errors }
func (s *Subscriber) report(err error) {
	if err != nil {
		select {
		case s.errors <- err:
		default:
		}
	}
}

// Subscribe starts a SimpleConsumer for an explicit, preprovisioned route.
// The durable failure callback must commit before returning nil. It must not
// perform an ungoverned business retry. Failure to commit never authorizes Ack.
func (s *Subscriber) Subscribe(ctx context.Context, topic, subscription string, handler transport.Handler, failed func(context.Context, transport.Failure) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || ctx.Err() != nil {
		return errors.New("subscriber stopping or context ended")
	}
	if handler == nil || failed == nil {
		return errors.New("handler and durable failure callback required")
	}
	var route transport.RouteBinding
	for _, r := range s.config.Routes {
		if r.Topic == topic && r.Subscription == subscription && r.ConsumerGroup != "" {
			route = r
			break
		}
	}
	if route.ConsumerGroup == "" {
		return errors.New("subscription not in trusted route manifest")
	}
	if _, ok := s.running[route.ConsumerGroup]; ok {
		return errors.New("duplicate consumer group")
	}
	inner, err := driver.NewSimpleConsumer(&driver.Config{Endpoint: s.config.Endpoint, ConsumerGroup: route.ConsumerGroup, Credentials: &credentials.SessionCredentials{AccessKey: s.config.AccessKey, AccessSecret: s.config.SecretKey}}, driver.WithClientFuncForSimpleConsumer(func(c *driver.Config, opts ...driver.ClientOption) (driver.Client, error) {
		return driver.NewClient(c, append(opts, driver.WithQueryRouteTimeout(s.config.RequestTimeout), driver.WithConnOptions(driver.WithTLSConfig(s.config.TLS)))...)
	}), driver.WithSimpleAwaitDuration(5*time.Second), driver.WithSimpleSubscriptionExpressions(map[string]*driver.FilterExpression{route.PhysicalTopic: driver.SUB_ALL}))
	if err != nil {
		return err
	}
	inner.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(s.config.RequestTimeout)
	if err = inner.Start(); err != nil {
		return errors.Join(err, inner.GracefulStop())
	}
	if ctx.Err() != nil {
		return errors.Join(ctx.Err(), inner.GracefulStop())
	}
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}
	s.running[route.ConsumerGroup] = inner
	health := &atomic.Int64{}
	s.health[route.ConsumerGroup] = health
	s.pollers.Add(1)
	go s.poll(inner, route, handler, failed, health)
	return nil
}

func (s *Subscriber) poll(inner consumer, route transport.RouteBinding, handler transport.Handler, failed func(context.Context, transport.Failure) error, health *atomic.Int64) {
	defer s.pollers.Done()
	for {
		// Reserve capacity before receive so invisible messages never accumulate
		// behind a local work queue. One receive per available slot is bounded.
		select {
		case <-s.ctx.Done():
			return
		case s.slots <- struct{}{}:
		}
		batch := 1
	reserve:
		for batch < 16 {
			select {
			case s.slots <- struct{}{}:
				batch++
			default:
				break reserve
			}
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.config.RequestTimeout+5*time.Second)
		messages, err := inner.Receive(ctx, int32(batch), s.config.InvisibleDuration)
		cancel()
		var empty *driver.ErrRpcStatus
		if errors.As(err, &empty) && empty.Code == 40401 {
			err = nil
		}
		if err == nil {
			health.Store(time.Now().UnixNano())
		}
		if err != nil || len(messages) == 0 {
			for i := 0; i < batch; i++ {
				<-s.slots
			}
			if s.ctx.Err() != nil {
				return
			}
			s.report(err)
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if len(messages) > batch {
			s.report(errors.New("driver returned more messages than requested"))
			messages = messages[:batch]
		}
		for i := len(messages); i < batch; i++ {
			<-s.slots
		}
		for _, m := range messages {
			s.handlers.Add(1)
			go func(m *driver.MessageView) {
				defer s.handlers.Done()
				defer func() { <-s.slots }()
				s.handle(inner, route, m, handler, failed)
			}(m)
		}
	}
}

func (s *Subscriber) handle(inner consumer, route transport.RouteBinding, m *driver.MessageView, handler transport.Handler, failed func(context.Context, transport.Failure) error) {
	ctx, cancel := context.WithTimeout(s.config.DeliveryContext, s.config.HandlerTimeout)
	defer cancel()
	attempt := m.GetDeliveryAttempt()
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 65535 {
		attempt = 65535
	}
	received := transport.Received{Provider: "rocketmq", PhysicalTopic: m.GetTopic(), Wire: append([]byte(nil), m.GetBody()...), TransportID: m.GetMessageId(), Topic: route.Topic, Channel: route.Subscription, Payload: append([]byte(nil), m.GetBody()...), Attempts: uint16(attempt)}
	if born := m.GetBornTimestamp(); born != nil {
		received.Timestamp = born.UnixNano()
	}
	var decodeErr error
	if m.GetTopic() != route.PhysicalTopic {
		decodeErr = errors.New("received physical topic differs from trusted subscription")
	} else {
		envelope, recognized, err := legacy.Decode(received.Payload)
		decodeErr = err
		if recognized && err == nil {
			received.ID = envelope.UUID
			received.Metadata = envelope.Metadata
			received.Payload = envelope.Payload
		}
	}
	d := &delivery{received: received, view: m, inner: inner, ctx: ctx, timeout: s.config.RequestTimeout, retry: s.config.RetryDelay}
	done := make(chan error, 1)
	go func() { done <- s.dispatch(ctx, d, decodeErr, handler, failed) }()

	ticker := time.NewTicker(s.config.InvisibleDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if !d.Settled() {
				if err == nil {
					s.report(d.Ack())
				} else {
					s.report(d.Nack(err))
				}
			}
			return
		case <-ctx.Done():
			// Do not free the handler slot until the host actually returns. A host
			// ignoring cancellation stays visible to Close/resource supervision.
			s.report(ctx.Err())
			<-done
			return
		case <-ticker.C:
			d.mu.Lock()
			if !d.settled && ctx.Err() == nil {
				if err := inner.ChangeInvisibleDuration(m, s.config.InvisibleDuration); err != nil {
					s.report(err)
					cancel()
				}
			}
			d.mu.Unlock()
		}
	}
}

// Close first ends receives, then drains actual host handlers. On timeout it
// leaves their resources owned and may be retried; it never pretends shutdown.
func (s *Subscriber) Close(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = true
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.drainOnce.Do(func() { go func() { s.pollers.Wait(); s.handlers.Wait(); close(s.drained) }() })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.drained:
	}
	s.shutdownOnce.Do(func() {
		go func() {
			s.mu.Lock()
			running := maps.Clone(s.running)
			s.mu.Unlock()
			var err error
			for _, c := range running {
				err = errors.Join(err, c.GracefulStop())
			}
			s.shutdownErr = err
			close(s.shutdownDone)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.shutdownDone:
		return s.shutdownErr
	}
}

type delivery struct {
	mu             sync.Mutex
	received       transport.Received
	view           *driver.MessageView
	inner          consumer
	ctx            context.Context
	timeout, retry time.Duration
	settled        bool
}

func (d *delivery) Message() transport.Received {
	r := d.received
	r.Payload = append([]byte(nil), r.Payload...)
	r.Wire = append([]byte(nil), r.Wire...)
	r.Metadata = maps.Clone(r.Metadata)
	return r
}
func (d *delivery) Settled() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.settled }
func (d *delivery) Ack() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return nil
	}
	ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
	defer cancel()
	if err := d.inner.Ack(ctx, d.view); err != nil {
		return err
	}
	d.settled = true
	return nil
}
func (d *delivery) Nack(_ error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return nil
	}
	if err := d.inner.ChangeInvisibleDuration(d.view, d.retry); err != nil {
		return err
	}
	d.settled = true
	return nil
}

// Ready requires recent successful receives (including an empty long poll) on
// every registered group. It does not prove business completion.
func (s *Subscriber) Ready(maxAge time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || len(s.running) == 0 || maxAge <= 0 {
		return false
	}
	for _, last := range s.health {
		if time.Since(time.Unix(0, last.Load())) > maxAge {
			return false
		}
	}
	return true
}

func (s *Subscriber) dispatch(ctx context.Context, d *delivery, decodeErr error, handler transport.Handler, failed func(context.Context, transport.Failure) error) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("handler or failure audit panic")
		}
	}()
	received := d.Message()
	if received.Attempts > s.config.MaxAttempts {
		return failed(ctx, transport.Failure{Provider: "rocketmq", Message: received, Cause: "delivery attempts exhausted"})
	}
	if decodeErr != nil {
		err = decodeErr
	} else {
		err = handler(ctx, d)
	}
	if err != nil && received.Attempts >= s.config.MaxAttempts && !d.Settled() {
		cause := "handler failed at delivery limit"
		if decodeErr != nil {
			cause = "invalid envelope or physical source"
		}
		return failed(ctx, transport.Failure{Provider: "rocketmq", Message: d.Message(), Cause: cause})
	}
	return err
}
