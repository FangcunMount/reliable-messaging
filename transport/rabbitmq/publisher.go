// Package rabbitmq contains an isolated AMQP publisher candidate. A host owns
// the connection, channel, topology, and reconnect lifecycle.
package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Route maps a stable logical destination to a host-provisioned AMQP route.
// It does not assert that every required consumer binding exists.
type Route struct {
	Exchange   string
	RoutingKey string
}

type sender interface {
	Send(context.Context, Route, message.Input) transport.Outcome
}

// Publisher allows one in-flight publish on its borrowed channel. The host
// must put the channel into confirm mode before New and must not share it with
// another publisher. The host validates required bindings separately.
type Publisher struct {
	sender   sender
	routes   map[string]Route
	slot     chan struct{}
	mu       sync.Mutex
	active   int
	stopping bool
	drained  chan struct{}
}

// New registers return and close listeners without network I/O or goroutines.
// The caller retains channel ownership and closes it after Drain. A channel
// without confirm mode can never produce Confirmed.
func New(ch *amqp.Channel, routes map[string]Route) (*Publisher, error) {
	if ch == nil {
		return nil, errors.New("host AMQP channel required")
	}
	if err := validateRoutes(routes); err != nil {
		return nil, err
	}
	return newPublisher(&amqpSender{
		channel: ch,
		returns: ch.NotifyReturn(make(chan amqp.Return, 1)),
		closed:  ch.NotifyClose(make(chan *amqp.Error, 1)),
	}, routes)
}

func newPublisher(s sender, routes map[string]Route) (*Publisher, error) {
	if s == nil {
		return nil, errors.New("sender required")
	}
	if err := validateRoutes(routes); err != nil {
		return nil, err
	}
	copied := make(map[string]Route, len(routes))
	for destination, route := range routes {
		copied[destination] = route
	}
	return &Publisher{
		sender: s, routes: copied, slot: make(chan struct{}, 1), drained: make(chan struct{}),
	}, nil
}

func validateRoutes(routes map[string]Route) error {
	if len(routes) == 0 {
		return errors.New("routes required")
	}
	for destination, route := range routes {
		if destination == "" || route.Exchange == "" {
			return errors.New("logical destination and exchange required")
		}
	}
	return nil
}

// Publish returns on ctx cancellation even though amqp091-go v1.10.0 ignores
// context during PublishWithDeferredConfirmWithContext. Its bounded worker
// retains the slot until the driver returns or the host closes the channel.
func (p *Publisher) Publish(ctx context.Context, m message.Message) transport.Result {
	in := m.Input()
	route, ok := p.routes[in.Destination]
	if !ok || !m.Valid() {
		return transport.Result{Outcome: transport.Rejected}
	}
	if ctx.Err() != nil {
		return transport.Result{Outcome: transport.Unknown}
	}
	select {
	case p.slot <- struct{}{}:
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
	p.mu.Lock()
	if p.stopping || ctx.Err() != nil {
		p.mu.Unlock()
		<-p.slot
		return transport.Result{Outcome: transport.Unknown}
	}
	p.active++
	p.mu.Unlock()
	done := make(chan transport.Outcome, 1)
	go func() {
		outcome := p.sender.Send(ctx, route, in)
		done <- outcome
		<-p.slot
		p.mu.Lock()
		p.active--
		if p.stopping && p.active == 0 {
			close(p.drained)
		}
		p.mu.Unlock()
	}()
	select {
	case outcome := <-done:
		return transport.Result{Outcome: outcome}
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
}

// Drain permanently rejects new admission and waits for the real driver call.
// It does not close the host-owned AMQP channel.
func (p *Publisher) Drain(ctx context.Context) error {
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
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

type amqpChannel interface {
	PublishWithDeferredConfirmWithContext(context.Context, string, string, bool, bool, amqp.Publishing) (*amqp.DeferredConfirmation, error)
}

type amqpSender struct {
	channel  amqpChannel
	returns  <-chan amqp.Return
	closed   <-chan *amqp.Error
	poisoned atomic.Bool
}

func (s *amqpSender) Send(ctx context.Context, route Route, in message.Input) transport.Outcome {
	if s.poisoned.Load() {
		return transport.Unknown
	}
	select {
	case <-s.closed:
		s.poisoned.Store(true)
		return transport.Unknown
	case <-s.returns: // a stale return means the channel was shared or lost correlation
		s.poisoned.Store(true)
		return transport.Unknown
	default:
	}
	occurred, err := time.Parse(time.RFC3339Nano, in.OccurredAt)
	if err != nil {
		return transport.Rejected
	}
	confirmation, err := s.channel.PublishWithDeferredConfirmWithContext(ctx, route.Exchange, route.RoutingKey, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    in.ID,
		ContentType:  in.ContentType,
		Type:         in.EventType,
		Timestamp:    occurred,
		Body:         in.Payload,
		Headers: amqp.Table{
			"x-rm-producer":       in.Producer,
			"x-rm-destination":    in.Destination,
			"x-rm-schema-version": in.SchemaVersion,
			"x-rm-scope":          in.Scope,
			"x-rm-occurred-at":    in.OccurredAt,
		},
	})
	if err != nil || confirmation == nil {
		s.poisoned.Store(true)
		return transport.Unknown
	}
	returned := false
	for {
		select {
		case ret, ok := <-s.returns:
			if !ok || ret.MessageId != in.ID || !bytes.Equal(ret.Body, in.Payload) {
				s.poisoned.Store(true)
				return transport.Unknown
			}
			returned = true
		case <-s.closed:
			s.poisoned.Store(true)
			return transport.Unknown
		case <-confirmation.Done():
			// RabbitMQ sends mandatory return before its confirm on this channel.
			// Both may be ready by the time this goroutine is scheduled.
			select {
			case ret, ok := <-s.returns:
				if !ok || ret.MessageId != in.ID || !bytes.Equal(ret.Body, in.Payload) {
					s.poisoned.Store(true)
					return transport.Unknown
				}
				returned = true
			default:
			}
			if returned {
				return transport.Rejected
			}
			if confirmation.Acked() {
				return transport.Confirmed
			}
			return transport.Unknown
		}
	}
}

var _ transport.Publisher = (*Publisher)(nil)
