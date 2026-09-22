// Package nsq adapts the pinned go-nsq producer without rewriting wire bytes.
package nsq

import (
	"context"
	"errors"
	"sync"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/nsqio/go-nsq"
)

type sender interface{ Publish(string, []byte) error }
type Publisher struct {
	producer sender
	routes   map[string]string
	slots    chan struct{}
	mu       sync.Mutex
	active   int
	stopping bool
	drained  chan struct{}
}

// New borrows a host-owned producer; it neither connects nor starts goroutines.
// The host must configure bounded driver dial/read/write timeouts and must not
// stop the producer until Drain succeeds. On Drain timeout, the host may Stop
// the producer to interrupt it, then call Drain again. Routes are copied.
func New(producer *driver.Producer, routes map[string]string, maxInFlight int) (*Publisher, error) {
	if producer == nil {
		return nil, errors.New("host NSQ producer required")
	}
	return newPublisher(producer, routes, maxInFlight)
}
func newPublisher(producer sender, routes map[string]string, maxInFlight int) (*Publisher, error) {
	if maxInFlight < 1 || maxInFlight > 1000 || len(routes) == 0 {
		return nil, errors.New("routes and in-flight limit 1..1000 required")
	}
	copied := make(map[string]string, len(routes))
	for destination, topic := range routes {
		if destination == "" || !driver.IsValidTopicName(topic) {
			return nil, errors.New("invalid logical destination or NSQ topic")
		}
		copied[destination] = topic
	}
	return &Publisher{producer: producer, routes: copied, slots: make(chan struct{}, maxInFlight), drained: make(chan struct{})}, nil
}

// Publish retains its in-flight slot until the real driver call finishes, even
// if the caller times out. A timed-out driver may still deliver: return Unknown.
// Driver errors are conservatively Unknown; local routing/validation failures
// are Rejected. A nil driver error confirms NSQ acceptance, not fsync or handling.
func (p *Publisher) Publish(ctx context.Context, m message.Message) transport.Result {
	topic, ok := p.routes[m.Input().Destination]
	if !ok || !m.Valid() {
		return transport.Result{Outcome: transport.Rejected}
	}
	if ctx.Err() != nil {
		return transport.Result{Outcome: transport.Unknown}
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
	p.mu.Lock()
	if p.stopping || ctx.Err() != nil {
		p.mu.Unlock()
		<-p.slots
		return transport.Result{Outcome: transport.Unknown}
	}
	p.active++
	p.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		err := p.producer.Publish(topic, m.Input().Payload)
		done <- err
		<-p.slots
		p.mu.Lock()
		p.active--
		if p.stopping && p.active == 0 {
			close(p.drained)
		}
		p.mu.Unlock()
	}()
	select {
	case err := <-done:
		if err == nil {
			return transport.Result{Outcome: transport.Confirmed}
		}
		return transport.Result{Outcome: transport.Unknown}
	case <-ctx.Done():
		return transport.Result{Outcome: transport.Unknown}
	}
}

// Drain permanently stops new admission and waits for underlying driver calls.
// It does not stop or close the host-owned producer. Concurrent calls are safe.
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
