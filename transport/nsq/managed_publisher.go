package nsq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/nsqio/go-nsq"
)

// ManagedPublisherConfig describes one SDK-owned NSQ producer. Routes may be
// empty when the host only publishes complete wire envelopes to explicit topics.
type ManagedPublisherConfig struct {
	Address     string
	Driver      *driver.Config
	Routes      map[string]string
	MaxInFlight int
}

type ownedPublisherProducer interface {
	sender
	Ping() error
	Stop()
}

// ManagedPublisher owns the producer connection and its shutdown. It has the
// same confirmation/unknown classification as Publisher, which borrows a
// producer for hosts that still need to own their driver lifecycle.
type ManagedPublisher struct {
	publisher *Publisher
	producer  ownedPublisherProducer
	stopOnce  sync.Once
	stopped   chan struct{}
}

// NewManagedPublisher connects and pings one nsqd before returning. A failed
// construction stops the newly created producer; it does not retain ownership.
func NewManagedPublisher(config ManagedPublisherConfig) (*ManagedPublisher, error) {
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return nil, fmt.Errorf("invalid NSQD address: %w", err)
	}
	if config.Driver == nil {
		config.Driver = driver.NewConfig()
	}
	if config.Driver.DialTimeout <= 0 || config.Driver.ReadTimeout <= 0 || config.Driver.WriteTimeout <= 0 {
		return nil, errors.New("bounded NSQ dial/read/write timeouts required")
	}
	copy := *config.Driver
	producer, err := driver.NewProducer(config.Address, &copy)
	if err != nil {
		return nil, fmt.Errorf("create NSQ producer: %w", err)
	}
	managed, err := newManagedPublisher(producer, config.Routes, config.MaxInFlight)
	if err != nil {
		producer.Stop()
		return nil, err
	}
	if err := producer.Ping(); err != nil {
		producer.Stop()
		return nil, fmt.Errorf("ping NSQ producer: %w", err)
	}
	return managed, nil
}

func newManagedPublisher(producer ownedPublisherProducer, routes map[string]string, maxInFlight int) (*ManagedPublisher, error) {
	if producer == nil {
		return nil, errors.New("NSQ producer required")
	}
	publisher, err := newPublisher(producer, routes, maxInFlight)
	if err != nil {
		return nil, err
	}
	return &ManagedPublisher{publisher: publisher, producer: producer, stopped: make(chan struct{})}, nil
}

func (p *ManagedPublisher) Publish(ctx context.Context, value message.Message) transport.Result {
	return p.publisher.Publish(ctx, value)
}

func (p *ManagedPublisher) PublishRaw(ctx context.Context, topic string, body []byte) transport.Result {
	return p.publisher.PublishRaw(ctx, topic, body)
}

// Close permanently stops admission, waits for every admitted driver call,
// then stops the owned producer. If ctx expires, ownership remains with this
// object and a later Close can finish waiting; timeout is not successful drain.
func (p *ManagedPublisher) Close(ctx context.Context) error {
	if err := p.publisher.Drain(ctx); err != nil {
		return err
	}
	p.stopProducer()
	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Interrupt is the explicit forced-shutdown path after a graceful Close
// deadline. It stops admission and interrupts the driver; any in-flight
// publication has an unknown broker outcome. Call Close again to verify drain.
func (p *ManagedPublisher) Interrupt() {
	p.publisher.beginDrain()
	p.stopProducer()
}

func (p *ManagedPublisher) stopProducer() {
	p.stopOnce.Do(func() {
		go func() {
			p.producer.Stop()
			close(p.stopped)
		}()
	})
}
