package nsq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/nsqio/go-nsq"
)

type handoffConsumer interface{ ConnectToNSQD(string) error }
type handoffProducer interface {
	Publish(string, []byte) error
	Stop()
}

// DirectHandoff owns the producers it creates for source nsqd addresses and
// borrows the failure consumer. Construction does not connect or start work.
// Ready is called only after the failure consumer has been registered; it
// connects that consumer to the source nsqd before a terminal publish.
type DirectHandoff struct {
	consumer    handoffConsumer
	newProducer func(string) (handoffProducer, error)
	slots       chan struct{}
	mu          sync.Mutex
	topic       string
	producers   map[string]handoffProducer
	active      int
	stopping    bool
	stopped     bool
	drained     chan struct{}
}

func NewDirectHandoff(consumer *driver.Consumer, config *driver.Config, maxInFlight int) (*DirectHandoff, error) {
	if consumer == nil || config == nil {
		return nil, errors.New("failure consumer and NSQ config required")
	}
	if config.DialTimeout <= 0 || config.ReadTimeout <= 0 || config.WriteTimeout <= 0 {
		return nil, errors.New("bounded NSQ dial/read/write timeouts required")
	}
	copy := *config
	return newDirectHandoff(consumer, func(address string) (handoffProducer, error) {
		producerConfig := copy
		return driver.NewProducer(address, &producerConfig)
	}, maxInFlight)
}

func newDirectHandoff(consumer handoffConsumer, factory func(string) (handoffProducer, error), maxInFlight int) (*DirectHandoff, error) {
	if consumer == nil || factory == nil || maxInFlight < 1 || maxInFlight > 1000 {
		return nil, errors.New("consumer, producer factory and in-flight limit 1..1000 required")
	}
	return &DirectHandoff{
		consumer: consumer, newProducer: factory, slots: make(chan struct{}, maxInFlight),
		producers: make(map[string]handoffProducer), drained: make(chan struct{}),
	}, nil
}

func (h *DirectHandoff) Ready(ctx context.Context, address, topic string) error {
	if address == "" || !driver.IsValidTopicName(topic) || ctx.Err() != nil {
		return errors.New("valid source NSQD, topic and live context required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopping {
		return errors.New("NSQ failed handoff is closing")
	}
	if h.topic != "" && h.topic != topic {
		return errors.New("NSQ failed handoff topic changed")
	}
	if err := h.consumer.ConnectToNSQD(address); err != nil && !errors.Is(err, driver.ErrAlreadyConnected) {
		return fmt.Errorf("connect failure consumer to source NSQD: %w", err)
	}
	if h.producers[address] == nil {
		producer, err := h.newProducer(address)
		if err != nil {
			return fmt.Errorf("create source NSQD handoff producer: %w", err)
		}
		h.producers[address] = producer
	}
	h.topic = topic
	return nil
}

func (h *DirectHandoff) Publish(ctx context.Context, address, topic string, body []byte) transport.Result {
	unknown := transport.Result{Outcome: transport.Unknown}
	if address == "" || !driver.IsValidTopicName(topic) || ctx.Err() != nil {
		return unknown
	}
	select {
	case h.slots <- struct{}{}:
	case <-ctx.Done():
		return unknown
	}
	h.mu.Lock()
	producer := h.producers[address]
	if h.stopping || producer == nil || h.topic != topic || ctx.Err() != nil {
		h.mu.Unlock()
		<-h.slots
		return unknown
	}
	h.active++
	h.mu.Unlock()
	ownedBody := append([]byte(nil), body...)
	done := make(chan error, 1)
	go func() {
		err := producer.Publish(topic, ownedBody)
		done <- err
		<-h.slots
		h.mu.Lock()
		h.active--
		if h.stopping && h.active == 0 {
			close(h.drained)
		}
		h.mu.Unlock()
	}()
	select {
	case err := <-done:
		if err == nil {
			return transport.Result{Outcome: transport.Confirmed}
		}
		return unknown
	case <-ctx.Done():
		return unknown
	}
}

// Close stops admission, waits for all actual driver sends, then stops owned
// producers. A timeout leaves ownership intact; a later Close may finish it.
// The borrowed consumer is never stopped here.
func (h *DirectHandoff) Close(ctx context.Context) error {
	h.mu.Lock()
	if !h.stopping {
		h.stopping = true
		if h.active == 0 {
			close(h.drained)
		}
	}
	drained := h.drained
	h.mu.Unlock()
	select {
	case <-drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.stopped {
		for _, producer := range h.producers {
			producer.Stop()
		}
		h.stopped = true
	}
	return nil
}
