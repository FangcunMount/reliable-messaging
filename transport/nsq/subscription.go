package nsq

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

// Handoff publishes durable terminal failures. Ready must establish that the
// failure consumer is connected to the source nsqd before Publish is called.
// Only Confirmed authorizes finishing the original delivery; Unknown may
// already have published and will cause a physical duplicate on redelivery.
type Handoff interface {
	Ready(context.Context, string, string) error
	Publish(context.Context, string, string, []byte) transport.Result
}

type Backoff struct {
	BaseDelay, MaxDelay time.Duration
	JitterFraction      float64
}

type SubscriptionConfig struct {
	Topic, Channel, FailedHandoffGroup string
	MaxAttempts                        uint16
	Retry                              Backoff
	Handler                            transport.Handler
	FailedHandler                      func(context.Context, legacy.FailedHandoff) error
	Handoff                            Handoff
}

// Subscription is a connection-free binding for a host-owned NSQ consumer.
// The host must attach FailureHandler and connect the failure consumer before
// it connects a business consumer using BusinessHandler. It must set the
// driver's MaxAttempts to 0 so the SDK, not go-nsq, owns terminal settlement.
type Subscription struct {
	config       SubscriptionConfig
	failureTopic string
	mu           sync.Mutex
	causes       map[string]error
	activeMu     sync.Mutex
	active       int
	idle         chan struct{}
}

func NewSubscription(config SubscriptionConfig) (*Subscription, error) {
	if !driver.IsValidTopicName(config.Topic) || !driver.IsValidChannelName(config.Channel) {
		return nil, errors.New("valid NSQ topic and channel required")
	}
	if config.FailedHandoffGroup != "" && !driver.IsValidChannelName(config.FailedHandoffGroup) {
		return nil, errors.New("invalid failed handoff group")
	}
	if config.MaxAttempts == 0 || config.Handler == nil || config.FailedHandler == nil || config.Handoff == nil {
		return nil, errors.New("bounded delivery, handler, durable failed handler and handoff required")
	}
	if config.Retry.BaseDelay < 0 || config.Retry.MaxDelay < 0 || config.Retry.JitterFraction < 0 || config.Retry.JitterFraction > 1 {
		return nil, errors.New("invalid retry backoff")
	}
	topic := legacy.FailedHandoffTopic(config.Topic, config.Channel)
	if config.FailedHandoffGroup != "" {
		topic = legacy.FailedHandoffTopicForGroup(config.Topic, config.FailedHandoffGroup)
	}
	idle := make(chan struct{})
	close(idle)
	return &Subscription{config: config, failureTopic: topic, causes: make(map[string]error), idle: idle}, nil
}

func (s *Subscription) FailureTopic() string   { return s.failureTopic }
func (s *Subscription) FailureChannel() string { return legacy.FailedHandoffChannel }

// ConsumerConfig copies host settings and disables go-nsq's built-in attempt
// cutoff. Both business and failure consumers must use this configuration;
// otherwise go-nsq may FIN a poison message without invoking this binding.
func (s *Subscription) ConsumerConfig(base *driver.Config) *driver.Config {
	if base == nil {
		base = driver.NewConfig()
	}
	copy := *base
	copy.MaxAttempts = 0
	return &copy
}

func (s *Subscription) BusinessHandler(ctx context.Context) driver.Handler {
	return driver.HandlerFunc(func(raw *driver.Message) error { return s.HandleBusiness(ctx, raw) })
}

func (s *Subscription) FailureHandler(ctx context.Context) driver.Handler {
	return driver.HandlerFunc(func(raw *driver.Message) error { return s.HandleFailure(ctx, raw) })
}

func (s *Subscription) HandleBusiness(ctx context.Context, raw *driver.Message) error {
	if raw == nil {
		return errors.New("nil NSQ business delivery")
	}
	s.enter()
	defer s.leave()
	raw.DisableAutoResponse()
	physicalID := string(raw.ID[:])
	decoded, recognized, decodeErr := legacy.Decode(raw.Body)
	if !recognized {
		decoded = legacy.Envelope{UUID: physicalID, Payload: append([]byte(nil), raw.Body...)}
	}
	if decoded.UUID == "" {
		decoded.UUID = physicalID
	}
	received := transport.Received{
		ID: decoded.UUID, TransportID: physicalID, Topic: s.config.Topic, Channel: s.config.Channel,
		Metadata: copyStringMap(decoded.Metadata), Payload: append([]byte(nil), decoded.Payload...),
		Attempts: raw.Attempts, Timestamp: raw.Timestamp,
	}
	if decodeErr != nil {
		return s.fail(ctx, raw, received, fmt.Errorf("decode message envelope: %w", decodeErr))
	}
	key := s.key(physicalID)
	if raw.Attempts > s.config.MaxAttempts {
		cause := s.cause(key)
		if cause == nil {
			cause = errors.New("transport delivery exhausted after prior handler failure")
		}
		return s.fail(ctx, raw, received, cause)
	}
	delivery := &nsqDelivery{
		received: received,
		ack: func() error {
			s.forget(key)
			raw.Finish()
			return nil
		},
		nack: func(cause error) error { return s.fail(ctx, raw, received, cause) },
	}
	if err := s.config.Handler(ctx, delivery); err != nil {
		if !delivery.Settled() {
			return delivery.Nack(err)
		}
		return delivery.resultError()
	}
	if !delivery.Settled() {
		return delivery.Ack()
	}
	return delivery.resultError()
}

func (s *Subscription) HandleFailure(ctx context.Context, raw *driver.Message) error {
	if raw == nil {
		return errors.New("nil NSQ failed delivery")
	}
	s.enter()
	defer s.leave()
	raw.DisableAutoResponse()
	failed, err := legacy.DecodeFailedHandoff(raw.Body)
	if err != nil {
		id := string(raw.ID[:])
		failed = legacy.FailedHandoff{
			Topic: s.failureTopic, Channel: legacy.FailedHandoffChannel, UUID: id,
			TransportMessageID: id, Payload: append([]byte(nil), raw.Body...),
			Attempts: int(raw.Attempts), Timestamp: raw.Timestamp, Cause: err.Error(),
		}
	}
	if err := s.config.FailedHandler(ctx, failed); err != nil {
		raw.RequeueWithoutBackoff(s.retryDelay(int(raw.Attempts), failed.UUID))
		return err
	}
	raw.Finish()
	return nil
}

func (s *Subscription) fail(ctx context.Context, raw *driver.Message, received transport.Received, cause error) error {
	if cause == nil {
		cause = errors.New("message nacked by handler")
	}
	key := s.key(received.TransportID)
	s.remember(key, cause)
	if int(raw.Attempts) < int(s.config.MaxAttempts) {
		raw.RequeueWithoutBackoff(s.retryDelay(int(raw.Attempts), received.ID))
		return nil
	}
	body, err := legacy.EncodeFailedHandoff(legacy.FailedHandoff{
		Topic: s.config.Topic, Channel: s.config.Channel, UUID: received.ID,
		TransportMessageID: received.TransportID, Metadata: received.Metadata,
		Payload: received.Payload, Attempts: int(s.config.MaxAttempts),
		Timestamp: received.Timestamp, Cause: cause.Error(),
	})
	if err == nil {
		if raw.NSQDAddress == "" {
			err = errors.New("source NSQD address required for failed handoff")
		} else {
			err = s.config.Handoff.Ready(ctx, raw.NSQDAddress, s.failureTopic)
		}
	}
	if err == nil {
		if result := s.config.Handoff.Publish(ctx, raw.NSQDAddress, s.failureTopic, body); result.Outcome != transport.Confirmed {
			err = fmt.Errorf("NSQ failed handoff not confirmed: outcome %d", result.Outcome)
		}
	}
	if err != nil {
		raw.RequeueWithoutBackoff(s.retryDelay(int(raw.Attempts), received.ID))
		return err
	}
	s.forget(key)
	raw.Finish()
	return nil
}

func (s *Subscription) key(id string) string {
	return s.config.Topic + "\x00" + s.config.Channel + "\x00" + id
}
func (s *Subscription) remember(key string, cause error) {
	s.mu.Lock()
	s.causes[key] = cause
	s.mu.Unlock()
}
func (s *Subscription) cause(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.causes[key]
}
func (s *Subscription) forget(key string) {
	s.mu.Lock()
	delete(s.causes, key)
	s.mu.Unlock()
}

func (s *Subscription) enter() {
	s.activeMu.Lock()
	if s.active == 0 {
		s.idle = make(chan struct{})
	}
	s.active++
	s.activeMu.Unlock()
}

func (s *Subscription) leave() {
	s.activeMu.Lock()
	s.active--
	if s.active == 0 {
		close(s.idle)
	}
	s.activeMu.Unlock()
}

// WaitIdle is safe after both attached consumers have stopped admission.
func (s *Subscription) WaitIdle(ctx context.Context) error {
	s.activeMu.Lock()
	idle := s.idle
	s.activeMu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Subscription) retryDelay(attempt int, id string) time.Duration {
	backoff := s.config.Retry
	if backoff.BaseDelay <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	limit := backoff.MaxDelay
	if limit < backoff.BaseDelay {
		limit = backoff.BaseDelay
	}
	delay := backoff.BaseDelay
	for step := 1; step < attempt && delay < limit; step++ {
		if delay > limit/2 {
			delay = limit
			break
		}
		delay *= 2
	}
	if delay > limit {
		delay = limit
	}
	if backoff.JitterFraction == 0 {
		return delay
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(id + ":" + strconv.Itoa(attempt)))
	unit := float64(hash.Sum32()%20001)/10000 - 1
	return delay + time.Duration(float64(delay)*backoff.JitterFraction*unit)
}

type nsqDelivery struct {
	mu       sync.Mutex
	done     chan struct{}
	result   error
	received transport.Received
	ack      func() error
	nack     func(error) error
}

func (d *nsqDelivery) Message() transport.Received {
	d.mu.Lock()
	defer d.mu.Unlock()
	m := d.received
	m.Payload = append([]byte(nil), m.Payload...)
	m.Metadata = copyStringMap(m.Metadata)
	return m
}
func (d *nsqDelivery) Ack() error             { return d.settle(true, nil) }
func (d *nsqDelivery) Nack(cause error) error { return d.settle(false, cause) }
func (d *nsqDelivery) Settled() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done != nil
}
func (d *nsqDelivery) resultError() error {
	d.mu.Lock()
	done := d.done
	d.mu.Unlock()
	if done == nil {
		return nil
	}
	<-done
	return d.result
}
func (d *nsqDelivery) settle(ack bool, cause error) error {
	d.mu.Lock()
	if d.done != nil {
		done := d.done
		d.mu.Unlock()
		<-done
		return d.result
	}
	d.done = make(chan struct{})
	d.mu.Unlock()
	var result error
	if ack {
		result = d.ack()
	} else {
		result = d.nack(cause)
	}
	d.mu.Lock()
	d.result = result
	close(d.done)
	d.mu.Unlock()
	return result
}

func copyStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
