package nsq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

type SubscriberConfig struct {
	NSQDAddresses      []string
	LookupdAddresses   []string
	Driver             *driver.Config
	DeliveryContext    context.Context
	MaxInFlight        int
	MaxAttempts        uint16
	Retry              Backoff
	FailedHandoffGroup string
}

type runningSubscription struct {
	binding  *Subscription
	business *driver.Consumer
	failure  *driver.Consumer
	handoff  *DirectHandoff
}

// Subscriber owns the consumers and handoff producers it creates. NewSubscriber
// only validates and copies configuration. Subscribe explicitly starts network
// work; Close stops admission and waits for actual handlers and sends.
// The host explicitly selects direct nsqd or lookupd discovery. EventBus and
// service integration remain separate M6-04B work before IAM/QS migration.
type Subscriber struct {
	config     SubscriberConfig
	mu         sync.Mutex
	running    []*runningSubscription
	identities map[string]struct{}
	stopping   bool
}

func NewSubscriber(config SubscriberConfig) (*Subscriber, error) {
	if (len(config.NSQDAddresses) == 0) == (len(config.LookupdAddresses) == 0) || config.MaxAttempts == 0 {
		return nil, errors.New("exactly one of NSQD or lookupd addresses, plus bounded attempts, required")
	}
	if config.FailedHandoffGroup != "" && !driver.IsValidChannelName(config.FailedHandoffGroup) {
		return nil, errors.New("invalid failed handoff group")
	}
	if config.Retry.BaseDelay < 0 || config.Retry.MaxDelay < 0 || config.Retry.JitterFraction < 0 || config.Retry.JitterFraction > 1 {
		return nil, errors.New("invalid retry backoff")
	}
	if config.Driver == nil {
		config.Driver = driver.NewConfig()
	}
	if config.DeliveryContext == nil {
		config.DeliveryContext = context.Background()
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = config.Driver.MaxInFlight
	}
	if config.MaxInFlight < 1 || config.MaxInFlight > 1000 {
		return nil, errors.New("max in-flight must be 1..1000")
	}
	if config.Driver.DialTimeout <= 0 || config.Driver.ReadTimeout <= 0 || config.Driver.WriteTimeout <= 0 {
		return nil, errors.New("bounded NSQ dial/read/write timeouts required")
	}
	addresses := make([]string, 0, len(config.NSQDAddresses))
	seen := make(map[string]struct{})
	for _, address := range config.NSQDAddresses {
		if _, _, err := net.SplitHostPort(address); err != nil {
			return nil, fmt.Errorf("invalid NSQD address %q: %w", address, err)
		}
		if _, exists := seen[address]; !exists {
			addresses = append(addresses, address)
			seen[address] = struct{}{}
		}
	}
	copy := *config.Driver
	copy.MaxAttempts = 0
	copy.MaxInFlight = config.MaxInFlight
	config.Driver = &copy
	config.NSQDAddresses = addresses
	config.LookupdAddresses = append([]string(nil), config.LookupdAddresses...)
	return &Subscriber{config: config, identities: make(map[string]struct{})}, nil
}

// Subscribe registers the failure consumer on every configured nsqd before
// connecting the business consumer. A failure to connect either side stops
// both; the original business message remains on the broker.
func (s *Subscriber) Subscribe(ctx context.Context, topic, channel string, handler transport.Handler, failed func(context.Context, legacy.FailedHandoff) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return errors.New("NSQ subscriber is stopping")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !driver.IsValidTopicName(topic) || !driver.IsValidChannelName(channel) || handler == nil || failed == nil {
		return errors.New("valid topic, channel, handler and durable failure handler required")
	}
	identity := topic + "\x00" + channel
	if _, exists := s.identities[identity]; exists {
		return errors.New("duplicate NSQ topic/channel subscription")
	}
	failureTopic := legacy.FailedHandoffTopic(topic, channel)
	if s.config.FailedHandoffGroup != "" {
		failureTopic = legacy.FailedHandoffTopicForGroup(topic, s.config.FailedHandoffGroup)
	}
	for _, running := range s.running {
		if running.binding.FailureTopic() == failureTopic {
			return errors.New("duplicate NSQ failure topic in one subscriber")
		}
	}
	addresses := s.config.NSQDAddresses
	if len(s.config.LookupdAddresses) > 0 {
		resolved, err := resolveTopicProducers(ctx, s.config.LookupdAddresses, topic)
		if err != nil {
			return fmt.Errorf("resolve NSQD producers for %s: %w", topic, err)
		}
		addresses = resolved
	}
	failureConsumer, err := driver.NewConsumer(failureTopic, legacy.FailedHandoffChannel, s.config.Driver)
	if err != nil {
		return fmt.Errorf("create failure consumer: %w", err)
	}
	handoff, err := NewDirectHandoff(failureConsumer, s.config.Driver, s.config.MaxInFlight)
	if err != nil {
		failureConsumer.Stop()
		return err
	}
	binding, err := NewSubscription(SubscriptionConfig{
		Topic: topic, Channel: channel, FailedHandoffGroup: s.config.FailedHandoffGroup,
		MaxAttempts: s.config.MaxAttempts, Retry: s.config.Retry,
		Handler: handler, FailedHandler: failed, Handoff: handoff,
	})
	if err != nil {
		failureConsumer.Stop()
		return err
	}
	businessConsumer, err := driver.NewConsumer(topic, channel, s.config.Driver)
	if err != nil {
		failureConsumer.Stop()
		return fmt.Errorf("create business consumer: %w", err)
	}
	failureConsumer.AddConcurrentHandlers(binding.FailureHandler(s.config.DeliveryContext), s.config.MaxInFlight)
	businessConsumer.AddConcurrentHandlers(binding.BusinessHandler(s.config.DeliveryContext), s.config.MaxInFlight)
	if err := failureConsumer.ConnectToNSQDs(addresses); err != nil {
		return errors.Join(fmt.Errorf("connect failure consumer: %w", err), cleanupPartial(businessConsumer, failureConsumer, binding, handoff))
	}
	if err := s.connectBusiness(businessConsumer, addresses); err != nil {
		return errors.Join(fmt.Errorf("connect business consumer: %w", err), cleanupPartial(businessConsumer, failureConsumer, binding, handoff))
	}
	s.running = append(s.running, &runningSubscription{
		binding: binding, business: businessConsumer, failure: failureConsumer, handoff: handoff,
	})
	s.identities[identity] = struct{}{}
	return nil
}

func (s *Subscriber) connectBusiness(consumer *driver.Consumer, addresses []string) error {
	if len(s.config.LookupdAddresses) > 0 {
		return consumer.ConnectToNSQLookupds(s.config.LookupdAddresses)
	}
	return consumer.ConnectToNSQDs(addresses)
}

func cleanupPartial(business, failure *driver.Consumer, binding *Subscription, handoff *DirectHandoff) error {
	business.Stop()
	failure.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	return errors.Join(waitConsumer(ctx, business), waitConsumer(ctx, failure), binding.WaitIdle(ctx), handoff.Close(ctx))
}

// Close is retryable after a context timeout. The subscriber remains stopped
// for new registrations; successful return proves all owned sends drained.
func (s *Subscriber) Close(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = true
	running := append([]*runningSubscription(nil), s.running...)
	s.mu.Unlock()
	for _, subscription := range running {
		subscription.business.Stop()
		subscription.failure.Stop()
	}
	for _, subscription := range running {
		if err := waitConsumer(ctx, subscription.business); err != nil {
			return err
		}
		if err := waitConsumer(ctx, subscription.failure); err != nil {
			return err
		}
		if err := subscription.binding.WaitIdle(ctx); err != nil {
			return err
		}
		if err := subscription.handoff.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

func waitConsumer(ctx context.Context, consumer *driver.Consumer) error {
	select {
	case <-consumer.StopChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
