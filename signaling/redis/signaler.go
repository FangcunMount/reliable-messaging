package redis

import (
	"context"
	"fmt"

	"github.com/FangcunMount/reliable-messaging/signaling"
	goredis "github.com/redis/go-redis/v9"
)

// Signaler 实现 signaling.Notifier 与 signaling.Watcher。
type Signaler[T signaling.Signal] struct {
	client goredis.UniversalClient
	opts   Options
	codec  Codec[T]
}

// NewSignaler 创建 Redis signaling 实现。
func NewSignaler[T signaling.Signal](client goredis.UniversalClient, opts Options) *Signaler[T] {
	return NewSignalerWithCodec[T](client, opts, JSONCodec[T]{})
}

// NewSignalerWithCodec 使用自定义 codec 创建 Signaler。
func NewSignalerWithCodec[T signaling.Signal](client goredis.UniversalClient, opts Options, codec Codec[T]) *Signaler[T] {
	if codec == nil {
		codec = JSONCodec[T]{}
	}
	return &Signaler[T]{
		client: client,
		opts:   normalizeOptions(opts),
		codec:  codec,
	}
}

// Notify 发布一个信号（best-effort）。
func (s *Signaler[T]) Notify(ctx context.Context, signal T) error {
	if signal.SignalName() == "" {
		return signaling.ErrEmptySignalName
	}
	payload, err := s.codec.Marshal(signal)
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, s.channel(signal.SignalName()), payload).Err()
}

// Watch 监听信号并交给 handler 处理。
func (s *Signaler[T]) Watch(ctx context.Context, handler signaling.Handler[T]) error {
	if handler == nil {
		return signaling.ErrNilHandler
	}

	var zero T
	signalName := zero.SignalName()
	if signalName == "" && s.opts.Channel == "" {
		return signaling.ErrEmptySignalName
	}

	pubsub := s.client.Subscribe(ctx, s.channel(signalName))
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}

	msgCh := pubsub.Channel(goredis.WithChannelSize(s.opts.BufferSize))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			var signal T
			if err := s.codec.Unmarshal([]byte(msg.Payload), &signal); err != nil {
				s.handleError(err)
				continue
			}
			handler(ctx, signal)
		}
	}
}

func (s *Signaler[T]) channel(signalName string) string {
	if s.opts.Channel != "" {
		return s.opts.Channel
	}
	return fmt.Sprintf("%s:%s", s.opts.Prefix, signalName)
}

func (s *Signaler[T]) handleError(err error) {
	if s.opts.ErrorHandler != nil {
		s.opts.ErrorHandler(err)
	}
}
