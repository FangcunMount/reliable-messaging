package signaling

import "context"

// Notifier 提供信号发送能力。
type Notifier[T Signal] interface {
	Notify(ctx context.Context, signal T) error
}
