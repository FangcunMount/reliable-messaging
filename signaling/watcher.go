package signaling

import "context"

// Watcher 提供信号监听能力。
type Watcher[T Signal] interface {
	Watch(ctx context.Context, handler Handler[T]) error
}
