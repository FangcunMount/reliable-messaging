package signaling

import "context"

// Handler 定义信号处理函数。
type Handler[T Signal] func(ctx context.Context, signal T)
