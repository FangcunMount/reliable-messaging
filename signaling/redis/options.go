package redis

import "time"

const (
	defaultPrefix     = "signal"
	defaultBufferSize = 100
)

// Options 定义 Redis signaling 行为。
type Options struct {
	// Prefix 与 signal_name 组成 channel：{prefix}:{signal_name}。
	Prefix string
	// Channel 显式指定 channel，优先级高于 Prefix + SignalName。
	Channel string
	// BufferSize 为 Pub/Sub channel 缓冲区大小。
	BufferSize int
	// ReadTimeout 保留字段，用于后续扩展读取超时策略。
	ReadTimeout time.Duration
	// ErrorHandler 处理监听中的非致命错误（如 decode 失败）。
	ErrorHandler func(error)
}

// DefaultOptions 返回默认配置。
func DefaultOptions() Options {
	return Options{
		Prefix:     defaultPrefix,
		BufferSize: defaultBufferSize,
	}
}

func normalizeOptions(opts Options) Options {
	if opts.Prefix == "" && opts.Channel == "" {
		opts.Prefix = defaultPrefix
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	return opts
}
