package signaling

// Signal 描述一个临时通知信号。
// SignalName 用于路由到 Redis channel；
// SignalKey 用于业务侧按键分发（例如 assessment_id）。
type Signal interface {
	SignalName() string
	SignalKey() string
}
