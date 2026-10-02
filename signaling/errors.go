package signaling

import "errors"

var (
	// ErrEmptySignalName 表示信号名为空。
	ErrEmptySignalName = errors.New("signal name is empty")
	// ErrNilHandler 表示监听处理函数为空。
	ErrNilHandler = errors.New("signal handler is nil")
)
