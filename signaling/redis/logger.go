package redis

// ErrorLogger adapts loggers with an Error method. ErrorHandler remains host-owned.
type ErrorLogger interface {
	Error(args ...any)
}
