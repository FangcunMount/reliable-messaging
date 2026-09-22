// Package transport defines delivery confirmation, not business completion.
package transport

import (
	"context"
	"github.com/FangcunMount/reliable-messaging/message"
)

type Outcome uint8

const (
	Unknown Outcome = iota
	Confirmed
	Rejected
)

// Result is transport-level evidence. Unknown includes lost confirmations;
// retrying it may duplicate delivery and must keep the original message.
type Result struct{ Outcome Outcome }

// Publisher must return when ctx ends and must not mutate delivery identity.
// Implementations classify uncertainty as Unknown, never as Confirmed.
type Publisher interface {
	Publish(context.Context, message.Message) Result
}
