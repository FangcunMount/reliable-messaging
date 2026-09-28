package transport

import "context"

// Received identifies one broker delivery. ID is the application identity;
// TransportID identifies this physical broker message. The two may differ.
type Received struct {
	ID, TransportID, Topic, Channel string
	Metadata                        map[string]string
	Payload                         []byte
	Attempts                        uint16
	Timestamp                       int64
}

// Delivery is a single-use broker delivery. Message returns an owned copy.
// Ack and Nack are safe to call more than once; only the first settles it.
// Nack's cause is retained for transport failure auditing.
type Delivery interface {
	Message() Received
	Ack() error
	Nack(error) error
	Settled() bool
}

// Handler owns business decisions such as idempotency, unknown event handling,
// durable holds and permission checks. A nil return without explicit settlement
// acknowledges the delivery; an error without settlement retries it.
type Handler func(context.Context, Delivery) error
