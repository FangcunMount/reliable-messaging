package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
)

func TestMissingHostResources(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil DB accepted")
	}
	if _, err := Bind(nil); err == nil {
		t.Fatal("nil transaction accepted")
	}
	var a *Appender
	if err := a.Append(context.Background(), message.Message{}, time.Now()); err == nil {
		t.Fatal("nil appender accepted")
	}
}

func TestOpaqueKeyRejectsMySQLCoercion(t *testing.T) {
	s := &Store{}
	for _, key := range []string{"", "0", "01", "1junk", "1e0", "+1", "18446744073709551616"} {
		err := s.Confirm(context.Background(), outbox.Claim{RecordID: key, Token: "token", Version: 1})
		if !errors.Is(err, outbox.ErrStaleClaim) {
			t.Fatalf("noncanonical key %q accepted: %v", key, err)
		}
	}
}
