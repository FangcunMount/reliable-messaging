package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
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
