package mysql

import (
	"context"
	"errors"
	"testing"
)

func TestNoImplicitSQLAndIdentifierValidation(t *testing.T) {
	for _, name := range []string{"", "db.table", "x;DROP TABLE y", "`table`"} {
		if _, err := New(name); err == nil {
			t.Fatalf("accepted unsafe table identifier %q", name)
		}
	}
	store, err := New("host_outbox")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{"source", "target", "original"}
	if err := store.Confirm(context.Background(), nil, id, "hash"); !errors.Is(err, ErrConflict) {
		t.Fatal("missing original transaction was not rejected")
	}
	if _, err := store.Pending(context.Background(), nil, 20); err == nil {
		t.Fatal("pending must not open its own transaction")
	}
}
