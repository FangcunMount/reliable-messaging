package rabbitmq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/FangcunMount/reliable-messaging/transport"
)

func TestBindingMatchesRoutingKey(t *testing.T) {
	tests := []struct {
		kind, binding, key string
		want               bool
	}{
		{"direct", "a.b", "a.b", true},
		{"direct", "a.*", "a.b", false},
		{"fanout", "", "any.value", true},
		{"topic", "a.*", "a.b", true},
		{"topic", "a.*", "a.b.c", false},
		{"topic", "a.#", "a", true},
		{"topic", "a.#", "a.b.c", true},
		{"topic", "#.b", "a.b", true},
		{"topic", "#.b", "a.c", false},
	}
	for _, tc := range tests {
		if got := bindingMatches(tc.kind, tc.binding, tc.key); got != tc.want {
			t.Errorf("%s %q matches %q = %v, want %v", tc.kind, tc.binding, tc.key, got, tc.want)
		}
	}
}

func TestManagementVerifierDistinguishesMissingFromUnknown(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "password" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if current := int(status.Load()); current != http.StatusOK {
			w.WriteHeader(current)
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/exchanges/"):
			_, _ = w.Write([]byte(`{"name":"assessment","type":"direct","durable":true}`))
		case strings.HasPrefix(r.URL.Path, "/api/queues/"):
			_, _ = w.Write([]byte(`{"name":"worker","type":"classic","durable":true}`))
		case strings.HasPrefix(r.URL.Path, "/api/bindings/"):
			_, _ = w.Write([]byte(`[{"source":"assessment","destination":"worker","routing_key":"submitted"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	v, err := NewManagementVerifier(server.URL, "/", "user", "password", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	route := Route{Exchange: "assessment", ExchangeKind: "direct", RoutingKey: "submitted",
		RequiredQueues: []RequiredQueue{{Name: "worker", BindingKey: "submitted", QueueType: "classic"}}}
	if got := v.Verify(context.Background(), route); got != transport.Confirmed {
		t.Fatalf("valid topology = %v", got)
	}
	status.Store(http.StatusNotFound)
	if got := v.Verify(context.Background(), route); got != transport.Rejected {
		t.Fatalf("missing topology = %v", got)
	}
	status.Store(http.StatusServiceUnavailable)
	if got := v.Verify(context.Background(), route); got != transport.Unknown {
		t.Fatalf("management outage = %v", got)
	}
}
