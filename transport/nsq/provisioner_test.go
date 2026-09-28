package nsq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProvisionerRequiresEveryDeclaredNSQD(t *testing.T) {
	var prepared atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/topic/create" || r.URL.Query().Get("topic") != "events" {
			t.Errorf("wrong preparation request: %s %s", r.Method, r.URL.String())
		}
		prepared.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	provisioner, err := NewProvisioner(good.Client(), []string{good.URL, good.URL, bad.URL})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Load() != 0 {
		t.Fatal("constructor performed network work")
	}
	if err := provisioner.EnsureChannel(context.Background(), "events", "business"); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("partial preparation was hidden: %v", err)
	}
	if prepared.Load() != 1 {
		t.Fatalf("duplicate endpoint was prepared %d times", prepared.Load())
	}
}

func TestProvisionerRejectsImplicitEndpointAndEphemeralChannel(t *testing.T) {
	for _, endpoint := range []string{"127.0.0.1:4151", "http://127.0.0.1:4151/path", "http://127.0.0.1:4151?topic=x", "file:///tmp/nsqd"} {
		if _, err := NewProvisioner(http.DefaultClient, []string{endpoint}); err == nil {
			t.Fatalf("accepted implicit or non-HTTP endpoint %q", endpoint)
		}
	}
	provisioner, err := NewProvisioner(http.DefaultClient, []string{"http://127.0.0.1:4151"})
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioner.EnsureChannel(context.Background(), "events", "live#ephemeral"); err == nil {
		t.Fatal("ephemeral channel treated as durable preparation")
	}
}
