package nsq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestResolveTopicProducersDeduplicatesAndUsesBroadcastAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/lookup" || r.URL.Query().Get("topic") != "business.events" {
			t.Errorf("wrong lookup request %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"producers":[{"broadcast_address":"nsqd-b","hostname":"ignore","tcp_port":4150},{"broadcast_address":"nsqd-a","tcp_port":4151},{"broadcast_address":"nsqd-b","tcp_port":4150},{"hostname":"fallback","tcp_port":4152},{"broadcast_address":"bad","tcp_port":0}]}`))
	}))
	defer server.Close()
	addresses, err := resolveTopicProducers(context.Background(), []string{server.URL}, "business.events")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"fallback:4152", "nsqd-a:4151", "nsqd-b:4150"}
	if !reflect.DeepEqual(addresses, want) {
		t.Fatalf("addresses = %v, want %v", addresses, want)
	}
}

func TestResolveTopicProducersRequiresUsableSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"producers":[]}`))
	}))
	defer server.Close()
	if _, err := resolveTopicProducers(context.Background(), []string{server.URL}, "business.events"); err == nil {
		t.Fatal("empty lookupd result accepted")
	}
	if _, err := resolveTopicProducers(context.Background(), []string{"file:///etc/passwd"}, "business.events"); err == nil {
		t.Fatal("unsupported lookupd scheme accepted")
	}
}

func TestResolveTopicProducersKeepsHealthyLookupdWhenPeerFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"producers":[{"broadcast_address":"nsqd-a","tcp_port":4150}]}`))
	}))
	defer server.Close()
	addresses, err := resolveTopicProducers(context.Background(), []string{"file:///unsupported", server.URL}, "business.events")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(addresses, []string{"nsqd-a:4150"}) {
		t.Fatalf("healthy lookupd producers lost: %v", addresses)
	}
}

func TestResolveBootstrapSourcesUsesOneActiveNodeBeforeTopicExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/lookup":
			w.WriteHeader(http.StatusNotFound)
		case "/nodes":
			_, _ = w.Write([]byte(`{"producers":[{"broadcast_address":"nsqd-b","tcp_port":4150},{"broadcast_address":"nsqd-a","tcp_port":4150}]}`))
		default:
			t.Errorf("unexpected lookupd path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	addresses, err := resolveBootstrapSources(context.Background(), []string{server.URL}, "new.topic")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(addresses, []string{"nsqd-a:4150"}) {
		t.Fatalf("bootstrap addresses = %v", addresses)
	}
}

func TestResolveBootstrapSourcesFailsWithoutAnyNSQD(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lookup" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"producers":[]}`))
	}))
	defer server.Close()
	if _, err := resolveBootstrapSources(context.Background(), []string{server.URL}, "new.topic"); err == nil {
		t.Fatal("missing NSQD bootstrap node accepted")
	}
}
