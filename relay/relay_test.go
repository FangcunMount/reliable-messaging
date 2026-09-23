package relay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/message"
	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/transport"
)

type fakeStore struct {
	mu       sync.Mutex
	claims   []outbox.Claim
	scans    int
	scanCh   chan struct{}
	writes   []string
	writeErr error
	cancel   context.CancelFunc
}

func (s *fakeStore) ClaimDue(_ context.Context, n int, _ time.Duration) ([]outbox.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scans++
	if s.scanCh != nil {
		select {
		case s.scanCh <- struct{}{}:
		default:
		}
	}
	if n > len(s.claims) {
		n = len(s.claims)
	}
	c := s.claims[:n]
	s.claims = s.claims[n:]
	return c, nil
}
func (s *fakeStore) write(kind string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, kind)
	if s.cancel != nil {
		s.cancel()
	}
	return s.writeErr
}
func (s *fakeStore) Confirm(context.Context, outbox.Claim) error { return s.write("confirm") }
func (s *fakeStore) Retry(_ context.Context, _ outbox.Claim, delay time.Duration, _ string) error {
	if delay <= 0 {
		return errors.New("retry delay not positive")
	}
	return s.write("retry")
}
func (s *fakeStore) Quarantine(context.Context, outbox.Claim, string) error {
	return s.write("quarantine")
}

type publishFunc func(context.Context, message.Message) transport.Result

func (f publishFunc) Publish(ctx context.Context, m message.Message) transport.Result {
	return f(ctx, m)
}
func config() Config {
	return Config{Concurrency: 2, PollInterval: time.Millisecond, Lease: time.Second, PublishTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, Retry: func(outbox.Claim, transport.Outcome) RetryDecision { return RetryDecision{Delay: time.Second} }, Observe: func(Event) {}}
}

func TestPostCommitWakeRescansWithoutWaitingForPollInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeStore{scanCh: make(chan struct{}, 2)}
	wake := make(chan struct{}, 1)
	c := config()
	c.PollInterval = time.Hour
	c.Wake = wake
	r, err := New(s, publishFunc(func(context.Context, message.Message) transport.Result {
		return transport.Result{Outcome: transport.Confirmed}
	}), c)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-s.scanCh:
	case <-time.After(time.Second):
		t.Fatal("initial scan did not start")
	}
	wake <- struct{}{}
	select {
	case <-s.scanCh:
	case <-time.After(time.Second):
		t.Fatal("post-commit wake did not trigger a scan")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not stop")
	}
}

func TestMissedPostCommitWakeStillUsesPeriodicScan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeStore{scanCh: make(chan struct{}, 2)}
	c := config()
	c.PollInterval = 20 * time.Millisecond
	c.Wake = make(chan struct{}) // No notification arrives.
	r, err := New(s, publishFunc(func(context.Context, message.Message) transport.Result {
		return transport.Result{Outcome: transport.Confirmed}
	}), c)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	for range 2 {
		select {
		case <-s.scanCh:
		case <-time.After(time.Second):
			t.Fatal("periodic recovery scan did not run")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not stop")
	}
}

func TestBoundedDrainAndSingleRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeStore{claims: make([]outbox.Claim, 5)}
	entered := make(chan struct{}, 5)
	release := make(chan struct{})
	p := publishFunc(func(ctx context.Context, _ message.Message) transport.Result {
		entered <- struct{}{}
		select {
		case <-release:
			return transport.Result{Outcome: transport.Confirmed}
		case <-ctx.Done():
			return transport.Result{Outcome: transport.Unknown}
		}
	})
	c := config()
	c.PublishTimeout = time.Second
	c.Lease = 2 * time.Second
	r, err := New(s, p, c)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("workers did not enter")
		}
	}
	if err = r.Run(context.Background()); err == nil {
		t.Fatal("concurrent Run accepted")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("returned before admitted work drained")
	default:
	}
	close(release)
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain stuck")
	}
	if len(s.writes) != 2 || len(s.claims) != 3 || s.scans != 1 {
		t.Fatalf("admission/drain mismatch: %+v", s)
	}
}
func TestOutcomeAndWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outcome    transport.Outcome
		quarantine bool
		writeErr   error
		want       string
		event      string
	}{
		{"confirmed", transport.Confirmed, false, nil, "confirm", "write_succeeded"},
		{"unknown", transport.Unknown, false, nil, "retry", "write_succeeded"},
		{"rejected", transport.Rejected, true, nil, "quarantine", "write_succeeded"},
		{"stale", transport.Confirmed, false, outbox.ErrStaleClaim, "confirm", "stale_write_rejected"},
		{"database", transport.Confirmed, false, errors.New("DB unavailable"), "confirm", "write_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &fakeStore{claims: make([]outbox.Claim, 1), cancel: cancel, writeErr: tc.writeErr}
			events := []Event{}
			c := config()
			c.Observe = func(e Event) { events = append(events, e) }
			c.Retry = func(outbox.Claim, transport.Outcome) RetryDecision {
				return RetryDecision{Delay: time.Second, Quarantine: tc.quarantine}
			}
			r, err := New(s, publishFunc(func(context.Context, message.Message) transport.Result { return transport.Result{Outcome: tc.outcome} }), c)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if len(s.writes) != 1 || s.writes[0] != tc.want || len(events) != 2 || events[1].Kind != tc.event {
				t.Fatalf("wrong result: %v %v", s.writes, events)
			}
		})
	}
}
func TestPublishTimeoutIsRecoverable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeStore{claims: make([]outbox.Claim, 1), cancel: cancel}
	c := config()
	c.PublishTimeout = time.Millisecond
	r, err := New(s, publishFunc(func(ctx context.Context, _ message.Message) transport.Result {
		<-ctx.Done()
		return transport.Result{Outcome: transport.Unknown}
	}), c)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish ignored timeout")
	}
	if len(s.writes) != 1 || s.writes[0] != "retry" {
		t.Fatal("unknown timeout not retried")
	}
}
