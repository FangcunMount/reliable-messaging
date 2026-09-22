// Package relay schedules durable delivery attempts. It owns no host resources.
package relay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FangcunMount/reliable-messaging/outbox"
	"github.com/FangcunMount/reliable-messaging/transport"
)

// RetryDecision is explicitly supplied by the host; the SDK does not invent
// business retry budgets. Quarantine preserves the record for host governance.
type RetryDecision struct {
	Delay      time.Duration
	Quarantine bool
}
type RetryPolicy func(outbox.Claim, transport.Outcome) RetryDecision

// Event contains bounded categories, not payloads or metric identity labels.
// Err is for host diagnostics and must not be emitted verbatim without review.
type Event struct {
	Kind    string
	Outcome transport.Outcome
	Err     error
}

// Observer is synchronous and must be fast, nonblocking and concurrency-safe.
type Observer func(Event)

type Config struct {
	Concurrency                                       int
	PollInterval, Lease, PublishTimeout, WriteTimeout time.Duration
	Retry                                             RetryPolicy
	Observe                                           Observer
}
type Relay struct {
	store     outbox.Store
	publisher transport.Publisher
	config    Config
	running   atomic.Bool
}

// New performs no I/O and starts no goroutines.
func New(store outbox.Store, publisher transport.Publisher, c Config) (*Relay, error) {
	if store == nil || publisher == nil || c.Retry == nil || c.Observe == nil {
		return nil, errors.New("store, publisher, retry policy and observer required")
	}
	if c.Concurrency < 1 || c.Concurrency > 1000 || c.PollInterval <= 0 || c.PublishTimeout <= 0 || c.WriteTimeout <= 0 || c.Lease > 24*time.Hour || c.Lease <= c.PublishTimeout || c.WriteTimeout >= c.Lease-c.PublishTimeout {
		return nil, errors.New("invalid relay bounds: lease must exceed publish plus write timeout")
	}
	return &Relay{store: store, publisher: publisher, config: c}, nil
}

// Run stops admission on cancellation and drains the admitted batch before
// returning. Each publish and write has its own timeout. Drivers and host
// callbacks must honor their contracts; Go cannot terminate a hung callback.
// Hosts close DB/broker resources only after Run returns. Store scan errors
// return to the host supervisor; claimed rows remain recoverable by lease.
func (r *Relay) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("relay already running")
	}
	defer r.running.Store(false)
	for {
		if ctx.Err() != nil {
			return nil
		}
		claims, err := r.store.ClaimDue(ctx, r.config.Concurrency, r.config.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.config.Observe(Event{Kind: "claim_failed", Err: err})
			return err
		}
		if len(claims) > r.config.Concurrency {
			return errors.New("store exceeded requested claim limit")
		}
		var wg sync.WaitGroup
		for _, claim := range claims {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go func(c outbox.Claim) { defer wg.Done(); r.deliver(context.WithoutCancel(ctx), c) }(claim)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return nil
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
func (r *Relay) deliver(ctx context.Context, c outbox.Claim) {
	publishCtx, cancel := context.WithTimeout(ctx, r.config.PublishTimeout)
	result := r.publisher.Publish(publishCtx, c.Message)
	cancel()
	if result.Outcome != transport.Confirmed && result.Outcome != transport.Rejected {
		result.Outcome = transport.Unknown
	}
	r.config.Observe(Event{Kind: "publish_result", Outcome: result.Outcome})
	writeCtx, finish := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer finish()
	var err error
	if result.Outcome == transport.Confirmed {
		err = r.store.Confirm(writeCtx, c)
	} else {
		decision := r.config.Retry(c, result.Outcome)
		code := "publish_unknown"
		if result.Outcome == transport.Rejected {
			code = "publish_rejected"
		}
		if decision.Quarantine {
			err = r.store.Quarantine(writeCtx, c, code)
		} else if decision.Delay <= 0 {
			err = errors.New("retry policy requires positive delay or explicit quarantine")
		} else {
			err = r.store.Retry(writeCtx, c, decision.Delay, code)
		}
	}
	if err != nil {
		kind := "write_failed"
		if errors.Is(err, outbox.ErrStaleClaim) {
			kind = "stale_write_rejected"
		}
		r.config.Observe(Event{Kind: kind, Outcome: result.Outcome, Err: err})
		return
	}
	r.config.Observe(Event{Kind: "write_succeeded", Outcome: result.Outcome})
}
