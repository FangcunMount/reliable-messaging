// Host lifecycle reference: explicit start, stop admission, drain, then close.
// No production Relay implementation is implied by this example.
package main

import (
	"context"
	"fmt"
)

type runner struct{ jobs <-chan func() }

func newRunner(jobs <-chan func()) runner { return runner{jobs: jobs} }

// run returns after an already admitted operation finishes. The real Relay
// must additionally bound transport calls, drain duration and lease recovery.
func (r runner) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case job, ok := <-r.jobs:
			if !ok {
				return
			}
			if ctx.Err() != nil {
				return
			}
			job()
		}
	}
}

func main() {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	jobs := make(chan func())
	r := newRunner(jobs) // No goroutine or resources acquired by construction.
	done := make(chan struct{})
	go func() { defer close(done); r.run(ctx) }() // Host starts explicitly.
	started, release := make(chan struct{}), make(chan struct{})
	jobs <- func() { close(started); <-release }
	<-started
	stop() // Stop admission; do not close host resources while work is in flight.
	close(release)
	<-done // Drain completes before the host closes DB / transport resources.
	fmt.Println("PASS explicit start, stop admission, drain, then host resource close")
}
