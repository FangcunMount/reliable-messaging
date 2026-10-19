package transaction

import (
	"context"
	"time"
)

// Limits bound admission before goroutines/threads/DB queries are created.
type Limits struct {
	Sends, Branches, CheckWorkers, CheckQueue, ReconcileBatch int
	CheckTimeout, RPCTimeout, Lease, ReconcileInterval        time.Duration
}

func DefaultLimits() Limits {
	return Limits{8, 16, 4, 32, 100, 2 * time.Second, 5 * time.Second, 60 * time.Second, 5 * time.Second}
}
func (l Limits) Valid() bool {
	return l.Sends > 0 && l.Sends <= 1024 && l.Branches > 0 && l.Branches <= MaxBranches && l.CheckWorkers > 0 && l.CheckWorkers <= 128 && l.CheckQueue >= 0 && l.CheckQueue <= 10000 && l.ReconcileBatch > 0 && l.ReconcileBatch <= 1000 && l.CheckTimeout > 0 && l.RPCTimeout > 0 && l.Lease > l.RPCTimeout && l.ReconcileInterval > 0
}

type Runtime interface {
	Start(context.Context) error
	StopAdmission()
	Close(context.Context) error
	Wait(context.Context) error
}

// ShutdownIncomplete reports work still owned by this runtime at the deadline.
// Counts are a snapshot; Wait/Close can continue draining the same resources.
type ShutdownIncomplete struct {
	ActiveSends, RunningChecks, QueuedChecks int
	RuntimePending                           bool
	Cause                                    error
}

func (e *ShutdownIncomplete) Error() string        { return "transaction runtime shutdown incomplete" }
func (e *ShutdownIncomplete) Unwrap() error        { return e.Cause }
func (e *ShutdownIncomplete) Is(target error) bool { return target == ErrShutdownIncomplete }
