//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

func observationProofs(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	binding := transaction.Binding{Owner: "observe.mysql", StoreID: "observation-store", WriterEpoch: "epoch"}
	if _, err := db.ExecContext(ctx, `INSERT INTO rm_tx_binding VALUES(?,?,?,1)`, binding.Owner, binding.StoreID, binding.WriterEpoch); err != nil {
		t.Fatal(err)
	}
	s, err := New(db, binding)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := s.ObserveProtocol(ctx)
	if err != nil || empty.AsOf.IsZero() || empty.ConfirmationGapBranches != 0 {
		t.Fatal("empty owner observation failed", err)
	}
	makeExecution := func(id string) transaction.Execution {
		input := plan(t, "observe-"+id, "observe-"+id).Input()
		input.Owner = binding.Owner
		second := input.Branches[0]
		second.ID = "second"
		input.Branches = append(input.Branches, second)
		p, err := transaction.NewPlan(input)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Reserve(ctx, transaction.RequestFor(p), p, transaction.ReserveOptions{Lease: time.Minute})
		if err != nil || !r.Acquired {
			t.Fatal("observation reservation failed", err)
		}
		return r.Execution
	}
	_ = makeExecution("reserved")
	preparing := makeExecution("preparing")
	if err = s.BeginPrepare(ctx, preparing); err != nil {
		t.Fatal(err)
	}
	ek := key(preparing)
	if _, err = db.ExecContext(ctx, `UPDATE rm_tx_execution SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 2 MINUTE WHERE execution_key=?`, ek[:]); err != nil {
		t.Fatal(err)
	}
	gap := makeExecution("gap")
	if err = s.BeginPrepare(ctx, gap); err != nil {
		t.Fatal(err)
	}
	for _, ref := range gap.References() {
		if err = s.RecordHalf(ctx, gap, ref, transaction.HalfReceipt{Outcome: transaction.HalfUnknown}); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.Abort(ctx, gap); err != nil || !ok {
		t.Fatal("gap abort failed", err)
	}
	ek = key(gap)
	if _, err = db.ExecContext(ctx, `UPDATE rm_tx_witness SET half_recorded_at=UTC_TIMESTAMP(6)-INTERVAL 10 MINUTE WHERE execution_key=?`, ek[:]); err != nil {
		t.Fatal(err)
	}
	untracked := makeExecution("untracked")
	if err = s.BeginPrepare(ctx, untracked); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Abort(ctx, untracked); err != nil || !ok {
		t.Fatal("untracked abort failed", err)
	}
	committed := makeExecution("committed")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	guard, err := s.Bind(ctx, tx, committed)
	if err != nil {
		t.Fatal(err)
	}
	if err = guard.CommitWitness(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := s.ObserveProtocol(ctx)
	if err != nil || before.UnobservedCommittedBranches != 0 {
		t.Fatal("uncommitted witness leaked into observation", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.ObserveProtocol(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReservedExecutions != 1 || snapshot.PreparingExecutions != 1 || snapshot.PreparedExpiryAgeSeconds < 120 ||
		snapshot.ConfirmationGapBranches != 2 || snapshot.OldestConfirmationObservationAgeSeconds < 600 ||
		snapshot.UntrackedAbortedBranches != 2 || snapshot.UnobservedCommittedBranches != 2 || snapshot.MetadataConflicts != 0 {
		t.Fatalf("wrong owner-isolated metadata snapshot: %+v", snapshot)
	}
	for _, ref := range gap.References() {
		if err = s.RecordRecoveryFinal(ctx, ref, transaction.FinalRollbackConfirmed); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = s.ObserveProtocol(ctx)
	if err != nil || snapshot.ConfirmationGapBranches != 0 || snapshot.UntrackedAbortedBranches != 2 {
		t.Fatal("positive audit did not clear only known gap", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.ObserveProtocol(cancelled); err == nil {
		t.Fatal("cancelled observation became empty success")
	}
	wrong := *s
	wrong.binding.WriterEpoch = "wrong"
	if _, err = wrong.ObserveProtocol(ctx); !errors.Is(err, transaction.ErrBinding) {
		t.Fatal("wrong writer binding accepted", err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE rm_tx_binding SET schema_version=2 WHERE owner=?`, binding.Owner); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ObserveProtocol(ctx); !errors.Is(err, transaction.ErrSchema) {
		t.Fatal("wrong schema accepted", err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE rm_tx_binding SET schema_version=1 WHERE owner=?`, binding.Owner); err != nil {
		t.Fatal(err)
	}
	ek = key(gap)
	if _, err = db.ExecContext(ctx, `UPDATE rm_tx_witness SET half_outcome='invented' WHERE execution_key=?`, ek[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ObserveProtocol(ctx); err == nil {
		t.Fatal("invalid metadata became zero success")
	}
}
