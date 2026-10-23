//go:build integration

package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
)

func observationProofs(t *testing.T, db *driver.Database) {
	ctx := context.Background()
	binding := transaction.Binding{Owner: "observe.mongo", StoreID: "observation-store", WriterEpoch: "epoch"}
	if _, err := db.Collection("rm_tx_binding").InsertOne(ctx, bson.M{"_id": binding.Owner, "store_id": binding.StoreID, "writer_epoch": binding.WriterEpoch, "schema_version": 1}); err != nil {
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
	if _, err = db.Collection("rm_tx_execution").UpdateOne(ctx, bson.M{"_id": ek[:]}, driver.Pipeline{bson.D{{Key: "$set", Value: bson.M{"lease_until": bson.M{"$subtract": bson.A{"$$NOW", 120000}}}}}}); err != nil {
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
	// Fixture-only clock control; protocol state and identity are unchanged.
	if _, err = db.Collection("rm_tx_execution").UpdateOne(ctx, bson.M{"_id": ek[:]}, bson.M{"$set": bson.M{"witnesses.$[].half_recorded_at": time.Now().UTC().Add(-10 * time.Minute)}}); err != nil {
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
	session, err := db.Client().StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(ctx)
	if err = session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	sc := driver.NewSessionContext(ctx, session)
	guard, err := s.Bind(sc, committed)
	if err != nil {
		t.Fatal(err)
	}
	if err = guard.CommitWitness(sc); err != nil {
		t.Fatal(err)
	}
	before, err := s.ObserveProtocol(ctx)
	if err != nil || before.UnobservedCommittedBranches != 0 {
		t.Fatal("uncommitted witness leaked into observation", err)
	}
	if err = session.CommitTransaction(sc); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.ObserveProtocol(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReservedExecutions != 1 || snapshot.PreparingExecutions != 1 || snapshot.PreparedExpiryAgeSeconds < 120 ||
		snapshot.ConfirmationGapBranches != 2 || snapshot.OldestConfirmationObservationAgeSeconds < 590 ||
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
	if _, err = db.Collection("rm_tx_binding").UpdateOne(ctx, bson.M{"_id": binding.Owner}, bson.M{"$set": bson.M{"schema_version": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ObserveProtocol(ctx); !errors.Is(err, transaction.ErrSchema) {
		t.Fatal("wrong schema accepted", err)
	}
	if _, err = db.Collection("rm_tx_binding").UpdateOne(ctx, bson.M{"_id": binding.Owner}, bson.M{"$set": bson.M{"schema_version": 1}}); err != nil {
		t.Fatal(err)
	}
	ek = key(gap)
	if _, err = db.Collection("rm_tx_execution").UpdateOne(ctx, bson.M{"_id": ek[:]}, bson.M{"$set": bson.M{"witnesses.$[].half_outcome": "invented"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ObserveProtocol(ctx); err == nil {
		t.Fatal("invalid metadata became zero success")
	}
}
