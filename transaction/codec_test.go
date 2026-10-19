package transaction

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestPlanCodecPreservesFirstBytesAndRejectsCorruption(t *testing.T) {
	for _, p := range plans(t) {
		data, err := EncodePlan(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodePlan(data)
		if err != nil || !reflect.DeepEqual(p.Input(), got.Input()) {
			t.Fatal("codec changed first identity or bytes", err)
		}
		for _, bad := range [][]byte{append(append([]byte(nil), data...), []byte("{}")...), bytes.Replace(data, []byte(`"version":1`), []byte(`"version":2`), 1), bytes.Replace(data, []byte(`"plan_digest":"`), []byte(`"plan_digest":"00`), 1)} {
			if _, err := DecodePlan(bad); err == nil {
				t.Fatal("accepted corrupt/versioned plan")
			}
		}
	}
}

func TestAdvanceRequiresAllOldWitnessesAbortedAndNewGenerationExplicit(t *testing.T) {
	p := plans(t)[1]
	e := Execution{Plan: p, Fence: 1, WriterEpoch: "epoch", Token: [32]byte{1}, LeaseUntil: time.Now().Add(time.Minute)}
	refs := e.References()
	evidence := make([]*Evidence, len(refs))
	for _, execState := range []State{Prepared, Committed, Aborted} {
		for _, branchState := range []State{Prepared, Committed, Aborted} {
			for i, r := range refs {
				evidence[i] = &Evidence{r, execState, branchState}
			}
			err := ValidateAdvance(e, p, evidence, false)
			if (err == nil) != (execState == Aborted && branchState == Aborted) {
				t.Fatal("advanced unproven old execution", execState, branchState, err)
			}
		}
	}
	in := p.Input()
	in.OperationID += "-generation-2"
	candidate, err := NewPlan(in)
	if err != nil {
		t.Fatal(err)
	}
	if ValidateAdvance(e, candidate, evidence, false) == nil {
		t.Fatal("retry changed immutable plan")
	}
	if err = ValidateAdvance(e, candidate, evidence, true); err != nil {
		t.Fatal(err)
	}
	if ValidateAdvance(e, p, evidence, true) == nil {
		t.Fatal("replan overwrote old operation")
	}
	initial := InitialResult(e)
	if initial.Local != LocalNotStarted || len(initial.Branches) != len(refs) {
		t.Fatal("incorrect initial state")
	}
	for _, b := range initial.Branches {
		if b.Half != HalfNotAttempted || b.Final != FinalNotAttempted {
			t.Fatal("initial state falsely attempted")
		}
	}
	if !DefaultLimits().Valid() {
		t.Fatal("default limits invalid")
	}
	l := DefaultLimits()
	l.Lease = l.RPCTimeout
	if l.Valid() {
		t.Fatal("unsafe lease budget accepted")
	}
	for _, ref := range refs {
		raw, err := EncodeReference(ref)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeReference(raw)
		if err != nil || got != ref {
			t.Fatal("reference roundtrip", err)
		}
		if _, err = DecodeReference(append(raw, []byte("{}")...)); err == nil {
			t.Fatal("trailing reference accepted")
		}
	}
}

func TestInstalledLanguagesShareStoredPlanVectors(t *testing.T) {
	data, err := os.ReadFile("../contracts/transaction-storage-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Version int               `json:"version"`
		Plans   []json.RawMessage `json:"plans"`
		Keys    []struct {
			Reference json.RawMessage `json:"reference"`
			Execution string          `json:"execution_key"`
			Request   string          `json:"request_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	if fixtures.Version != 1 || len(fixtures.Plans) != 2 {
		t.Fatal("storage fixtures required")
	}
	for i, raw := range fixtures.Plans {
		got, err := DecodePlan(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Input(), plans(t)[i].Input()) {
			t.Fatal("Python persisted representation differs")
		}
		ref, err := DecodeReference(fixtures.Keys[i].Reference)
		if err != nil {
			t.Fatal(err)
		}
		ek := ExecutionKey(ref.Owner, ref.OperationID, ref.WriterEpoch, ref.Fence)
		rk := RequestFor(got).KeyDigest()
		if hex.EncodeToString(ek[:]) != fixtures.Keys[i].Execution || hex.EncodeToString(rk[:]) != fixtures.Keys[i].Request {
			t.Fatal("cross-language DB key mismatch")
		}
		encoded, err := EncodePlan(got)
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		if json.Unmarshal(encoded, &a) != nil || json.Unmarshal(raw, &b) != nil || !reflect.DeepEqual(a, b) {
			t.Fatal("Go output differs from shared storage shape")
		}
	}
}

func TestExecutionFenceIsPortableAndEveryBranchMustAgree(t *testing.T) {
	p := plans(t)[1]
	if _, err := p.Reference(p.Input().Branches[0].ID, MaxFence+1, "epoch"); err == nil {
		t.Fatal("Mongo int64 overflow accepted")
	}
	refs := []Reference{}
	for _, b := range p.Input().Branches {
		r, err := p.Reference(b.ID, MaxFence, "epoch")
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, r)
	}
	for _, state := range []State{Prepared, Committed, Aborted} {
		ev := make([]*Evidence, len(refs))
		for i, r := range refs {
			ev[i] = &Evidence{r, state, state}
		}
		want := LocalUnknown
		if state == Committed {
			want = LocalCommitted
		}
		if state == Aborted {
			want = LocalAborted
		}
		if DecideLocal(refs, ev) != want {
			t.Fatal("incorrect terminal classification")
		}
		ev[0] = nil
		if DecideLocal(refs, ev) != LocalUnknown {
			t.Fatal("missing witness accepted")
		}
	}
	if ClassifyHalf(false, false) != HalfUnknown || ClassifyHalf(true, true) != HalfUnknown {
		t.Fatal("ambiguous half accepted")
	}
}

func TestProtocolReferenceEncodeNeverExceedsDecoderBudget(t *testing.T) {
	ref, err := plans(t)[0].Reference("branch-0", 1, "epoch")
	if err != nil {
		t.Fatal(err)
	}
	// Each identity is valid alone, but JSON escaping expands the aggregate.
	ref.Owner = string(bytes.Repeat([]byte{1}, 255))
	ref.Scope = ref.Owner
	ref.OperationID = ref.Owner
	ref.BranchID = ref.Owner
	ref.MessageID = ref.Owner
	ref.LogicalTopic = ref.Owner
	ref.PhysicalTopic = ref.Owner
	ref.RouteRevision = ref.Owner
	ref.WriterEpoch = ref.Owner
	if !ref.Valid() {
		t.Fatal("fixture must exercise aggregate encoding limit")
	}
	if _, err := EncodeReference(ref); err == nil {
		t.Fatal("encoded an undecodable Reference")
	}
}

func TestLeaseLimitsMatchPersistentStoreRange(t *testing.T) {
	for _, duration := range []time.Duration{25 * time.Hour, time.Millisecond / 2} {
		limits := DefaultLimits()
		limits.Lease, limits.RPCTimeout = duration, time.Nanosecond
		if limits.Valid() {
			t.Fatal("lease outside persistent-store range admitted", duration)
		}
	}
}
