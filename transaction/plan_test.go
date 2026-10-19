package transaction

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/FangcunMount/reliable-messaging/message"
)

func plans(t *testing.T) []FrozenPlan {
	t.Helper()
	data, err := os.ReadFile("../contracts/transaction-plans-draft-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version int
		Plans   []struct {
			Owner, Scope string
			OperationID  string `json:"operation_id"`
			Key          string `json:"idempotency_key"`
			InputHash    string `json:"input_hash"`
			Digest       string `json:"plan_digest"`
			Branches     []struct {
				ID            string
				PhysicalTopic string `json:"physical_topic"`
				Revision      string `json:"route_revision"`
				Message       message.Input
				Wire          []byte
				Fingerprint   string `json:"message_fingerprint"`
				WireHash      string `json:"wire_hash"`
			}
		}
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || len(fixture.Plans) != 2 {
		t.Fatal("required contract vectors missing")
	}
	var result []FrozenPlan
	for _, vector := range fixture.Plans {
		in := PlanInput{Owner: vector.Owner, Scope: vector.Scope, OperationID: vector.OperationID, IdempotencyKey: vector.Key}
		inputHash, err := hex.DecodeString(vector.InputHash)
		if err != nil || len(inputHash) != 32 {
			t.Fatal("invalid vector hash")
		}
		copy(in.InputHash[:], inputHash)
		for _, branch := range vector.Branches {
			m, err := message.New(branch.Message)
			if err != nil {
				t.Fatal(err)
			}
			fp := m.Fingerprint()
			if hex.EncodeToString(fp[:]) != branch.Fingerprint {
				t.Fatal("business fingerprint drift")
			}
			in.Branches = append(in.Branches, BranchInput{ID: branch.ID, PhysicalTopic: branch.PhysicalTopic, RouteRevision: branch.Revision, Message: m, Wire: branch.Wire})
		}
		p, err := NewPlan(in)
		if err != nil {
			t.Fatal(err)
		}
		digest := p.Digest()
		if hex.EncodeToString(digest[:]) != vector.Digest {
			t.Fatal("cross-language plan digest differs")
		}
		for i, branch := range vector.Branches {
			ref, err := p.Reference(branch.ID, 1, "writer-epoch-a")
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(ref.WireHash[:]) != branch.WireHash || ref.MessageFingerprint != in.Branches[i].Message.Fingerprint() {
				t.Fatal("reference did not preserve both wire and business fingerprints")
			}
		}
		result = append(result, p)
	}
	return result
}

func TestFrozenPlansShareGoPythonContractVectors(t *testing.T) { plans(t) }

func TestPlanOwnsFirstWireAndRequestLookupIgnoresNewGeneratedIdentity(t *testing.T) {
	first := plans(t)[0]
	in := first.Input()
	p, err := NewPlan(in)
	if err != nil {
		t.Fatal(err)
	}
	original := p.Input()
	in.Branches[0].Wire[0] ^= 255
	in.Branches[0].ID = "new-branch"
	if !reflect.DeepEqual(p.Input(), original) {
		t.Fatal("caller mutated reserved plan")
	}
	copy := p.Input()
	copy.Branches[0].Wire[0] ^= 255
	if !reflect.DeepEqual(p.Input(), original) {
		t.Fatal("accessor exposed mutable wire")
	}
	if err := p.MatchRequest(original.Owner, original.Scope, original.IdempotencyKey, original.InputHash); err != nil {
		t.Fatal(err)
	}
	changedInput := original.InputHash
	changedInput[0] ^= 1
	if !errors.Is(p.MatchRequest(original.Owner, original.Scope, original.IdempotencyKey, changedInput), ErrInputConflict) {
		t.Fatal("same key/different input accepted")
	}
	for _, identity := range [][3]string{{"other", original.Scope, original.IdempotencyKey}, {original.Owner, "other", original.IdempotencyKey}, {original.Owner, original.Scope, "other"}} {
		if !errors.Is(p.MatchRequest(identity[0], identity[1], identity[2], original.InputHash), ErrRequestIdentity) {
			t.Fatal("different request identity accepted")
		}
	}
	// Route changes affect the protocol plan, never the original business ID/FP.
	copy = p.Input()
	copy.Branches[0].RouteRevision = "next"
	next, err := NewPlan(copy)
	if err != nil {
		t.Fatal(err)
	}
	if next.Digest() == p.Digest() || next.Input().Branches[0].Message.Fingerprint() != original.Branches[0].Message.Fingerprint() {
		t.Fatal("route/business identity conflated")
	}
}

func TestPlanRejectsEmptyDuplicateAndCrossScopeBranches(t *testing.T) {
	base := plans(t)[0].Input()
	for _, mutate := range []func(*PlanInput){
		func(p *PlanInput) { p.Owner = "" }, func(p *PlanInput) { p.OperationID = "" }, func(p *PlanInput) { p.IdempotencyKey = "" },
		func(p *PlanInput) { p.Branches = nil }, func(p *PlanInput) { p.Branches = append(p.Branches, p.Branches[0]) },
		func(p *PlanInput) { p.Scope = "another-org" }, func(p *PlanInput) { p.Branches[0].Wire = nil },
		func(p *PlanInput) { p.Branches[0].Message = message.Message{} },
	} {
		candidate := clone(base)
		mutate(&candidate)
		if _, err := NewPlan(candidate); !errors.Is(err, ErrInvalidPlan) {
			t.Fatal("invalid plan accepted")
		}
	}
}

func TestOnlyExactMatchingTerminalEvidenceResolves(t *testing.T) {
	p := plans(t)[0]
	ref, err := p.Reference(p.Input().Branches[0].ID, 7, "epoch-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []State{Prepared, Committed, Aborted, "unexpected"} {
		for _, branch := range []State{Prepared, Committed, Aborted, "unexpected"} {
			want := Unknown
			if operation == Committed && branch == Committed {
				want = Commit
			}
			if operation == Aborted && branch == Aborted {
				want = Rollback
			}
			if got := Resolve(ref, &Evidence{Reference: ref, ExecutionState: operation, BranchState: branch}); got != want {
				t.Fatalf("%s/%s resolved %s, want %s", operation, branch, got, want)
			}
		}
	}
	if Resolve(ref, nil) != Unknown || Resolve(Reference{}, &Evidence{}) != Unknown {
		t.Fatal("missing evidence not unknown")
	}
	proof := &Evidence{Reference: ref, ExecutionState: Committed, BranchState: Committed}
	for i := 0; i < reflect.TypeOf(ref).NumField(); i++ {
		forged := ref
		f := reflect.ValueOf(&forged).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "-wrong")
		case reflect.Uint64:
			f.SetUint(f.Uint() + 1)
		case reflect.Array:
			f.Index(0).SetUint(f.Index(0).Uint() ^ 1)
		default:
			t.Fatal("uncovered reference field")
		}
		if Resolve(forged, proof) != Unknown {
			t.Fatalf("unbound reference field %s", reflect.TypeOf(ref).Field(i).Name)
		}
	}
}

func TestReferenceRejectsUnreservedBranchAndZeroFence(t *testing.T) {
	if _, err := plans(t)[0].Reference("not-a-branch", 1, "epoch"); err == nil {
		t.Fatal("unreserved branch accepted")
	}
	if _, err := plans(t)[0].Reference("branch-0", 0, "epoch"); err == nil {
		t.Fatal("zero fence accepted")
	}
}

func TestNewFenceReusesFrozenIdentityWithoutReopeningOldTerminal(t *testing.T) {
	p := plans(t)[0]
	oldRef, err := p.Reference("branch-0", 1, "epoch")
	if err != nil {
		t.Fatal(err)
	}
	newRef, err := p.Reference("branch-0", 2, "epoch")
	if err != nil {
		t.Fatal(err)
	}
	oldProof := &Evidence{Reference: oldRef, ExecutionState: Aborted, BranchState: Aborted}
	newProof := &Evidence{Reference: newRef, ExecutionState: Committed, BranchState: Committed}
	if oldRef.PlanDigest != newRef.PlanDigest || oldRef.WireHash != newRef.WireHash || oldRef.MessageID != newRef.MessageID {
		t.Fatal("new fence changed frozen business identity")
	}
	if Resolve(oldRef, oldProof) != Rollback || Resolve(newRef, newProof) != Commit || Resolve(oldRef, newProof) != Unknown || Resolve(newRef, oldProof) != Unknown {
		t.Fatal("execution terminal evidence crossed fences")
	}
}
