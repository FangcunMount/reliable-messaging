// Package transaction defines provisional transaction-message contracts.
// It performs no I/O and does not own a host database transaction. These APIs
// are not frozen until the RocketMQ M2 original MySQL/Mongo proofs pass.
package transaction

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"unicode/utf8"

	"github.com/FangcunMount/reliable-messaging/message"
)

var (
	ErrInvalidPlan     = errors.New("invalid frozen transaction plan")
	ErrRequestIdentity = errors.New("transaction request identity differs")
	ErrInputConflict   = errors.New("transaction idempotency key has different business input")
)

// BranchInput contains the original application message and its already frozen
// wire bytes. PhysicalTopic and RouteRevision come from the trusted manifest;
// changing a broker route does not change Message.Fingerprint().
type BranchInput struct {
	ID, PhysicalTopic, RouteRevision string
	Message                          message.Message
	Wire                             []byte
}

type PlanInput struct {
	Owner, Scope, OperationID, IdempotencyKey string
	InputHash                                 [32]byte
	Branches                                  []BranchInput
}

type FrozenPlan struct {
	input  PlanInput
	digest [32]byte
}

func required(values ...string) bool {
	for _, value := range values {
		if value == "" || !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func clone(in PlanInput) PlanInput {
	in.Branches = append([]BranchInput(nil), in.Branches...)
	for i := range in.Branches {
		in.Branches[i].Wire = append([]byte(nil), in.Branches[i].Wire...)
	}
	return in
}

func field(h hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	h.Write(size[:])
	h.Write(value)
}

func NewPlan(in PlanInput) (FrozenPlan, error) {
	if !required(in.Owner, in.Scope, in.OperationID, in.IdempotencyKey) || len(in.Branches) == 0 {
		return FrozenPlan{}, ErrInvalidPlan
	}
	ids := make(map[string]bool, len(in.Branches))
	for _, branch := range in.Branches {
		if !required(branch.ID, branch.PhysicalTopic, branch.RouteRevision) || ids[branch.ID] || !branch.Message.Valid() || len(branch.Wire) == 0 || branch.Message.Input().Scope != in.Scope {
			return FrozenPlan{}, ErrInvalidPlan
		}
		ids[branch.ID] = true
	}
	in = clone(in)
	h := sha256.New()
	h.Write([]byte("rm-tx-plan-draft-v1\x00"))
	for _, value := range []string{in.Owner, in.Scope, in.OperationID, in.IdempotencyKey} {
		field(h, []byte(value))
	}
	field(h, in.InputHash[:])
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(in.Branches)))
	field(h, count[:])
	for _, branch := range in.Branches {
		fingerprint, wireHash := branch.Message.Fingerprint(), sha256.Sum256(branch.Wire)
		field(h, []byte(branch.ID))
		field(h, fingerprint[:])
		field(h, []byte(branch.PhysicalTopic))
		field(h, []byte(branch.RouteRevision))
		field(h, wireHash[:])
	}
	plan := FrozenPlan{input: in}
	copy(plan.digest[:], h.Sum(nil))
	return plan, nil
}

func (p FrozenPlan) Valid() bool      { return p.input.Owner != "" }
func (p FrozenPlan) Input() PlanInput { return clone(p.input) }
func (p FrozenPlan) Digest() [32]byte { return p.digest }

// MatchRequest is used on the durable lookup/unique-key winner BEFORE creating
// new IDs, timestamps or ciphertext. Only stable business input determines a
// conflict; a retry must reuse the first complete plan, including its first wire.
func (p FrozenPlan) MatchRequest(owner, scope, key string, inputHash [32]byte) error {
	if !p.Valid() || p.input.Owner != owner || p.input.Scope != scope || p.input.IdempotencyKey != key {
		return ErrRequestIdentity
	}
	if p.input.InputHash != inputHash {
		return ErrInputConflict
	}
	return nil
}
