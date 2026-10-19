package transaction

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/FangcunMount/reliable-messaging/message"
)

// CodecVersion versions the stored representation, not the business wire.
const CodecVersion = 1
const MaxPlanBytes = 8 << 20

type storedMessage struct {
	Producer      string `json:"producer"`
	ID            string `json:"message_id"`
	Destination   string `json:"destination"`
	EventType     string `json:"event_type"`
	SchemaVersion string `json:"schema_version"`
	Scope         string `json:"scope"`
	ContentType   string `json:"content_type"`
	OccurredAt    string `json:"occurred_at"`
	Payload       []byte `json:"payload"`
}
type storedBranch struct {
	ID            string        `json:"branch_id"`
	PhysicalTopic string        `json:"physical_topic"`
	Revision      string        `json:"route_revision"`
	Message       storedMessage `json:"message"`
	Wire          []byte        `json:"wire"`
}
type storedPlan struct {
	Version   int            `json:"version"`
	Owner     string         `json:"owner"`
	Scope     string         `json:"scope"`
	Operation string         `json:"operation_id"`
	Key       string         `json:"idempotency_key"`
	InputHash string         `json:"input_hash"`
	Digest    string         `json:"plan_digest"`
	Branches  []storedBranch `json:"branches"`
}

// EncodePlan preserves exact message fields and bytes, including timestamp text.
func EncodePlan(p FrozenPlan) ([]byte, error) {
	if !p.Valid() {
		return nil, ErrInvalidPlan
	}
	in := p.Input()
	digest := p.Digest()
	s := storedPlan{Version: CodecVersion, Owner: in.Owner, Scope: in.Scope, Operation: in.OperationID, Key: in.IdempotencyKey, InputHash: hex.EncodeToString(in.InputHash[:]), Digest: hex.EncodeToString(digest[:])}
	for _, b := range in.Branches {
		m := b.Message.Input()
		s.Branches = append(s.Branches, storedBranch{b.ID, b.PhysicalTopic, b.RouteRevision, storedMessage{m.Producer, m.ID, m.Destination, m.EventType, m.SchemaVersion, m.Scope, m.ContentType, m.OccurredAt, m.Payload}, b.Wire})
	}
	data, err := json.Marshal(s)
	if len(data) > MaxPlanBytes {
		return nil, ErrInvalidPlan
	}
	return data, err
}

func DecodePlan(data []byte) (FrozenPlan, error) {
	if len(data) == 0 || len(data) > MaxPlanBytes {
		return FrozenPlan{}, ErrInvalidPlan
	}
	var s storedPlan
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return FrozenPlan{}, fmt.Errorf("decode plan: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return FrozenPlan{}, ErrInvalidPlan
	}
	if s.Version != CodecVersion {
		return FrozenPlan{}, ErrInvalidPlan
	}
	h, err := hex.DecodeString(s.InputHash)
	if err != nil || len(h) != 32 {
		return FrozenPlan{}, ErrInvalidPlan
	}
	in := PlanInput{Owner: s.Owner, Scope: s.Scope, OperationID: s.Operation, IdempotencyKey: s.Key}
	copy(in.InputHash[:], h)
	for _, b := range s.Branches {
		v := b.Message
		m, err := message.New(message.Input{Producer: v.Producer, ID: v.ID, Destination: v.Destination, EventType: v.EventType, SchemaVersion: v.SchemaVersion, Scope: v.Scope, ContentType: v.ContentType, OccurredAt: v.OccurredAt, Payload: v.Payload})
		if err != nil {
			return FrozenPlan{}, err
		}
		in.Branches = append(in.Branches, BranchInput{ID: b.ID, PhysicalTopic: b.PhysicalTopic, RouteRevision: b.Revision, Message: m, Wire: b.Wire})
	}
	p, err := NewPlan(in)
	if err != nil {
		return FrozenPlan{}, err
	}
	digest := p.Digest()
	if s.Digest != hex.EncodeToString(digest[:]) {
		return FrozenPlan{}, ErrInvalidPlan
	}
	return p, nil
}
