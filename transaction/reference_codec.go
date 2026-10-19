package transaction

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
)

type storedReference struct {
	Owner       string `json:"owner"`
	Scope       string `json:"scope"`
	Operation   string `json:"operation_id"`
	Branch      string `json:"branch_id"`
	Message     string `json:"message_id"`
	Logical     string `json:"logical_topic"`
	Physical    string `json:"physical_topic"`
	Revision    string `json:"route_revision"`
	Epoch       string `json:"writer_epoch"`
	Plan        string `json:"plan_digest"`
	Wire        string `json:"wire_hash"`
	Fingerprint string `json:"message_fingerprint"`
	Fence       uint64 `json:"fence"`
}

func EncodeReference(r Reference) ([]byte, error) {
	if !r.Valid() {
		return nil, ErrInvalidPlan
	}
	return json.Marshal(storedReference{r.Owner, r.Scope, r.OperationID, r.BranchID, r.MessageID, r.LogicalTopic, r.PhysicalTopic, r.RouteRevision, r.WriterEpoch, hex.EncodeToString(r.PlanDigest[:]), hex.EncodeToString(r.WireHash[:]), hex.EncodeToString(r.MessageFingerprint[:]), r.Fence})
}
func DecodeReference(data []byte) (Reference, error) {
	if len(data) == 0 || len(data) > 8192 {
		return Reference{}, ErrInvalidPlan
	}
	var s storedReference
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return Reference{}, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Reference{}, ErrInvalidPlan
	}
	r := Reference{Owner: s.Owner, Scope: s.Scope, OperationID: s.Operation, BranchID: s.Branch, MessageID: s.Message, LogicalTopic: s.Logical, PhysicalTopic: s.Physical, RouteRevision: s.Revision, WriterEpoch: s.Epoch, Fence: s.Fence}
	for _, v := range []struct {
		source string
		dest   *[32]byte
	}{{s.Plan, &r.PlanDigest}, {s.Wire, &r.WireHash}, {s.Fingerprint, &r.MessageFingerprint}} {
		h, err := hex.DecodeString(v.source)
		if err != nil || len(h) != 32 {
			return Reference{}, ErrInvalidPlan
		}
		copy(v.dest[:], h)
	}
	if !r.Valid() {
		return Reference{}, ErrInvalidPlan
	}
	return r, nil
}
func ExecutionKey(owner, operation, epoch string, fence uint64) [32]byte {
	h := sha256.New()
	h.Write([]byte("rm-tx-execution-v1\x00"))
	for _, v := range []string{owner, operation, epoch} {
		field(h, []byte(v))
	}
	var f [8]byte
	for i := 7; i >= 0; i-- {
		f[i] = byte(fence)
		fence >>= 8
	}
	field(h, f[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func BranchKey(id string) [32]byte { return sha256.Sum256([]byte(id)) }
