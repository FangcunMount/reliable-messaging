package mongo

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Store struct {
	db      *driver.Database
	binding transaction.Binding
}

var _ transaction.AuthorizedStore = (*Store)(nil)

type branchDoc struct {
	Reference []byte            `bson:"reference"`
	State     transaction.State `bson:"state"`
}
type executionDoc struct {
	ID             []byte            `bson:"_id"`
	Request        []byte            `bson:"request_key"`
	Owner          string            `bson:"owner"`
	Plan           []byte            `bson:"plan"`
	Digest         []byte            `bson:"plan_digest"`
	Fence          int64             `bson:"fence"`
	Epoch          string            `bson:"writer_epoch"`
	Token          []byte            `bson:"token"`
	Lease          time.Time         `bson:"lease_until"`
	State          transaction.State `bson:"state"`
	Version        int64             `bson:"guard_version"`
	Branches       []branchDoc       `bson:"witnesses"`
	PrepareStarted bool              `bson:"prepare_started"`
}
type requestDoc struct {
	ID      []byte `bson:"_id"`
	Owner   string `bson:"owner"`
	Scope   string `bson:"scope"`
	Key     string `bson:"idempotency_key"`
	Input   []byte `bson:"input_hash"`
	Active  []byte `bson:"active_execution"`
	Version int64  `bson:"version"`
}

func New(db *driver.Database, b transaction.Binding) (*Store, error) {
	if db == nil || !b.Valid() {
		return nil, transaction.ErrBinding
	}
	primary := db.Client().Database(db.Name(), options.Database().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Majority()).SetWriteConcern(writeconcern.Majority()))
	return &Store{primary, b}, nil
}
func key(e transaction.Execution) [32]byte {
	return transaction.ExecutionKey(e.Plan.Input().Owner, e.Plan.Input().OperationID, e.WriterEpoch, e.Fence)
}
func (s *Store) bindingIn(ctx context.Context, writer bool) error {
	var b struct {
		Store   string `bson:"store_id"`
		Epoch   string `bson:"writer_epoch"`
		Version int    `bson:"schema_version"`
	}
	if err := s.db.Collection("rm_tx_binding").FindOne(ctx, bson.M{"_id": s.binding.Owner}).Decode(&b); err != nil {
		return err
	}
	if b.Store != s.binding.StoreID || (writer && b.Epoch != s.binding.WriterEpoch) {
		return transaction.ErrBinding
	}
	if b.Version != SchemaVersion {
		return transaction.ErrSchema
	}
	return nil
}
func (s *Store) protocolTx(ctx context.Context, fn func(driver.SessionContext) (transaction.Reservation, error)) (transaction.Reservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	session, err := s.db.Client().StartSession()
	if err != nil {
		return transaction.Reservation{}, err
	}
	defer session.EndSession(ctx)
	result, err := session.WithTransaction(ctx, func(sc driver.SessionContext) (any, error) { return fn(sc) }, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary()))
	if err != nil {
		return transaction.Reservation{}, err
	}
	r, ok := result.(transaction.Reservation)
	if !ok {
		return r, transaction.ErrInvalidPlan
	}
	return r, nil
}
func (s *Store) decode(d executionDoc, capability bool) (transaction.Reservation, error) {
	p, err := transaction.DecodePlan(d.Plan)
	if err != nil {
		return transaction.Reservation{}, err
	}
	digest := p.Digest()
	if d.Fence <= 0 || !bytes.Equal(d.Digest, digest[:]) || p.Input().Owner != s.binding.Owner || len(d.Token) != 32 {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	e := transaction.Execution{Plan: p, Fence: uint64(d.Fence), WriterEpoch: d.Epoch, LeaseUntil: d.Lease}
	copy(e.Token[:], d.Token)
	if !e.Valid() {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	ek := key(e)
	if !bytes.Equal(ek[:], d.ID) {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	if !capability {
		e.Token = [32]byte{}
	}
	return transaction.Reservation{Execution: e, State: d.State}, nil
}
func (s *Store) lookupIn(ctx context.Context, r transaction.Request) (transaction.Reservation, error) {
	rk := r.KeyDigest()
	var root requestDoc
	if err := s.db.Collection("rm_tx_request").FindOne(ctx, bson.M{"_id": rk[:]}).Decode(&root); err != nil {
		return transaction.Reservation{}, err
	}
	if root.Owner != r.Owner || root.Scope != r.Scope || root.Key != r.Key {
		return transaction.Reservation{}, transaction.ErrRequestIdentity
	}
	if !bytes.Equal(root.Input, r.InputHash[:]) {
		return transaction.Reservation{}, transaction.ErrInputConflict
	}
	var d executionDoc
	if err := s.db.Collection("rm_tx_execution").FindOne(ctx, bson.M{"_id": root.Active}).Decode(&d); err != nil {
		return transaction.Reservation{}, err
	}
	result, err := s.decode(d, false)
	if err != nil {
		return result, err
	}
	return result, r.Match(result.Execution.Plan)
}
func (s *Store) Lookup(ctx context.Context, r transaction.Request) (transaction.Reservation, error) {
	if !r.Valid() || r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	result, err := s.protocolTx(ctx, func(sc driver.SessionContext) (transaction.Reservation, error) {
		if err := s.bindingIn(sc, false); err != nil {
			return transaction.Reservation{}, err
		}
		return s.lookupIn(sc, r)
	})
	if errors.Is(err, driver.ErrNoDocuments) {
		err = transaction.ErrNotFound
	}
	return result, err
}
func (s *Store) newDoc(ctx context.Context, p transaction.FrozenPlan, fence uint64, o transaction.ReserveOptions) (executionDoc, transaction.Execution, error) {
	e := transaction.Execution{Plan: p, Fence: fence, WriterEpoch: s.binding.WriterEpoch}
	if _, err := rand.Read(e.Token[:]); err != nil {
		return executionDoc{}, e, err
	}
	cursor, err := s.db.Collection("rm_tx_binding").Aggregate(ctx, driver.Pipeline{bson.D{{Key: "$match", Value: bson.M{"_id": s.binding.Owner}}}, bson.D{{Key: "$project", Value: bson.M{"now": "$$NOW"}}}})
	if err != nil {
		return executionDoc{}, e, err
	}
	defer cursor.Close(ctx)
	if !cursor.Next(ctx) {
		return executionDoc{}, e, transaction.ErrBinding
	}
	var clock struct {
		Now time.Time `bson:"now"`
	}
	if err = cursor.Decode(&clock); err != nil {
		return executionDoc{}, e, err
	}
	e.LeaseUntil = clock.Now.Add(o.Lease).Truncate(time.Millisecond)
	raw, err := transaction.EncodePlan(p)
	if err != nil {
		return executionDoc{}, e, err
	}
	ek, rk, digest := key(e), transaction.RequestFor(p).KeyDigest(), p.Digest()
	d := executionDoc{ID: ek[:], Request: rk[:], Owner: p.Input().Owner, Plan: raw, Digest: digest[:], Fence: int64(fence), Epoch: e.WriterEpoch, Token: e.Token[:], Lease: e.LeaseUntil, State: transaction.Prepared}
	for _, ref := range e.References() {
		raw, err := transaction.EncodeReference(ref)
		if err != nil {
			return d, e, err
		}
		d.Branches = append(d.Branches, branchDoc{raw, transaction.Prepared})
	}
	return d, e, nil
}
func (s *Store) Reserve(ctx context.Context, r transaction.Request, p transaction.FrozenPlan, o transaction.ReserveOptions) (transaction.Reservation, error) {
	if !o.Valid() || !p.Valid() || r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	if err := r.Match(p); err != nil {
		return transaction.Reservation{}, err
	}
	if err := s.ValidateSchema(ctx); err != nil {
		return transaction.Reservation{}, err
	}
	result, err := s.protocolTx(ctx, func(sc driver.SessionContext) (transaction.Reservation, error) {
		if err := s.bindingIn(sc, true); err != nil {
			return transaction.Reservation{}, err
		}
		current, err := s.lookupIn(sc, r)
		if err == nil {
			return current, nil
		}
		if !errors.Is(err, driver.ErrNoDocuments) {
			return current, err
		}
		d, e, err := s.newDoc(sc, p, 1, o)
		if err != nil {
			return transaction.Reservation{}, err
		}
		rk := r.KeyDigest()
		if _, err = s.db.Collection("rm_tx_request").InsertOne(sc, requestDoc{ID: rk[:], Owner: r.Owner, Scope: r.Scope, Key: r.Key, Input: r.InputHash[:], Active: d.ID}); err != nil {
			return transaction.Reservation{}, err
		}
		if _, err = s.db.Collection("rm_tx_execution").InsertOne(sc, d); err != nil {
			return transaction.Reservation{}, err
		}
		return transaction.Reservation{Execution: e, Acquired: true, State: transaction.Prepared}, nil
	})
	if driver.IsDuplicateKeyError(err) {
		return s.Lookup(ctx, r)
	}
	return result, err
}
func (s *Store) Evidence(ctx context.Context, ref transaction.Reference) (*transaction.Evidence, error) {
	if !ref.Valid() || ref.Owner != s.binding.Owner {
		return nil, transaction.ErrBinding
	}
	if err := s.bindingIn(ctx, false); err != nil {
		return nil, err
	}
	ek := transaction.ExecutionKey(ref.Owner, ref.OperationID, ref.WriterEpoch, ref.Fence)
	var d executionDoc
	if err := s.db.Collection("rm_tx_execution").FindOne(ctx, bson.M{"_id": ek[:]}).Decode(&d); err != nil {
		if errors.Is(err, driver.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	for _, branch := range d.Branches {
		actual, err := transaction.DecodeReference(branch.Reference)
		if err != nil {
			return nil, err
		}
		if actual == ref {
			return &transaction.Evidence{Reference: actual, ExecutionState: d.State, BranchState: branch.State}, nil
		}
	}
	return nil, nil
}

func (s *Store) BeginPrepare(ctx context.Context, e transaction.Execution) error {
	if !e.Valid() || e.Plan.Input().Owner != s.binding.Owner || e.WriterEpoch != s.binding.WriterEpoch {
		return transaction.ErrBinding
	}
	if err := s.bindingIn(ctx, true); err != nil {
		return err
	}
	ek, digest := key(e), e.Plan.Digest()
	res, err := s.db.Collection("rm_tx_execution").UpdateOne(ctx, bson.M{"_id": ek[:], "state": transaction.Prepared, "token": e.Token[:], "plan_digest": digest[:], "prepare_started": false, "$expr": bson.M{"$gt": bson.A{"$lease_until", "$$NOW"}}}, bson.M{"$set": bson.M{"prepare_started": true}})
	if err != nil {
		return err
	}
	if res.ModifiedCount != 1 {
		return transaction.ErrStaleExecution
	}
	return nil
}
func (s *Store) Abort(ctx context.Context, e transaction.Execution) (bool, error) {
	return s.abort(ctx, e, false)
}
func (s *Store) abort(ctx context.Context, e transaction.Execution, expired bool) (bool, error) {
	if !e.Valid() || e.Plan.Input().Owner != s.binding.Owner {
		return false, transaction.ErrBinding
	}
	if err := s.bindingIn(ctx, false); err != nil {
		return false, err
	}
	ek, digest := key(e), e.Plan.Digest()
	filter := bson.M{"_id": ek[:], "state": transaction.Prepared, "token": e.Token[:], "plan_digest": digest[:]}
	var expected []branchDoc
	for _, ref := range e.References() {
		raw, err := transaction.EncodeReference(ref)
		if err != nil {
			return false, err
		}
		expected = append(expected, branchDoc{raw, transaction.Prepared})
	}
	filter["witnesses"] = expected
	if expired {
		filter["$expr"] = bson.M{"$lte": bson.A{"$lease_until", "$$NOW"}}
	}
	result, err := s.db.Collection("rm_tx_execution").UpdateOne(ctx, filter, bson.M{"$set": bson.M{"state": transaction.Aborted, "witnesses.$[].state": transaction.Aborted}, "$inc": bson.M{"guard_version": 1}})
	if err != nil {
		return false, err
	}
	return result.ModifiedCount == 1, nil
}
func (s *Store) ReconcileOnce(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, transaction.ErrInvalidPlan
	}
	if err := s.bindingIn(ctx, false); err != nil {
		return 0, err
	}
	cur, err := s.db.Collection("rm_tx_execution").Find(ctx, bson.M{"owner": s.binding.Owner, "state": transaction.Prepared, "$expr": bson.M{"$lte": bson.A{"$lease_until", "$$NOW"}}}, options.Find().SetSort(bson.D{{Key: "lease_until", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return 0, err
	}
	var docs []executionDoc
	if err = cur.All(ctx, &docs); err != nil {
		return 0, err
	}
	n := 0
	for _, d := range docs {
		r, err := s.decode(d, true)
		if err != nil {
			return n, err
		}
		ok, err := s.abort(ctx, r.Execution, true)
		if err != nil {
			return n, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}
func (s *Store) Retry(ctx context.Context, a transaction.RetryAuthorization, o transaction.ReserveOptions) (transaction.Reservation, error) {
	return s.advance(ctx, a.Previous, a.Previous.Plan, o, false)
}
func (s *Store) Replan(ctx context.Context, a transaction.ReplanAuthorization, o transaction.ReserveOptions) (transaction.Reservation, error) {
	return s.advance(ctx, a.Previous, a.Candidate, o, true)
}
func (s *Store) advance(ctx context.Context, previous transaction.Execution, p transaction.FrozenPlan, o transaction.ReserveOptions, replan bool) (transaction.Reservation, error) {
	if !previous.Plan.Valid() || !p.Valid() || !o.Valid() {
		return transaction.Reservation{}, transaction.ErrInvalidPlan
	}
	r := transaction.RequestFor(previous.Plan)
	if r.Owner != s.binding.Owner {
		return transaction.Reservation{}, transaction.ErrBinding
	}
	return s.protocolTx(ctx, func(sc driver.SessionContext) (transaction.Reservation, error) {
		if err := s.bindingIn(sc, true); err != nil {
			return transaction.Reservation{}, err
		}
		current, err := s.lookupIn(sc, r)
		if err != nil {
			return current, err
		}
		if key(current.Execution) != key(previous) || current.Execution.Plan.Digest() != previous.Plan.Digest() {
			return transaction.Reservation{}, transaction.ErrStaleExecution
		}
		var evidence []*transaction.Evidence
		for _, ref := range previous.References() {
			e, err := s.Evidence(sc, ref)
			if err != nil {
				return current, err
			}
			evidence = append(evidence, e)
		}
		if err = transaction.ValidateAdvance(previous, p, evidence, replan); err != nil {
			return current, err
		}
		d, e, err := s.newDoc(sc, p, previous.Fence+1, o)
		if err != nil {
			return current, err
		}
		if _, err = s.db.Collection("rm_tx_execution").InsertOne(sc, d); err != nil {
			return current, err
		}
		rk, old := r.KeyDigest(), key(previous)
		res, err := s.db.Collection("rm_tx_request").UpdateOne(sc, bson.M{"_id": rk[:], "active_execution": old[:]}, bson.M{"$set": bson.M{"active_execution": d.ID}, "$inc": bson.M{"version": 1}})
		if err != nil {
			return current, err
		}
		if res.ModifiedCount != 1 {
			return current, transaction.ErrStaleExecution
		}
		return transaction.Reservation{Execution: e, Acquired: true, State: transaction.Prepared}, nil
	})
}
