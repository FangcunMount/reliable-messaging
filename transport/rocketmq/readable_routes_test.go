package rocketmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
)

// This proves orchestration and isolation; real Broker/storage are separate gates.
func TestReadableReplanPreservesWireAndNeverExecutesRetiredRoutes(t *testing.T) {
	for _, state := range []transaction.State{transaction.Prepared, transaction.Committed, transaction.Aborted} {
		t.Run(string(state), func(t *testing.T) {
			previous, base := transactionFixture(t, 1)
			original := previous.Plan
			base.state = state
			store := &authorizedMemoryStore{transactionMemoryStore: base}
			physical, err := transport.PhysicalTopicV3("logical", "TRANSACTION", "test.mysql", "v2")
			if err != nil {
				t.Fatal(err)
			}
			active := TransactionRoute{LogicalTopic: "logical", PhysicalTopic: physical, Revision: "v2", NamingScheme: transport.ReadableNamingV1}
			history := TransactionRoute{LogicalTopic: "logical", PhysicalTopic: transactionTestTopic(t), Revision: "v2"}
			s, err := NewTransactionalSender(TransactionConfig{Endpoint: "127.0.0.1:8081", AccessKey: "fixture", SecretKey: "fixture", Owner: "test.mysql", Routes: []TransactionRoute{active}, HistoricalRoutes: []TransactionRoute{history}}, store)
			if err != nil {
				t.Fatal(err)
			}
			backend := &transactionFakeBackend{}
			s.factory = func(func(func()) bool, func(context.Context, *driver.MessageView) driver.TransactionResolution) (transactionBackend, error) {
				return backend, nil
			}
			if err = s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := s.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			if _, err = s.Retry(context.Background(), transaction.RetryAuthorization{Previous: previous}); !errors.Is(err, transaction.ErrBinding) {
				t.Fatal("retired Retry admitted", err)
			}
			if _, err = s.Execute(context.Background(), previous, func(context.Context, transaction.Execution) (transaction.LocalOutcome, error) {
				t.Error("retired business invoked")
				return transaction.LocalCommitted, nil
			}); !errors.Is(err, transaction.ErrBinding) {
				t.Fatal("retired Execute admitted", err)
			}
			input := previous.Plan.Input()
			input.OperationID = "explicit-route-handoff"
			input.Branches[0].PhysicalTopic = physical
			candidate, err := transaction.NewPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			if original.Input().Branches[0].Message.Fingerprint() != candidate.Input().Branches[0].Message.Fingerprint() || string(original.Input().Branches[0].Wire) != string(candidate.Input().Branches[0].Wire) || original.Digest() == candidate.Digest() {
				t.Fatal("route migration changed message or retained old plan digest")
			}
			result, err := s.Replan(context.Background(), transaction.ReplanAuthorization{Previous: previous, Candidate: candidate})
			if state != transaction.Aborted {
				if !errors.Is(err, transaction.ErrStaleExecution) {
					t.Fatal("non-Aborted handoff admitted", err)
				}
			} else if err != nil || !result.Acquired || result.Execution.Plan.Digest() != candidate.Digest() {
				t.Fatal("explicit aborted handoff rejected", err)
			}
			if backend.prepares.Load() != 0 || original.Digest() != previous.Plan.Digest() {
				t.Fatal("handoff sent or rewrote evidence")
			}
			ref := previous.References()[0]
			raw, _ := transaction.EncodeReference(ref)
			if s.checkMessage(context.Background(), ref.PhysicalTopic, raw, original.Input().Branches[0].Wire) != driver.UNKNOWN {
				t.Fatal("historical route acquired active Checker")
			}
		})
	}
}

func TestReadableSubscriberNeverRegistersHistoricalInputs(t *testing.T) {
	group, _ := transport.ConsumerGroupV3("qs.events", "worker")
	var active, history []transport.RouteBinding
	for _, owner := range []string{"qs.mysql", "qs.mongo"} {
		physical, _ := transport.PhysicalTopicV3("qs.events", "TRANSACTION", owner, "m3-1")
		old, _ := transport.PhysicalTopicV2("qs.events", "TRANSACTION", owner, "m3-1")
		oldGroup, _ := transport.ConsumerGroupV2("qs.events", "worker")
		active = append(active, transport.RouteBinding{Topic: "qs.events", Subscription: "worker", PhysicalTopic: physical, ConsumerGroup: group, MessageType: "TRANSACTION", Owner: owner, RouteRevision: "m3-1", NamingScheme: transport.ReadableNamingV1})
		history = append(history, transport.RouteBinding{Topic: "qs.events", Subscription: "worker", PhysicalTopic: old, ConsumerGroup: oldGroup, MessageType: "TRANSACTION", Owner: owner, RouteRevision: "m3-1"})
	}
	s, err := NewSubscriber(SubscriberConfig{Config: Config{Endpoint: "127.0.0.1:1", AccessKey: "fixture", SecretKey: "fixture", Routes: active, HistoricalRoutes: history, MaxInFlight: 2, RequestTimeout: time.Second}, MaxAttempts: 8, InvisibleDuration: 15 * time.Second, HandlerTimeout: time.Second, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	inputs, route, err := s.subscriptionRoutes("qs.events", "worker")
	if err != nil || len(inputs) != 2 || route.ConsumerGroup != group || cap(s.slots) != 2 || len(s.running) != 0 {
		t.Fatal("history registered or budget amplified", err)
	}
	for _, r := range history {
		if _, ok := inputs[r.PhysicalTopic]; ok {
			t.Fatal("historical topic subscribed")
		}
	}
}
