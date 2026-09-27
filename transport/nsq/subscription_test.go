package nsq

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	driver "github.com/nsqio/go-nsq"
)

type deliveryDelegate struct {
	finished int
	requeued int
	delay    time.Duration
	backoff  bool
}

func (d *deliveryDelegate) OnFinish(*driver.Message) { d.finished++ }
func (d *deliveryDelegate) OnRequeue(_ *driver.Message, delay time.Duration, backoff bool) {
	d.requeued++
	d.delay, d.backoff = delay, backoff
}
func (d *deliveryDelegate) OnTouch(*driver.Message) {}

type handoffProbe struct {
	readyErr error
	result   transport.Result
	ready    int
	pub      int
	address  string
	topic    string
	body     []byte
}

func (p *handoffProbe) Ready(_ context.Context, address, topic string) error {
	p.ready++
	p.address, p.topic = address, topic
	return p.readyErr
}
func (p *handoffProbe) Publish(_ context.Context, address, topic string, body []byte) transport.Result {
	p.pub++
	p.address, p.topic = address, topic
	p.body = append([]byte(nil), body...)
	return p.result
}

func rawMessage(body []byte, attempts uint16) (*driver.Message, *deliveryDelegate) {
	var id driver.MessageID
	copy(id[:], "0123456789abcdef")
	raw := driver.NewMessage(id, body)
	raw.Attempts = attempts
	raw.NSQDAddress = "nsqd:4150"
	delegate := &deliveryDelegate{}
	raw.Delegate = delegate
	return raw, delegate
}

func testSubscription(t *testing.T, handler transport.Handler, failed func(context.Context, legacy.FailedHandoff) error, handoff *handoffProbe) *Subscription {
	t.Helper()
	s, err := NewSubscription(SubscriptionConfig{
		Topic: "qs.evaluation.lifecycle", Channel: "qs-worker", MaxAttempts: 2,
		Retry:   Backoff{BaseDelay: time.Second, MaxDelay: time.Minute, JitterFraction: 0},
		Handler: handler, FailedHandler: failed, Handoff: handoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBusinessHandlerPreservesIdentitiesAndExplicitAck(t *testing.T) {
	const payload = `{"event":"提交"}`
	body, err := legacy.Encode(legacy.Envelope{UUID: "app-uuid", Metadata: map[string]string{"event_type": "answersheet.submitted"}, Payload: []byte(payload)}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	s := testSubscription(t, func(_ context.Context, d transport.Delivery) error {
		called++
		m := d.Message()
		if m.ID != "app-uuid" || m.TransportID != "0123456789abcdef" || m.Attempts != 1 || string(m.Payload) != payload || m.Metadata["event_type"] != "answersheet.submitted" {
			t.Fatalf("wrong delivery: %+v", m)
		}
		m.Payload[0] = 'x'
		m.Metadata["event_type"] = "changed"
		if next := d.Message(); string(next.Payload) != payload || next.Metadata["event_type"] != "answersheet.submitted" {
			t.Fatalf("delivery mutated through accessor: %+v", next)
		}
		if err := d.Ack(); err != nil {
			return err
		}
		return errors.New("handler error after explicit ack")
	}, func(context.Context, legacy.FailedHandoff) error { return nil }, &handoffProbe{result: transport.Result{Outcome: transport.Confirmed}})
	raw, delegate := rawMessage(body, 1)
	if err := s.BusinessHandler(context.Background()).HandleMessage(raw); err != nil {
		t.Fatal(err)
	}
	if called != 1 || delegate.finished != 1 || delegate.requeued != 0 || !raw.IsAutoResponseDisabled() {
		t.Fatalf("unexpected settlement: called=%d delegate=%+v", called, delegate)
	}
	base := driver.NewConfig()
	base.MaxAttempts = 5
	copied := s.ConsumerConfig(base)
	if copied.MaxAttempts != 0 || base.MaxAttempts != 5 {
		t.Fatalf("driver cutoff not safely disabled: copy=%d base=%d", copied.MaxAttempts, base.MaxAttempts)
	}
}

func TestTerminalHandoffUnknownRetainsOriginalAndSkipsHandlerOnRedelivery(t *testing.T) {
	cause := errors.New("business error")
	probe := &handoffProbe{result: transport.Result{Outcome: transport.Unknown}}
	called := 0
	s := testSubscription(t, func(context.Context, transport.Delivery) error {
		called++
		return cause
	}, func(context.Context, legacy.FailedHandoff) error { return nil }, probe)
	body, err := legacy.Encode(legacy.Envelope{UUID: "app-uuid", Payload: []byte("event")}, legacy.Revision1)
	if err != nil {
		t.Fatal(err)
	}
	first, firstDelegate := rawMessage(body, 1)
	if err := s.HandleBusiness(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if firstDelegate.requeued != 1 || firstDelegate.finished != 0 || firstDelegate.delay != time.Second || firstDelegate.backoff || probe.pub != 0 {
		t.Fatalf("first failure: delegate=%+v handoff=%+v", firstDelegate, probe)
	}
	terminal, terminalDelegate := rawMessage(body, 2)
	if err := s.HandleBusiness(context.Background(), terminal); err == nil {
		t.Fatal("unknown handoff confirmation must report uncertainty")
	}
	if terminalDelegate.requeued != 1 || terminalDelegate.finished != 0 || probe.ready != 1 || probe.pub != 1 || probe.topic != s.FailureTopic() || probe.address != "nsqd:4150" {
		t.Fatalf("unknown handoff settled original: delegate=%+v handoff=%+v", terminalDelegate, probe)
	}
	failed, err := legacy.DecodeFailedHandoff(probe.body)
	if err != nil {
		t.Fatal(err)
	}
	if failed.UUID != "app-uuid" || failed.TransportMessageID != "0123456789abcdef" || failed.Attempts != 2 || failed.Cause != cause.Error() || string(failed.Payload) != "event" {
		t.Fatalf("identity or cause lost: %+v", failed)
	}
	probe.result.Outcome = transport.Confirmed
	replayed, replayedDelegate := rawMessage(body, 3)
	if err := s.HandleBusiness(context.Background(), replayed); err != nil {
		t.Fatal(err)
	}
	if called != 2 || replayedDelegate.finished != 1 || replayedDelegate.requeued != 0 || probe.pub != 2 {
		t.Fatalf("terminal replay invoked business handler or lost handoff: called=%d delegate=%+v handoff=%+v", called, replayedDelegate, probe)
	}
}

func TestFailureAuditControlsHandoffSettlement(t *testing.T) {
	auditErr := errors.New("mysql unavailable")
	failedCalls := 0
	s := testSubscription(t, func(context.Context, transport.Delivery) error { return nil }, func(_ context.Context, failed legacy.FailedHandoff) error {
		failedCalls++
		if failed.UUID != "app-uuid" || failed.TransportMessageID != "0123456789abcdef" || failed.Topic != "qs.evaluation.lifecycle" {
			t.Fatalf("failed identity lost: %+v", failed)
		}
		if failedCalls == 1 {
			return auditErr
		}
		return nil
	}, &handoffProbe{result: transport.Result{Outcome: transport.Confirmed}})
	body, err := legacy.EncodeFailedHandoff(legacy.FailedHandoff{
		Topic: "qs.evaluation.lifecycle", Channel: "qs-worker", UUID: "app-uuid",
		TransportMessageID: "0123456789abcdef", Payload: []byte("event"), Attempts: 2, Cause: "business error",
	})
	if err != nil {
		t.Fatal(err)
	}
	first, firstDelegate := rawMessage(body, 1)
	if err := s.HandleFailure(context.Background(), first); !errors.Is(err, auditErr) || firstDelegate.requeued != 1 || firstDelegate.finished != 0 {
		t.Fatalf("audit failure acknowledged: err=%v delegate=%+v", err, firstDelegate)
	}
	second, secondDelegate := rawMessage(body, 2)
	if err := s.HandleFailure(context.Background(), second); err != nil || secondDelegate.finished != 1 || secondDelegate.requeued != 0 {
		t.Fatalf("durable audit did not finish: err=%v delegate=%+v", err, secondDelegate)
	}
}

func TestCorruptRecognizedEnvelopeAndRawFallback(t *testing.T) {
	probe := &handoffProbe{result: transport.Result{Outcome: transport.Confirmed}}
	called := 0
	s := testSubscription(t, func(_ context.Context, d transport.Delivery) error {
		called++
		if m := d.Message(); m.ID != "0123456789abcdef" || !bytes.Equal(m.Payload, []byte("raw body")) {
			t.Fatalf("raw fallback changed: %+v", m)
		}
		return nil
	}, func(context.Context, legacy.FailedHandoff) error { return nil }, probe)
	encoded, err := legacy.Encode(legacy.Envelope{UUID: "corrupt-uuid", Payload: []byte("event")}, legacy.Revision2)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt a single checksum hex digit without changing the JSON envelope.
	corrupt := bytes.Replace(encoded, []byte(`"checksum":"`), []byte(`"checksum":"f`), 1)
	if bytes.Equal(corrupt, encoded) {
		t.Fatalf("checksum marker not found: %s", encoded)
	}
	raw, delegate := rawMessage(corrupt, 2)
	if err := s.HandleBusiness(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if called != 0 || delegate.finished != 1 || probe.pub != 1 {
		t.Fatalf("corrupt envelope reached business: called=%d delegate=%+v", called, delegate)
	}
	failed, err := legacy.DecodeFailedHandoff(probe.body)
	if err != nil || failed.UUID != "corrupt-uuid" || !strings.Contains(failed.Cause, "checksum") {
		t.Fatalf("corruption evidence lost: failed=%+v err=%v", failed, err)
	}
	plain, plainDelegate := rawMessage([]byte("raw body"), 1)
	if err := s.HandleBusiness(context.Background(), plain); err != nil || called != 1 || plainDelegate.finished != 1 {
		t.Fatalf("raw delivery changed: err=%v called=%d delegate=%+v", err, called, plainDelegate)
	}
}

func TestNackWithoutCauseIsRetryAndMissingSourceNeverFinishes(t *testing.T) {
	probe := &handoffProbe{result: transport.Result{Outcome: transport.Confirmed}}
	s := testSubscription(t, func(_ context.Context, d transport.Delivery) error {
		return d.Nack(nil)
	}, func(context.Context, legacy.FailedHandoff) error { return nil }, probe)
	raw, delegate := rawMessage([]byte("raw"), 1)
	if err := s.HandleBusiness(context.Background(), raw); err != nil || delegate.requeued != 1 || delegate.finished != 0 {
		t.Fatalf("nil-cause nack acknowledged: err=%v delegate=%+v", err, delegate)
	}
	terminal, terminalDelegate := rawMessage([]byte("raw"), 2)
	terminal.NSQDAddress = ""
	if err := s.HandleBusiness(context.Background(), terminal); err == nil || terminalDelegate.requeued != 1 || terminalDelegate.finished != 0 || probe.pub != 0 {
		t.Fatalf("missing source acknowledged: err=%v delegate=%+v probe=%+v", err, terminalDelegate, probe)
	}
}

func TestConcurrentSettlementWaitsForOriginalResult(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	want := errors.New("settlement failed")
	calls := 0
	nackInvoked := make(chan struct{}, 1)
	d := &nsqDelivery{ack: func() error {
		close(started)
		<-release
		calls++
		return want
	}, nack: func(error) error {
		nackInvoked <- struct{}{}
		return nil
	}}
	results := make(chan error, 2)
	go func() { results <- d.Ack() }()
	<-started
	go func() { results <- d.Nack(errors.New("late nack")) }()
	select {
	case <-results:
		t.Fatal("settlement returned before first callback completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; !errors.Is(err, want) {
			t.Fatalf("settlement result %d = %v", i, err)
		}
	}
	if calls != 1 || !d.Settled() {
		t.Fatalf("settlement ran %d times", calls)
	}
	select {
	case <-nackInvoked:
		t.Fatal("late nack ran after ack claimed settlement")
	default:
	}
}
