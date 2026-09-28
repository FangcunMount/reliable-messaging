package legacy

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestFailedHandoffRequiresRecoverableIdentity(t *testing.T) {
	valid := FailedHandoff{Topic: "topic", Channel: "channel", UUID: "application-id", Attempts: 1, Cause: "failed", Payload: []byte("payload")}
	for _, change := range []func(*FailedHandoff){
		func(v *FailedHandoff) { v.Topic = "" },
		func(v *FailedHandoff) { v.Channel = "" },
		func(v *FailedHandoff) { v.UUID = "" },
		func(v *FailedHandoff) { v.Attempts = 0 },
		func(v *FailedHandoff) { v.Cause = "" },
	} {
		value := valid
		change(&value)
		if _, err := EncodeFailedHandoff(value); err == nil {
			t.Fatalf("invalid failure identity accepted: %#v", value)
		}
	}
	wire, err := EncodeFailedHandoff(valid)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(wire, &record); err != nil {
		t.Fatal(err)
	}
	record["provider"] = "other"
	invalid, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeFailedHandoff(invalid); err == nil {
		t.Fatal("non-NSQ failure handoff accepted")
	}
}

func TestFailedHandoffDoesNotAliasCallerData(t *testing.T) {
	input := FailedHandoff{Topic: "topic", Channel: "channel", UUID: "id", Attempts: 1, Cause: "failed", Metadata: map[string]string{"source": "original"}, Payload: []byte("payload")}
	wire, err := EncodeFailedHandoff(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Metadata["source"] = "changed"
	input.Payload[0] = '!'
	decoded, err := DecodeFailedHandoff(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Metadata["source"] != "original" || !bytes.Equal(decoded.Payload, []byte("payload")) {
		t.Fatal("encoded failure handoff aliases caller data")
	}
}
