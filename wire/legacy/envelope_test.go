package legacy

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDecodeLeavesUnmarkedRawPayloadToTransport(t *testing.T) {
	for _, body := range [][]byte{[]byte("raw event"), []byte(`{"type":"other","payload":"aGVsbG8="}`), []byte(`{"type":`)} {
		got, recognized, err := Decode(body)
		if err != nil || recognized || got.UUID != "" {
			t.Fatalf("unmarked body %q = %#v, %t, %v", body, got, recognized, err)
		}
	}
}

func TestDecodeRejectsRecognizedCorruption(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"malformed payload", `{"type":"component-base.messaging.message.v1","uuid":"original","payload":42}`},
		{"missing checksum", `{"type":"component-base.messaging.message.v1","schema_revision":2,"uuid":"original","payload":"aGVsbG8="}`},
		{"unknown revision", `{"type":"component-base.messaging.message.v1","schema_revision":3,"uuid":"original","payload":"aGVsbG8="}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, recognized, err := Decode([]byte(tc.body))
			if err == nil || !recognized || got.UUID != "original" {
				t.Fatalf("corrupt recognized body = %#v, %t, %v", got, recognized, err)
			}
		})
	}
}

func TestEncodeDoesNotMutateInputOrAcceptUnknownRevision(t *testing.T) {
	input := Envelope{UUID: "original", Metadata: map[string]string{"source": "test"}, Payload: []byte("payload")}
	if _, err := Encode(input, 0); err == nil {
		t.Fatal("missing revision accepted")
	}
	if _, err := Encode(input, 3); err == nil {
		t.Fatal("future revision accepted")
	}
	wire, err := Encode(input, Revision2)
	if err != nil {
		t.Fatal(err)
	}
	input.Metadata["source"] = "changed"
	input.Payload[0] = '!'
	var encoded struct {
		Metadata map[string]string `json:"metadata"`
		Payload  []byte            `json:"payload"`
	}
	if err := json.Unmarshal(wire, &encoded); err != nil {
		t.Fatal(err)
	}
	if encoded.Metadata["source"] != "test" || !bytes.Equal(encoded.Payload, []byte("payload")) {
		t.Fatal("encoded bytes alias input")
	}
}
