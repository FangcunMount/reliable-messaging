package protected

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

func testKey(t *testing.T, kid string) jose.JSONWebKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return jose.JSONWebKey{Key: k, KeyID: kid}
}
func TestStrictWorkloadContextAndKeyring(t *testing.T) {
	sign, encrypt := testKey(t, "source.sign.1"), testKey(t, "target.encrypt.1")
	c := Context{Producer: "source", Destination: "target", Topic: "commands", MessageID: "original", Metadata: map[string]string{"profile": Profile}}
	ring := Keyring{Decrypt: map[string]jose.JSONWebKey{encrypt.KeyID: encrypt}, Signers: map[string]TrustedSigner{sign.KeyID: {Producer: c.Producer, Key: sign.Public()}}}
	for _, payload := range [][]byte{nil, []byte("中文🙂18446744073709551615")} {
		wire, err := Seal(c, payload, sign, encrypt.Public())
		if err != nil {
			t.Fatal(err)
		}
		opened, err := Open(wire, c, ring, 1024)
		if err != nil || string(opened) != string(payload) {
			t.Fatal("original bytes did not round trip", err)
		}
		for _, change := range []func(*Context){func(c *Context) { c.Topic = "other" }, func(c *Context) { c.Producer = "attacker" }, func(c *Context) { c.Destination = "other" }, func(c *Context) { c.MessageID = "new" }, func(c *Context) { c.Metadata = map[string]string{} }} {
			altered := c
			change(&altered)
			if _, err := Open(wire, altered, ring, 1024); !errors.Is(err, ErrProtection) {
				t.Fatal("context substitution accepted")
			}
		}
		missing := ring
		missing.Signers = map[string]TrustedSigner{}
		if _, err := Open(wire, c, missing, 1024); !errors.Is(err, ErrProtection) {
			t.Fatal("untrusted signer accepted")
		}
		mutated := append([]byte{}, wire...)
		mutated[len(mutated)-2] ^= 1
		if _, err := Open(mutated, c, ring, 1024); !errors.Is(err, ErrProtection) {
			t.Fatal("ciphertext mutation accepted")
		}
	}
	if _, err := Seal(c, []byte("body"), sign, sign.Public()); !errors.Is(err, ErrProtection) {
		t.Fatal("key reuse accepted")
	}
}

func TestPayloadLimitIncludesEncodingOverhead(t *testing.T) {
	sign, encrypt := testKey(t, "source.sign"), testKey(t, "target.encrypt")
	c := Context{Producer: "source", Destination: "target", Topic: "events", MessageID: "original"}
	body := make([]byte, 128*1024)
	wire, err := Seal(c, body, sign, encrypt.Public())
	if err != nil {
		t.Fatal(err)
	}
	ring := Keyring{Decrypt: map[string]jose.JSONWebKey{encrypt.KeyID: encrypt}, Signers: map[string]TrustedSigner{sign.KeyID: {Producer: c.Producer, Key: sign.Public()}}}
	if _, err := Open(wire, c, ring, len(body)); err != nil {
		t.Fatal("legal original body rejected because of encoding overhead", err)
	}
	if _, err := Open(wire, c, ring, len(body)-1); !errors.Is(err, ErrProtection) {
		t.Fatal("body size limit not enforced")
	}
}
