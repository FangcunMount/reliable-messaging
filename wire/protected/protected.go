// Package protected binds immutable bytes and workload routing to a strict JOSE profile.
// Hosts own keys, key rotation, authorization and persistence of the sealed wire.
package protected

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
)

const Profile = "rm-secure-v1"

var ErrProtection = errors.New("protected message rejected")

type Context struct {
	Producer    string            `json:"producer"`
	Destination string            `json:"destination"`
	Topic       string            `json:"topic"`
	MessageID   string            `json:"message_id"`
	Metadata    map[string]string `json:"metadata"`
}

type TrustedSigner struct {
	Producer string
	Key      jose.JSONWebKey
}
type Keyring struct {
	Decrypt map[string]jose.JSONWebKey
	Signers map[string]TrustedSigner
}
type record struct {
	Context       Context `json:"context"`
	PayloadSHA256 string  `json:"payload_sha256"`
	Payload       []byte  `json:"payload"`
}

func validContext(c Context) bool {
	return c.Producer != "" && c.Destination != "" && c.Topic != "" && c.MessageID != ""
}
func validKey(k jose.JSONWebKey, private bool) bool {
	if k.KeyID == "" || !k.Valid() {
		return false
	}
	switch key := k.Key.(type) {
	case *ecdsa.PrivateKey:
		return key.Curve == elliptic.P256()
	case *ecdsa.PublicKey:
		return !private && key.Curve == elliptic.P256()
	default:
		return false
	}
}

// Seal has no I/O. Persist the result once; never call Seal on technical redelivery.
func Seal(c Context, payload []byte, signing, recipient jose.JSONWebKey) ([]byte, error) {
	if !validContext(c) || !validKey(signing, true) || !validKey(recipient, false) {
		return nil, ErrProtection
	}
	if reflect.DeepEqual(signing.Public().Key, recipient.Public().Key) {
		return nil, ErrProtection
	}
	if c.Metadata == nil {
		c.Metadata = map[string]string{}
	}
	hash := sha256.Sum256(payload)
	body, err := json.Marshal(record{c, fmt.Sprintf("%x", hash), append([]byte{}, payload...)})
	if err != nil {
		return nil, ErrProtection
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: signing}, (&jose.SignerOptions{}).WithType(Profile))
	if err != nil {
		return nil, ErrProtection
	}
	signed, err := signer.Sign(body)
	if err != nil {
		return nil, ErrProtection
	}
	compact, err := signed.CompactSerialize()
	if err != nil {
		return nil, ErrProtection
	}
	encryptor, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.ECDH_ES_A256KW, Key: recipient.Public()}, (&jose.EncrypterOptions{}).WithType(Profile).WithContentType("JWS"))
	if err != nil {
		return nil, ErrProtection
	}
	encrypted, err := encryptor.Encrypt([]byte(compact))
	if err != nil {
		return nil, ErrProtection
	}
	wire, err := encrypted.CompactSerialize()
	if err != nil {
		return nil, ErrProtection
	}
	return []byte(wire), nil
}

// header rejects algorithm negotiation, embedded signing keys and remote key URLs.
func header(segment string, allowed ...string) (map[string]json.RawMessage, error) {
	body, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return nil, ErrProtection
	}
	var h map[string]json.RawMessage
	if json.Unmarshal(body, &h) != nil || h == nil {
		return nil, ErrProtection
	}
	for k := range h {
		found := false
		for _, a := range allowed {
			if k == a {
				found = true
				break
			}
		}
		if !found {
			return nil, ErrProtection
		}
	}
	return h, nil
}
func stringValue(h map[string]json.RawMessage, k string) string {
	var v string
	_ = json.Unmarshal(h[k], &v)
	return v
}

// Open authenticates before any business identity or body reference can be used.
func Open(wire []byte, expected Context, keys Keyring, maxPayload int) ([]byte, error) {
	if !validContext(expected) || maxPayload < 1 || len(wire) > 3*maxPayload+8192 {
		return nil, ErrProtection
	}
	parts := strings.Split(string(wire), ".")
	if len(parts) != 5 {
		return nil, ErrProtection
	}
	h, err := header(parts[0], "alg", "enc", "kid", "typ", "cty", "epk")
	if err != nil {
		return nil, err
	}
	if stringValue(h, "alg") != string(jose.ECDH_ES_A256KW) || stringValue(h, "enc") != string(jose.A256GCM) || stringValue(h, "typ") != Profile || stringValue(h, "cty") != "JWS" {
		return nil, ErrProtection
	}
	kid := stringValue(h, "kid")
	key, ok := keys.Decrypt[kid]
	if !ok || !validKey(key, true) || key.KeyID != kid {
		return nil, ErrProtection
	}
	encrypted, err := jose.ParseEncrypted(string(wire), []jose.KeyAlgorithm{jose.ECDH_ES_A256KW}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, ErrProtection
	}
	plain, err := encrypted.Decrypt(key)
	if err != nil {
		return nil, ErrProtection
	}
	signature := strings.Split(string(plain), ".")
	if len(signature) != 3 {
		return nil, ErrProtection
	}
	sh, err := header(signature[0], "alg", "kid", "typ")
	if err != nil {
		return nil, err
	}
	if stringValue(sh, "alg") != string(jose.ES256) || stringValue(sh, "typ") != Profile {
		return nil, ErrProtection
	}
	signer, ok := keys.Signers[stringValue(sh, "kid")]
	if !ok || signer.Producer != expected.Producer || !validKey(signer.Key, false) || signer.Key.KeyID != stringValue(sh, "kid") {
		return nil, ErrProtection
	}
	signed, err := jose.ParseSigned(string(plain), []jose.SignatureAlgorithm{jose.ES256})
	if err != nil {
		return nil, ErrProtection
	}
	body, err := signed.Verify(signer.Key.Public())
	if err != nil {
		return nil, ErrProtection
	}
	var value record
	if json.Unmarshal(body, &value) != nil {
		return nil, ErrProtection
	}
	if expected.Metadata == nil {
		expected.Metadata = map[string]string{}
	}
	if value.Context.Metadata == nil {
		value.Context.Metadata = map[string]string{}
	}
	if !reflect.DeepEqual(value.Context, expected) || len(value.Payload) > maxPayload {
		return nil, ErrProtection
	}
	hash := sha256.Sum256(value.Payload)
	if subtle.ConstantTimeCompare([]byte(value.PayloadSHA256), []byte(fmt.Sprintf("%x", hash))) != 1 {
		return nil, ErrProtection
	}
	return append([]byte(nil), value.Payload...), nil
}
