// A test-only cross-language codec probe. Input keys must be disposable test keys.
package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	durable "github.com/FangcunMount/reliable-messaging/delivery/mysql"
	"github.com/FangcunMount/reliable-messaging/transport"
	rmnsq "github.com/FangcunMount/reliable-messaging/transport/nsq"
	"github.com/FangcunMount/reliable-messaging/wire/legacy"
	protected "github.com/FangcunMount/reliable-messaging/wire/protected"
	jose "github.com/go-jose/go-jose/v4"
	_ "github.com/go-sql-driver/mysql"
	driver "github.com/nsqio/go-nsq"
)

type input struct {
	DSN        string            `json:"dsn"`
	Address    string            `json:"address"`
	Topic      string            `json:"topic"`
	Mode       string            `json:"mode"`
	Context    protected.Context `json:"context"`
	Payload    []byte            `json:"payload"`
	Signing    jose.JSONWebKey   `json:"signing"`
	Encryption jose.JSONWebKey   `json:"encryption"`
	Wire       []byte            `json:"wire"`
}

func main() {
	var v input
	if err := json.NewDecoder(os.Stdin).Decode(&v); err != nil {
		panic("invalid test input")
	}
	var body []byte
	var err error
	switch v.Mode {
	case "outbox-publish":
		db, openErr := sql.Open("mysql", v.DSN)
		if openErr != nil {
			err = openErr
			break
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tx, beginErr := db.BeginTx(ctx, nil)
		if beginErr != nil {
			err = beginErr
			break
		}
		defer tx.Rollback()
		store, _ := durable.New("rm_durable_outbox")
		rows, readErr := store.Pending(ctx, tx, 20)
		if readErr != nil {
			err = readErr
			break
		}
		if len(rows) != 1 {
			err = fmt.Errorf("expected one original pending message")
			break
		}
		if markErr := store.Published(ctx, tx, rows[0].Identity, rows[0].BodySHA256, 30); markErr != nil {
			err = markErr
			break
		}
		if commitErr := tx.Commit(); commitErr != nil {
			err = commitErr
			break
		}
		body = rows[0].Wire
	case "publish":
		cfg := driver.NewConfig()
		cfg.DialTimeout, cfg.ReadTimeout, cfg.WriteTimeout = time.Second, 5*time.Second, time.Second
		producer, createErr := driver.NewProducer(v.Address, cfg)
		if createErr != nil {
			err = createErr
			break
		}
		publisher, createErr := rmnsq.New(producer, nil, 1)
		if createErr != nil {
			producer.Stop()
			err = createErr
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := publisher.PublishRaw(ctx, v.Topic, v.Wire)
		cancel()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
		_ = publisher.Drain(drainCtx)
		drainCancel()
		producer.Stop()
		if result.Outcome != transport.Confirmed {
			err = fmt.Errorf("test publish not confirmed")
		} else {
			body = []byte("OK")
		}
	case "seal":
		body, err = protected.Seal(v.Context, v.Payload, v.Signing, v.Encryption)
	case "open":
		body, err = protected.Open(v.Wire, v.Context, protected.Keyring{
			Decrypt: map[string]jose.JSONWebKey{v.Encryption.KeyID: v.Encryption},
			Signers: map[string]protected.TrustedSigner{v.Signing.KeyID: {Producer: v.Context.Producer, Key: v.Signing.Public()}},
		}, 16*1024*1024)
	case "legacy":
		body, err = legacy.Encode(legacy.Envelope{UUID: v.Context.MessageID, Metadata: v.Context.Metadata, Payload: v.Payload}, legacy.Revision2)
	default:
		panic("unknown probe mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "codec rejected test input")
		os.Exit(1)
	}
	fmt.Println(base64.StdEncoding.EncodeToString(body))
}
