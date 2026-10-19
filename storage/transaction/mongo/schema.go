// Package mongo binds protocol witnesses to an original host Mongo transaction.
package mongo

import (
	"context"
	"errors"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"go.mongodb.org/mongo-driver/bson"
	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const SchemaVersion = 1

func (s *Store) ValidateSchema(ctx context.Context) error {
	cur, err := s.db.ListCollections(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	needed := map[string]bool{"rm_tx_binding": false, "rm_tx_request": false, "rm_tx_execution": false}
	for cur.Next(ctx) {
		var doc struct {
			Name    string `bson:"name"`
			Options struct {
				Collation struct {
					Locale string `bson:"locale"`
				} `bson:"collation"`
			} `bson:"options"`
		}
		if err = cur.Decode(&doc); err != nil {
			return err
		}
		if _, ok := needed[doc.Name]; ok {
			if doc.Options.Collation.Locale != "" && doc.Options.Collation.Locale != "simple" {
				return transaction.ErrSchema
			}
			needed[doc.Name] = true
		}
	}
	if err = cur.Err(); err != nil {
		return err
	}
	for _, exists := range needed {
		if !exists {
			return transaction.ErrSchema
		}
	}
	return nil
}

// InstallSchema is an EXPLICIT host migration helper, never called by New/Start.
// The host separately inserts its owner/store_id/writer_epoch/schema_version binding.
func InstallSchema(ctx context.Context, db *driver.Database) error {
	if db == nil {
		return transaction.ErrBinding
	}
	for _, name := range []string{"rm_tx_binding", "rm_tx_request", "rm_tx_execution"} {
		err := db.CreateCollection(ctx, name, options.CreateCollection().SetCollation(&options.Collation{Locale: "simple"}))
		if err != nil {
			var command driver.CommandError
			if !errors.As(err, &command) || command.Code != 48 {
				return err
			}
		}
		cursor, err := db.ListCollections(ctx, bson.M{"name": name})
		if err != nil {
			return err
		}
		var metadata struct {
			Options struct {
				Collation struct {
					Locale string `bson:"locale"`
				} `bson:"collation"`
			} `bson:"options"`
		}
		if !cursor.Next(ctx) {
			cursor.Close(ctx)
			return transaction.ErrSchema
		}
		err = cursor.Decode(&metadata)
		cursor.Close(ctx)
		if err != nil {
			return err
		}
		if metadata.Options.Collation.Locale != "" && metadata.Options.Collation.Locale != "simple" {
			return transaction.ErrSchema
		}
	}
	_, err := db.Collection("rm_tx_execution").Indexes().CreateOne(ctx, driver.IndexModel{Keys: bson.D{{Key: "owner", Value: 1}, {Key: "state", Value: 1}, {Key: "lease_until", Value: 1}}, Options: options.Index().SetName("rm_tx_expiry")})
	return err
}
