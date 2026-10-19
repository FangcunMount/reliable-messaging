// Package transactionexample illustrates ORIGINAL host transaction ownership.
// Schema/configuration/authorization are explicit host prerequisites; this
// package has no init(), network, DDL or lifecycle side effects.
package transactionexample

import (
	"context"
	"database/sql"

	mongostore "github.com/FangcunMount/reliable-messaging/storage/transaction/mongo"
	mysqlstore "github.com/FangcunMount/reliable-messaging/storage/transaction/mysql"
	"github.com/FangcunMount/reliable-messaging/transaction"
	"github.com/FangcunMount/reliable-messaging/transport/rocketmq"
	"go.mongodb.org/mongo-driver/bson"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
)

// SendSQL receives a fully frozen plan and a started owner sender. Host installs
// SDK schema plus host_business_events(id) separately. Never automatically retry
// this function after an unknown result: Lookup the original request instead.
func SendSQL(ctx context.Context, db *sql.DB, store *mysqlstore.Store, sender *rocketmq.TransactionalSender, plan transaction.FrozenPlan) (transaction.Result, error) {
	reservation, err := sender.Reserve(ctx, transaction.RequestFor(plan), plan)
	if err != nil {
		return transaction.Result{}, err
	}
	if !reservation.Acquired {
		// Duplicate is observation, not a new execution capability.
		return transaction.InitialResult(reservation.Execution), transaction.ErrStaleExecution
	}
	return sender.Execute(ctx, reservation.Execution, func(localCtx context.Context, execution transaction.Execution) (transaction.LocalOutcome, error) {
		tx, err := db.BeginTx(localCtx, nil)
		if err != nil {
			return transaction.LocalAborted, err
		}
		defer tx.Rollback()
		guard, err := store.Bind(localCtx, tx, execution) // first, original root transaction
		if err != nil {
			return transaction.LocalAborted, err
		}
		if _, err = tx.ExecContext(localCtx, "INSERT INTO host_business_events(id) VALUES(?)", execution.Plan.Input().IdempotencyKey); err != nil {
			return transaction.LocalAborted, err
		}
		if err = guard.CommitWitness(localCtx); err != nil {
			return transaction.LocalAborted, err
		}
		if err = tx.Commit(); err != nil {
			return transaction.LocalUnknown, err
		}
		return transaction.LocalCommitted, nil
	})
}

// ExecuteGORM receives the one-time reservation winner, not a plain DB handle
// masquerading as a transaction. GORM owns the actual root Commit/Rollback.
func ExecuteGORM(ctx context.Context, db *gorm.DB, store *mysqlstore.Store, sender *rocketmq.TransactionalSender, execution transaction.Execution) (transaction.Result, error) {
	return sender.Execute(ctx, execution, func(localCtx context.Context, e transaction.Execution) (transaction.LocalOutcome, error) {
		err := db.WithContext(localCtx).Transaction(func(tx *gorm.DB) error {
			guard, err := store.BindGORM(localCtx, tx, e)
			if err != nil {
				return err
			}
			if err = tx.Exec("INSERT INTO host_business_events(id) VALUES(?)", e.Plan.Input().IdempotencyKey).Error; err != nil {
				return err
			}
			return guard.CommitWitness(localCtx)
		})
		if err != nil {
			return transaction.LocalUnknown, err
		}
		return transaction.LocalCommitted, nil
	})
}

// ExecuteMongo freezes all messages before WithTransaction. The driver may
// repeat ONLY this pure DB callback; the SDK local executor still runs once.
func ExecuteMongo(ctx context.Context, db *mongo.Database, store *mongostore.Store, sender *rocketmq.TransactionalSender, execution transaction.Execution) (transaction.Result, error) {
	return sender.Execute(ctx, execution, func(localCtx context.Context, e transaction.Execution) (transaction.LocalOutcome, error) {
		session, err := db.Client().StartSession()
		if err != nil {
			return transaction.LocalAborted, err
		}
		defer session.EndSession(localCtx)
		_, err = session.WithTransaction(localCtx, func(sc mongo.SessionContext) (any, error) {
			guard, err := store.Bind(sc, e)
			if err != nil {
				return nil, err
			}
			if _, err = db.Collection("host_business_events").InsertOne(sc, bson.M{"_id": e.Plan.Input().IdempotencyKey}); err != nil {
				return nil, err
			}
			return nil, guard.CommitWitness(sc)
		})
		if err != nil {
			return transaction.LocalUnknown, err
		}
		return transaction.LocalCommitted, nil
	})
}
