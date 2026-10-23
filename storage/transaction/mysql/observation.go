package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"time"

	"github.com/FangcunMount/reliable-messaging/internal/transactionobservation"
	"github.com/FangcunMount/reliable-messaging/transaction"
)

//go:embed observation.sql
var observationSQL string

var _ transaction.ObservationReader = (*Store)(nil)

// ObserveProtocol executes one non-locking primary statement, excluding plan,
// reference, message identities and wire. Rows are streamed, not retained.
func (s *Store) ObserveProtocol(ctx context.Context) (transaction.ProtocolObservation, error) {
	ctx, cancel := transactionobservation.Budget(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, observationSQL, []byte(s.binding.Owner))
	if err != nil {
		return transaction.ProtocolObservation{}, err
	}
	defer rows.Close()
	var a transactionobservation.Accumulator
	for rows.Next() {
		var id, epoch, clock string
		var version int
		var ek, bk []byte
		var es, lease, bs, half, final, halfAt, finalAt sql.NullString
		var started sql.NullBool
		var first bool
		if err = rows.Scan(&id, &epoch, &version, &clock, &ek, &es, &started, &lease, &bk, &bs, &half, &final, &halfAt, &finalAt, &first); err != nil {
			return transaction.ProtocolObservation{}, err
		}
		if id != s.binding.StoreID || epoch != s.binding.WriterEpoch {
			return transaction.ProtocolObservation{}, transaction.ErrBinding
		}
		if version != SchemaVersion {
			return transaction.ProtocolObservation{}, transaction.ErrSchema
		}
		a.Snapshot.AsOf, err = time.ParseInLocation(clockLayout, clock, time.UTC)
		if err != nil {
			return transaction.ProtocolObservation{}, err
		}
		if ek == nil {
			continue
		}
		if len(ek) != 32 || !es.Valid || !started.Valid || !lease.Valid {
			return transaction.ProtocolObservation{}, transactionobservation.ErrMetadata
		}
		until, err := time.ParseInLocation(clockLayout, lease.String, time.UTC)
		if err != nil {
			return transaction.ProtocolObservation{}, err
		}
		if first {
			if err = a.Execution(transaction.State(es.String), started.Bool, until); err != nil {
				return transaction.ProtocolObservation{}, err
			}
		}
		if bk == nil {
			a.Snapshot.MetadataConflicts++
			continue
		}
		if len(bk) != 32 || !bs.Valid || !half.Valid || !final.Valid || half.String == "" || final.String == "" {
			return transaction.ProtocolObservation{}, transactionobservation.ErrMetadata
		}
		b := transactionobservation.Branch{State: transaction.State(bs.String), Half: transaction.HalfOutcome(half.String), Final: transaction.FinalOutcome(final.String)}
		if halfAt.Valid {
			b.HalfAt, err = time.ParseInLocation(clockLayout, halfAt.String, time.UTC)
			if err != nil {
				return transaction.ProtocolObservation{}, err
			}
		}
		if finalAt.Valid {
			b.FinalAt, err = time.ParseInLocation(clockLayout, finalAt.String, time.UTC)
			if err != nil {
				return transaction.ProtocolObservation{}, err
			}
		}
		if err = a.Branch(transaction.State(es.String), started.Bool, b); err != nil {
			return transaction.ProtocolObservation{}, err
		}
	}
	if err = rows.Err(); err != nil {
		return transaction.ProtocolObservation{}, err
	}
	if a.Snapshot.AsOf.IsZero() {
		return transaction.ProtocolObservation{}, transaction.ErrBinding
	}
	return a.Snapshot, nil
}
