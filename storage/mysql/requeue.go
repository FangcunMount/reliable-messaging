package mysql

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/FangcunMount/reliable-messaging/outbox"
)

// ConfirmedRequeue identifies one original, already-confirmed Outbox row.
// The host must first prove its business effect is missing, authorize the
// request, and write its audit decision in the same transaction. This SDK
// primitive performs no business inspection or permission check.
type ConfirmedRequeue struct {
	RecordID            string
	ExpectedVersion     uint64
	ExpectedFingerprint [sha256.Size]byte
	RequestID           string
}

// RequeueConfirmed schedules the existing row for Relay in the host's
// transaction. It preserves the original identity, payload, fingerprint,
// delivery counters and transport confirmation evidence. RequestID links the
// row to the host's durable authorization record; it is not authorization.
// A missing audit column is a schema error, not a reason to skip the audit.
func (a *Appender) RequeueConfirmed(ctx context.Context, input ConfirmedRequeue) (uint64, error) {
	if a == nil || a.tx == nil {
		return 0, errors.New("host SQL transaction required")
	}
	id, err := strconv.ParseUint(input.RecordID, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != input.RecordID ||
		input.ExpectedVersion == 0 || input.ExpectedVersion == math.MaxUint64 ||
		input.ExpectedFingerprint == [sha256.Size]byte{} ||
		input.RequestID == "" || len(input.RequestID) > 64 || strings.TrimSpace(input.RequestID) != input.RequestID {
		return 0, errors.New("valid original row, fingerprint, version and audit request required")
	}
	result, err := a.tx.ExecContext(ctx, `UPDATE rm_outbox SET state='retry_wait',next_attempt_at=UTC_TIMESTAMP(6),
	claim_token=NULL,lease_until=NULL,manual_replay_request_id=?,manual_replay_version=?,
	version=version+1,updated_at=UTC_TIMESTAMP(6)
	WHERE id=? AND state='published' AND transport_confirmed_at IS NOT NULL
	AND version=? AND fingerprint=?`, input.RequestID, input.ExpectedVersion+1,
		id, input.ExpectedVersion, input.ExpectedFingerprint[:])
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, fmt.Errorf("%w: affected %d rows", outbox.ErrStaleRequeue, n)
	}
	return input.ExpectedVersion + 1, nil
}
