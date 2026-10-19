package transaction

import (
	"context"
	"unicode/utf8"
)

// HalfReceipt stores Broker identities separately from the original business ID.
// Missing IDs after a lost acknowledgement stay empty, never reconstructed.
type HalfReceipt struct {
	Outcome                                         HalfOutcome
	BrokerMessageID, BrokerTransactionID, ErrorCode string
}

func (h HalfReceipt) Valid() bool {
	if len(h.BrokerMessageID) > 512 || len(h.BrokerTransactionID) > 4096 || len(h.ErrorCode) > 128 || !utf8.ValidString(h.ErrorCode) {
		return false
	}
	switch h.Outcome {
	case HalfPrepared:
		return required(h.BrokerMessageID, h.BrokerTransactionID)
	case HalfUnknown, HalfRejected:
		return true
	}
	return false
}

type ReceiptRecorder interface {
	RecordHalf(context.Context, Execution, Reference, HalfReceipt) error
	RecordFinal(context.Context, Execution, Reference, FinalOutcome, string) error
}

// PreparedStore is a Store with explicit startup validation and RPC audit. Its
// metadata never substitutes for the original-transaction witness decision.
type PreparedStore interface {
	Store
	ReceiptRecorder
	Binding() Binding
	Validate(context.Context) error
}

func MatchExecution(e Execution, ref Reference) bool {
	if !e.Valid() {
		return false
	}
	for _, r := range e.References() {
		if r == ref {
			return true
		}
	}
	return false
}
func ValidFinal(f FinalOutcome, code string) bool {
	return len(code) <= 128 && utf8.ValidString(code) && (f == FinalCommitConfirmed || f == FinalRollbackConfirmed || f == FinalUnknown)
}
