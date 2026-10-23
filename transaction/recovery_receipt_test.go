package transaction

import "testing"

func TestRecoveryFinalRequiresPositiveAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		outcome FinalOutcome
		state   State
	}{{FinalCommitConfirmed, Committed}, {FinalRollbackConfirmed, Aborted}} {
		got, err := RecoveryFinalState(tc.outcome)
		if err != nil || got != tc.state {
			t.Fatal("terminal audit mapping", got, err)
		}
	}
	for _, outcome := range []FinalOutcome{"", FinalUnknown, FinalNotAttempted, "invented"} {
		if _, err := RecoveryFinalState(outcome); err == nil {
			t.Fatal("non-confirmation accepted", outcome)
		}
	}
}
