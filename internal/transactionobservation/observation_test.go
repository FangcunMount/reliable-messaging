package transactionobservation

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
)

func TestSharedObservationMeaning(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/transaction-observation-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version int       `json:"version"`
		AsOf    time.Time `json:"as_of"`
		Cases   []struct {
			Name    string            `json:"name"`
			State   transaction.State `json:"state"`
			Started bool              `json:"started"`
			Lease   time.Time         `json:"lease"`
			Branch  *struct {
				State   transaction.State        `json:"state"`
				Half    transaction.HalfOutcome  `json:"half"`
				Final   transaction.FinalOutcome `json:"final"`
				HalfAt  *time.Time               `json:"half_at"`
				FinalAt *time.Time               `json:"final_at"`
			} `json:"branch"`
			Expected map[string]float64 `json:"expected"`
			Error    bool               `json:"error"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || len(fixture.Cases) != 14 {
		t.Fatal("required shared observation cases missing")
	}
	for _, c := range fixture.Cases {
		t.Run(c.Name, func(t *testing.T) {
			a := Accumulator{Snapshot: transaction.ProtocolObservation{AsOf: fixture.AsOf}}
			if err = a.Execution(c.State, c.Started, c.Lease); err != nil {
				t.Fatal(err)
			}
			if c.Branch != nil {
				b := Branch{State: c.Branch.State, Half: c.Branch.Half, Final: c.Branch.Final}
				if c.Branch.HalfAt != nil {
					b.HalfAt = *c.Branch.HalfAt
				}
				if c.Branch.FinalAt != nil {
					b.FinalAt = *c.Branch.FinalAt
				}
				err = a.Branch(c.State, c.Started, b)
				if (err != nil) != c.Error {
					t.Fatalf("error=%v expected error=%v", err, c.Error)
				}
				if err != nil {
					return
				}
			}
			s := a.Snapshot
			got := map[string]float64{
				"reserved_executions": float64(s.ReservedExecutions), "preparing_executions": float64(s.PreparingExecutions),
				"prepared_expiry_age_seconds": s.PreparedExpiryAgeSeconds, "confirmation_gap_branches": float64(s.ConfirmationGapBranches),
				"oldest_confirmation_observation_age_seconds": s.OldestConfirmationObservationAgeSeconds,
				"unaged_confirmation_gap_branches":            float64(s.UnagedConfirmationGapBranches), "untracked_aborted_branches": float64(s.UntrackedAbortedBranches),
				"unobserved_committed_branches": float64(s.UnobservedCommittedBranches), "metadata_conflicts": float64(s.MetadataConflicts),
			}
			for k, v := range got {
				if v != c.Expected[k] {
					t.Errorf("%s=%v want %v", k, v, c.Expected[k])
				}
			}
			for k := range c.Expected {
				if _, ok := got[k]; !ok {
					t.Errorf("unknown expected field %s", k)
				}
			}
		})
	}
}
