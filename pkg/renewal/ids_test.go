package renewal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkflowIDUsesPatientAndCanonicalCycleButNotAmount(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.FixedZone("BRT", -3*60*60))
	base := RenewalInput{
		PatientID:       "patient-42",
		PlanAmountCents: 29900,
		CycleStart:      start,
		CycleEnd:        start.AddDate(0, 1, 0),
	}

	first, err := WorkflowID(base)
	require.NoError(t, err)

	changedAmount := base
	changedAmount.PlanAmountCents = 39900
	second, err := WorkflowID(changedAmount)
	require.NoError(t, err)
	require.Equal(t, first, second)

	sameInstants := base
	sameInstants.CycleStart = base.CycleStart.UTC()
	sameInstants.CycleEnd = base.CycleEnd.UTC()
	third, err := WorkflowID(sameInstants)
	require.NoError(t, err)
	require.Equal(t, first, third)

	nextCycle := base
	nextCycle.CycleStart = base.CycleStart.AddDate(0, 1, 0)
	nextCycle.CycleEnd = base.CycleEnd.AddDate(0, 1, 0)
	fourth, err := WorkflowID(nextCycle)
	require.NoError(t, err)
	require.NotEqual(t, first, fourth)
	require.Len(t, first, len("renewal-")+sha256HexLength)
}

func TestWorkflowIDRejectsAmbiguousPatientWhitespace(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	_, err := WorkflowID(RenewalInput{
		PatientID:       " patient-42",
		PlanAmountCents: 29900,
		CycleStart:      start,
		CycleEnd:        start.AddDate(0, 1, 0),
	})
	require.EqualError(t, err, "patient_id cannot have surrounding whitespace")
}

const sha256HexLength = 64
