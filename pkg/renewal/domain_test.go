package renewal

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanChangeOnlyUpdatesAFutureAttempt(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	require.Equal(t, 29900, state.activeAttempt.AmountCents)
	require.True(t, state.applyPlanChange(PlanChangeSignal{NewAmountCents: 39900}))
	require.Equal(t, 29900, state.activeAttempt.AmountCents)
	require.Equal(t, 39900, state.pendingAmountCents)

	require.False(t, state.markAttemptFailed())
	state.beginAttempt()
	require.Equal(t, 39900, state.activeAttempt.AmountCents)
	require.Equal(t, 2, state.activeAttempt.Number)
}

func TestPlanChangeIsIgnoredAfterFinalAttemptStarts(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	state.markAttemptFailed()
	state.beginAttempt()
	state.markAttemptFailed()
	state.beginAttempt()

	require.False(t, state.applyPlanChange(PlanChangeSignal{NewAmountCents: 49900}))
	require.Equal(t, 29900, state.activeAttempt.AmountCents)
}

func TestPaymentResultFirstPayloadWins(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	original := PaymentResultSignal{ProcessorTxnID: "txn-1", AmountChargedCents: 0}
	require.Equal(t, paymentFailed, state.observePaymentResult(original))
	require.Equal(t, paymentDuplicate, state.observePaymentResult(original))
	require.Equal(t, paymentConflict, state.observePaymentResult(PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-1",
		AmountChargedCents: 29900,
	}))
	require.Equal(t, 1, len(state.seenPaymentResults))
	require.Equal(t, 2, state.ignoredPaymentResults)
}

func TestPaymentResultValidationAndOutOfStateHandling(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	require.Equal(t, paymentInvalid, state.observePaymentResult(PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-wrong-amount",
		AmountChargedCents: 1,
	}))

	state.markAttemptFailed()
	require.Equal(t, paymentOutOfState, state.observePaymentResult(PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-late",
		AmountChargedCents: 29900,
	}))

	state.beginAttempt()
	require.Equal(t, paymentDuplicate, state.observePaymentResult(PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-late",
		AmountChargedCents: 29900,
	}))
}

func TestTerminalResolutionIsWriteOnce(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	success := PaymentResultSignal{Succeeded: true, ProcessorTxnID: "txn-1", AmountChargedCents: 29900}

	require.NoError(t, state.lockPayment(success, now))
	require.ErrorIs(t, state.lockCancellation(CancellationReasonRequested, now), ErrAlreadyResolved)
	require.Equal(t, ResolutionPaid, state.terminal.Kind)
	require.Nil(t, state.terminal.Cancellation)
	require.NoError(t, state.checkInvariants())
}

func TestSubmissionErrorIsUncertainAndDoesNotTriggerDunning(t *testing.T) {
	t.Parallel()

	state := newTestState(t)
	state.recordSubmission(errors.New("connection reset"))

	require.Equal(t, PhaseAwaiting, state.phase)
	require.Equal(t, SubmissionUncertain, state.activeAttempt.Submission)
	require.Equal(t, 1, state.activeAttempt.Number)
}

func newTestState(t *testing.T) *renewalState {
	t.Helper()
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	input := WorkflowInput{
		Renewal: RenewalInput{
			PatientID:       "patient-42",
			PlanAmountCents: 29900,
			CycleStart:      start,
			CycleEnd:        start.AddDate(0, 1, 0),
		},
		Policy: DefaultDunningPolicy(),
	}
	state, err := newRenewalState(input, "renewal-test")
	require.NoError(t, err)
	return state
}
