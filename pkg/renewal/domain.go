package renewal

import (
	"errors"
	"fmt"
	"time"
)

type paymentDisposition int

const (
	paymentInvalid paymentDisposition = iota
	paymentDuplicate
	paymentConflict
	paymentOutOfState
	paymentSucceeded
	paymentFailed
)

type chargeAttempt struct {
	Number      int
	ID          string
	AmountCents int
	Submission  SubmissionState
}

type terminalResolution struct {
	Kind         Resolution
	EventID      string
	Payment      *PaymentEvent
	Cancellation *CancellationEvent
}

type renewalState struct {
	input                 WorkflowInput
	workflowID            string
	phase                 Phase
	pendingAmountCents    int
	activeAttempt         chargeAttempt
	seenPaymentResults    map[string]PaymentResultSignal
	ignoredPaymentResults int
	terminal              *terminalResolution
}

func newRenewalState(input WorkflowInput, workflowID string) (*renewalState, error) {
	if err := ValidateWorkflowInput(input); err != nil {
		return nil, err
	}
	if workflowID == "" {
		return nil, errors.New("workflow ID is required")
	}

	state := &renewalState{
		input:              input,
		workflowID:         workflowID,
		pendingAmountCents: input.Renewal.PlanAmountCents,
		seenPaymentResults: make(map[string]PaymentResultSignal),
	}
	state.beginAttempt()
	return state, nil
}

func (s *renewalState) beginAttempt() chargeAttempt {
	next := s.activeAttempt.Number + 1
	s.activeAttempt = chargeAttempt{
		Number:      next,
		ID:          AttemptID(s.workflowID, next),
		AmountCents: s.pendingAmountCents,
		Submission:  SubmissionPending,
	}
	s.phase = PhaseCharging
	return s.activeAttempt
}

func (s *renewalState) recordSubmission(err error) {
	if !s.canRecordSubmission() {
		return
	}
	if err == nil {
		s.activeAttempt.Submission = SubmissionAccepted
	} else {
		s.activeAttempt.Submission = SubmissionUncertain
	}
	s.phase = PhaseAwaiting
}

func (s *renewalState) applyPlanChange(change PlanChangeSignal) bool {
	if !s.canApplyPlanChange(change) {
		return false
	}
	s.pendingAmountCents = change.NewAmountCents
	return true
}

func (s *renewalState) observePaymentResult(signal PaymentResultSignal) paymentDisposition {
	if err := validateOpaqueID("processor_txn_id", signal.ProcessorTxnID); err != nil {
		s.ignoredPaymentResults++
		return paymentInvalid
	}

	if first, exists := s.seenPaymentResults[signal.ProcessorTxnID]; exists {
		s.ignoredPaymentResults++
		if first == signal {
			return paymentDuplicate
		}
		return paymentConflict
	}
	s.seenPaymentResults[signal.ProcessorTxnID] = signal

	if !s.canAcceptPaymentResult() {
		s.ignoredPaymentResults++
		return paymentOutOfState
	}

	amountMatchesAttempt := signal.AmountChargedCents == s.activeAttempt.AmountCents
	if signal.Succeeded {
		if !amountMatchesAttempt {
			s.ignoredPaymentResults++
			return paymentInvalid
		}
		return paymentSucceeded
	}

	reportsNoCharge := signal.AmountChargedCents == 0
	if !reportsNoCharge && !amountMatchesAttempt {
		s.ignoredPaymentResults++
		return paymentInvalid
	}
	return paymentFailed
}

func (s *renewalState) markAttemptFailed() bool {
	if s.hasTerminalResolution() {
		return false
	}
	if !s.hasAnotherAttempt() {
		return true
	}
	s.phase = PhaseBackingOff
	return false
}

func (s *renewalState) lockPayment(signal PaymentResultSignal, now time.Time) error {
	if s.hasTerminalResolution() {
		return ErrAlreadyResolved
	}
	eventID := ResolutionEventID(s.workflowID)
	event := PaymentEvent{
		PatientID:      s.input.Renewal.PatientID,
		AmountCents:    signal.AmountChargedCents,
		IdempotencyKey: eventID,
		EmittedAt:      now,
	}
	s.terminal = &terminalResolution{Kind: ResolutionPaid, EventID: eventID, Payment: &event}
	s.phase = PhaseEmitting
	return nil
}

func (s *renewalState) lockCancellation(reason string, now time.Time) error {
	if s.hasTerminalResolution() {
		return ErrAlreadyResolved
	}
	eventID := ResolutionEventID(s.workflowID)
	event := CancellationEvent{
		PatientID: s.input.Renewal.PatientID,
		Reason:    reason,
		EmittedAt: now,
	}
	s.terminal = &terminalResolution{Kind: ResolutionCancelled, EventID: eventID, Cancellation: &event}
	s.phase = PhaseEmitting
	return nil
}

func (s *renewalState) complete() {
	s.phase = PhaseCompleted
}

func (s *renewalState) maxAttempts() int {
	return len(s.input.Policy.RetryDelays) + 1
}

func (s *renewalState) hasTerminalResolution() bool {
	return s.terminal != nil
}

func (s *renewalState) hasAnotherAttempt() bool {
	return s.activeAttempt.Number < s.maxAttempts()
}

func (s *renewalState) canRecordSubmission() bool {
	return !s.hasTerminalResolution() && s.phase == PhaseCharging
}

func (s *renewalState) canApplyPlanChange(change PlanChangeSignal) bool {
	hasValidAmount := change.NewAmountCents > 0
	return hasValidAmount && !s.hasTerminalResolution() && s.hasAnotherAttempt()
}

func (s *renewalState) canAcceptPaymentResult() bool {
	phaseAcceptsResult := s.phase == PhaseCharging || s.phase == PhaseAwaiting
	return !s.hasTerminalResolution() && phaseAcceptsResult
}

func (s *renewalState) retryDelay() time.Duration {
	return s.input.Policy.RetryDelays[s.activeAttempt.Number-1]
}

func (s *renewalState) status() RenewalStatus {
	status := RenewalStatus{
		Phase:                 s.phase,
		Resolution:            ResolutionPending,
		Attempt:               s.activeAttempt.Number,
		MaxAttempts:           s.maxAttempts(),
		ActiveAmountCents:     s.activeAttempt.AmountCents,
		PendingAmountCents:    s.pendingAmountCents,
		Submission:            s.activeAttempt.Submission,
		SeenPaymentResults:    len(s.seenPaymentResults),
		IgnoredPaymentResults: s.ignoredPaymentResults,
	}
	if !s.hasTerminalResolution() {
		return status
	}
	status.Resolution = s.terminal.Kind
	if s.terminal.Cancellation != nil {
		status.CancellationReason = s.terminal.Cancellation.Reason
	}
	return status
}

func (s *renewalState) result() RenewalResult {
	result := RenewalResult{Resolution: s.terminal.Kind, Attempts: s.activeAttempt.Number}
	if s.terminal.Payment != nil {
		result.AmountCents = s.terminal.Payment.AmountCents
	}
	if s.terminal.Cancellation != nil {
		result.CancellationReason = s.terminal.Cancellation.Reason
	}
	return result
}

func (s *renewalState) checkInvariants() error {
	attemptWithinRange := s.activeAttempt.Number >= 1 && s.activeAttempt.Number <= s.maxAttempts()
	if !attemptWithinRange {
		return fmt.Errorf("attempt %d is outside [1,%d]", s.activeAttempt.Number, s.maxAttempts())
	}
	amountsRemainPositive := s.activeAttempt.AmountCents > 0 && s.pendingAmountCents > 0
	if !amountsRemainPositive {
		return errors.New("amounts must remain positive")
	}
	phaseRequiresResolution := s.phase == PhaseEmitting || s.phase == PhaseCompleted
	if !s.hasTerminalResolution() {
		if phaseRequiresResolution {
			return errors.New("terminal phase requires a resolution")
		}
		return nil
	}
	hasOnlyPaymentEvent := s.terminal.Payment != nil && s.terminal.Cancellation == nil
	if s.terminal.Kind == ResolutionPaid && !hasOnlyPaymentEvent {
		return errors.New("paid resolution must contain only a payment event")
	}
	hasOnlyCancellationEvent := s.terminal.Cancellation != nil && s.terminal.Payment == nil
	if s.terminal.Kind == ResolutionCancelled && !hasOnlyCancellationEvent {
		return errors.New("cancelled resolution must contain only a cancellation event")
	}
	return nil
}
