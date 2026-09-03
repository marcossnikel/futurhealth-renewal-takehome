package renewal

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	TaskQueue                    = "subscription-renewals"
	WorkflowName                 = "subscription-renewal"
	PaymentResultSignalName      = "payment_result"
	PlanChangeSignalName         = "plan_change"
	CancelRequestSignalName      = "cancel_request"
	StatusQueryName              = "renewal_status"
	AttemptChargeActivityName    = "attempt-charge"
	EmitPaymentActivityName      = "emit-payment-event"
	EmitCancellationActivityName = "emit-cancellation-event"
)

const (
	CancellationReasonRetriesExhausted = "retries_exhausted"
	CancellationReasonRequested        = "cancel_requested"
	CancellationReasonTimeout          = "timeout"
)

type RenewalInput struct {
	PatientID       string    `json:"patient_id"`
	PlanAmountCents int       `json:"plan_amount_cents"`
	CycleStart      time.Time `json:"cycle_start"`
	CycleEnd        time.Time `json:"cycle_end"`
}

type PaymentResultSignal struct {
	Succeeded          bool   `json:"succeeded"`
	ProcessorTxnID     string `json:"processor_txn_id"`
	AmountChargedCents int    `json:"amount_charged_cents"`
}

type PlanChangeSignal struct {
	NewAmountCents int `json:"new_amount_cents"`
}

type PaymentEvent struct {
	PatientID      string    `json:"patient_id"`
	AmountCents    int       `json:"amount_cents"`
	IdempotencyKey string    `json:"idempotency_key"`
	EmittedAt      time.Time `json:"emitted_at"`
}

type CancellationEvent struct {
	PatientID string    `json:"patient_id"`
	Reason    string    `json:"reason"`
	EmittedAt time.Time `json:"emitted_at"`
}

type DunningPolicy struct {
	RetryDelays      []time.Duration `json:"retry_delays"`
	ResolutionWindow time.Duration   `json:"resolution_window"`
}

type WorkflowInput struct {
	Renewal RenewalInput  `json:"renewal"`
	Policy  DunningPolicy `json:"policy"`
}

type Phase string

const (
	PhaseCharging   Phase = "charging"
	PhaseAwaiting   Phase = "awaiting_payment_result"
	PhaseBackingOff Phase = "backing_off"
	PhaseEmitting   Phase = "emitting_resolution"
	PhaseCompleted  Phase = "completed"
)

type Resolution string

const (
	ResolutionPending   Resolution = "pending"
	ResolutionPaid      Resolution = "paid"
	ResolutionCancelled Resolution = "cancelled"
)

type SubmissionState string

const (
	SubmissionPending   SubmissionState = "pending"
	SubmissionAccepted  SubmissionState = "accepted"
	SubmissionUncertain SubmissionState = "uncertain"
)

type RenewalStatus struct {
	Phase                 Phase           `json:"phase"`
	Resolution            Resolution      `json:"resolution"`
	Attempt               int             `json:"attempt"`
	MaxAttempts           int             `json:"max_attempts"`
	ActiveAmountCents     int             `json:"active_amount_cents"`
	PendingAmountCents    int             `json:"pending_amount_cents"`
	Submission            SubmissionState `json:"submission"`
	SeenPaymentResults    int             `json:"seen_payment_results"`
	IgnoredPaymentResults int             `json:"ignored_payment_results"`
	CancellationReason    string          `json:"cancellation_reason,omitempty"`
}

type RenewalResult struct {
	Resolution         Resolution `json:"resolution"`
	Attempts           int        `json:"attempts"`
	AmountCents        int        `json:"amount_cents,omitempty"`
	CancellationReason string     `json:"cancellation_reason,omitempty"`
}

var ErrAlreadyResolved = errors.New("renewal already resolved")

func DefaultDunningPolicy() DunningPolicy {
	return DunningPolicy{
		RetryDelays:      []time.Duration{time.Second, 2 * time.Second},
		ResolutionWindow: 12 * time.Second,
	}
}

func ValidateWorkflowInput(input WorkflowInput) error {
	if err := ValidateRenewalInput(input.Renewal); err != nil {
		return err
	}
	return ValidateDunningPolicy(input.Policy)
}

func ValidateDunningPolicy(policy DunningPolicy) error {
	for i, delay := range policy.RetryDelays {
		if delay <= 0 {
			return fmt.Errorf("retry_delays[%d] must be positive", i)
		}
	}
	if policy.ResolutionWindow <= 0 {
		return errors.New("resolution_window must be positive")
	}
	return nil
}

func ValidateRenewalInput(input RenewalInput) error {
	if strings.TrimSpace(input.PatientID) == "" {
		return errors.New("patient_id is required")
	}
	if input.PatientID != strings.TrimSpace(input.PatientID) {
		return errors.New("patient_id cannot have surrounding whitespace")
	}
	if input.PlanAmountCents <= 0 {
		return errors.New("plan_amount_cents must be positive")
	}
	missingCycleBoundary := input.CycleStart.IsZero() || input.CycleEnd.IsZero()
	if missingCycleBoundary {
		return errors.New("cycle_start and cycle_end are required")
	}
	if !input.CycleStart.Before(input.CycleEnd) {
		return errors.New("cycle_start must be before cycle_end")
	}
	return nil
}

func ValidatePaymentResultSignal(signal PaymentResultSignal) error {
	if err := validateOpaqueID("processor_txn_id", signal.ProcessorTxnID); err != nil {
		return err
	}
	if signal.AmountChargedCents < 0 {
		return errors.New("amount_charged_cents cannot be negative")
	}
	successfulPaymentHasNoAmount := signal.Succeeded && signal.AmountChargedCents == 0
	if successfulPaymentHasNoAmount {
		return errors.New("a successful payment must have a positive amount_charged_cents")
	}
	return nil
}

func ValidatePlanChangeSignal(signal PlanChangeSignal) error {
	if signal.NewAmountCents <= 0 {
		return errors.New("new_amount_cents must be positive")
	}
	return nil
}

func ValidateWorkflowID(workflowID string) error {
	return validateOpaqueID("workflow ID", workflowID)
}
