package renewal

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
)

type ChargeRequest struct {
	PatientID      string
	AmountCents    int
	IdempotencyKey string
}

type PaymentProcessor interface {
	SubmitCharge(context.Context, ChargeRequest) error
}

type ResolutionSink interface {
	PublishPayment(context.Context, string, PaymentEvent) error
	PublishCancellation(context.Context, string, CancellationEvent) error
}

type Activities struct {
	Processor PaymentProcessor
	Sink      ResolutionSink
}

func (a *Activities) AttemptCharge(ctx context.Context, patientID string, amountCents int) error {
	if a.Processor == nil {
		return errors.New("payment processor is not configured")
	}
	if patientID == "" {
		return errors.New("patient ID is required")
	}
	if amountCents <= 0 {
		return errors.New("charge amount must be positive")
	}

	info := activity.GetInfo(ctx)
	request := ChargeRequest{
		PatientID:      patientID,
		AmountCents:    amountCents,
		IdempotencyKey: info.ActivityID,
	}
	activity.GetLogger(ctx).Info("submitting charge",
		"workflow_id", info.WorkflowExecution.ID,
		"attempt_id", info.ActivityID,
		"activity_attempt", info.Attempt,
		"amount_cents", amountCents,
	)
	if err := a.Processor.SubmitCharge(ctx, request); err != nil {
		return fmt.Errorf("submit charge: %w", err)
	}
	return nil
}

func (a *Activities) EmitPaymentEvent(ctx context.Context, event PaymentEvent) error {
	if a.Sink == nil {
		return errors.New("resolution sink is not configured")
	}
	if err := validateOpaqueID("idempotency_key", event.IdempotencyKey); err != nil {
		return err
	}
	if err := a.Sink.PublishPayment(ctx, event.IdempotencyKey, event); err != nil {
		return fmt.Errorf("publish payment event: %w", err)
	}
	return nil
}

func (a *Activities) EmitCancellationEvent(ctx context.Context, event CancellationEvent) error {
	if a.Sink == nil {
		return errors.New("resolution sink is not configured")
	}
	eventID := activity.GetInfo(ctx).ActivityID
	if err := validateOpaqueID("event ID", eventID); err != nil {
		return err
	}
	if err := a.Sink.PublishCancellation(ctx, eventID, event); err != nil {
		return fmt.Errorf("publish cancellation event: %w", err)
	}
	return nil
}
