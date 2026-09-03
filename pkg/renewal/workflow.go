package renewal

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type eventBatch struct {
	paymentResults []PaymentResultSignal
	planChanges    []PlanChangeSignal
	cancel         bool
	deadlineReady  bool
	retryReady     bool
	chargeReady    bool
	chargeErr      error
	malformed      int
}

type batchPriority int

const (
	batchNoAction batchPriority = iota
	batchSuccess
	batchCancellation
	batchFailure
	batchDeadline
)

func RenewalWorkflow(ctx workflow.Context, input WorkflowInput) (RenewalResult, error) {
	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	state, err := newRenewalState(input, workflowID)
	if err != nil {
		return RenewalResult{}, fmt.Errorf("validate workflow input: %w", err)
	}

	if err := workflow.SetQueryHandler(ctx, StatusQueryName, func() (RenewalStatus, error) {
		return state.status(), nil
	}); err != nil {
		return RenewalResult{}, fmt.Errorf("register status query: %w", err)
	}

	logger := workflow.GetLogger(ctx)
	paymentResults := workflow.GetSignalChannel(ctx, PaymentResultSignalName)
	planChanges := workflow.GetSignalChannel(ctx, PlanChangeSignalName)
	cancelRequests := workflow.GetSignalChannel(ctx, CancelRequestSignalName)
	deadline := workflow.NewTimer(ctx, input.Policy.ResolutionWindow)

	chargeFuture, cancelCharge := startCharge(ctx, state.activeAttempt, input.Renewal.PatientID)
	var retryTimer workflow.Future
	var cancelRetry workflow.CancelFunc

	logger.Info("renewal started", "workflow_id", workflowID, "max_attempts", state.maxAttempts())

	for !state.hasTerminalResolution() {
		batch := awaitEventBatch(
			ctx,
			paymentResults,
			planChanges,
			cancelRequests,
			deadline,
			chargeFuture,
			retryTimer,
		)
		if batch.malformed > 0 {
			logger.Warn("malformed signals dropped", "count", batch.malformed)
		}

		for _, change := range batch.planChanges {
			accepted := state.applyPlanChange(change)
			logger.Info("plan change received", "accepted", accepted, "new_amount_cents", change.NewAmountCents)
		}

		var successfulResult *PaymentResultSignal
		hasFailedResult := false
		for i := range batch.paymentResults {
			result := batch.paymentResults[i]
			disposition := state.observePaymentResult(result)
			logger.Info("payment result received",
				"processor_txn_id", result.ProcessorTxnID,
				"disposition", disposition.String(),
			)
			switch disposition {
			case paymentSucceeded:
				if successfulResult == nil {
					successfulResult = &result
				}
			case paymentFailed:
				hasFailedResult = true
			}
		}

		// Temporal can make several signals or timers observable in one workflow
		// task. Resolve that batch deterministically: success, cancellation,
		// failure, then the deadline.
		hasSuccessfulResult := successfulResult != nil
		priority := chooseBatchPriority(hasSuccessfulResult, batch.cancel, hasFailedResult, batch.deadlineReady)
		switch priority {
		case batchSuccess:
			stopPending(cancelCharge, cancelRetry)
			if err := state.lockPayment(*successfulResult, workflow.Now(ctx).UTC()); err != nil {
				return RenewalResult{}, err
			}

		case batchCancellation:
			stopPending(cancelCharge, cancelRetry)
			if err := state.lockCancellation(CancellationReasonRequested, workflow.Now(ctx).UTC()); err != nil {
				return RenewalResult{}, err
			}

		case batchFailure:
			stopPending(cancelCharge, nil)
			chargeFuture = nil
			cancelCharge = nil
			attemptsExhausted := state.markAttemptFailed()
			if attemptsExhausted {
				if err := state.lockCancellation(CancellationReasonRetriesExhausted, workflow.Now(ctx).UTC()); err != nil {
					return RenewalResult{}, err
				}
				break
			}
			retryDelay := state.retryDelay()
			logger.Info("renewal retry scheduled",
				"failed_attempt", state.activeAttempt.Number,
				"next_attempt", state.activeAttempt.Number+1,
				"delay", retryDelay.String(),
			)
			retryTimer, cancelRetry = startRetryTimer(ctx, retryDelay)

		case batchDeadline:
			stopPending(cancelCharge, cancelRetry)
			if err := state.lockCancellation(CancellationReasonTimeout, workflow.Now(ctx).UTC()); err != nil {
				return RenewalResult{}, err
			}

		case batchNoAction:
			retryBackoffCompleted := batch.retryReady && state.phase == PhaseBackingOff
			chargeSubmissionCompleted := batch.chargeReady && state.phase == PhaseCharging
			switch {
			case retryBackoffCompleted:
				if cancelRetry != nil {
					cancelRetry()
				}
				retryTimer = nil
				cancelRetry = nil
				attempt := state.beginAttempt()
				chargeFuture, cancelCharge = startCharge(ctx, attempt, input.Renewal.PatientID)

			case chargeSubmissionCompleted:
				state.recordSubmission(batch.chargeErr)
				if cancelCharge != nil {
					cancelCharge()
				}
				chargeFuture = nil
				cancelCharge = nil
				if batch.chargeErr != nil {
					logger.Error("charge submission outcome is uncertain", "error", batch.chargeErr)
				}
			}
		}

		if err := state.checkInvariants(); err != nil {
			return RenewalResult{}, fmt.Errorf("renewal invariant: %w", err)
		}
	}

	logger.Info("renewal resolution selected", "resolution", state.terminal.Kind, "event_id", state.terminal.EventID)
	if err := emitResolution(ctx, state.terminal); err != nil {
		return RenewalResult{}, err
	}
	state.complete()
	return state.result(), nil
}

func chooseBatchPriority(success, cancellation, failure, deadline bool) batchPriority {
	switch {
	case success:
		return batchSuccess
	case cancellation:
		return batchCancellation
	case failure:
		return batchFailure
	case deadline:
		return batchDeadline
	default:
		return batchNoAction
	}
}

func awaitEventBatch(
	ctx workflow.Context,
	paymentResults workflow.ReceiveChannel,
	planChanges workflow.ReceiveChannel,
	cancelRequests workflow.ReceiveChannel,
	deadline workflow.Future,
	charge workflow.Future,
	retry workflow.Future,
) eventBatch {
	// Select one ready input, then drain signals already queued for the same
	// workflow task. This makes conflicting, near-simultaneous inputs follow the
	// explicit priority in chooseBatchPriority instead of selector callback order.
	batch := eventBatch{}
	selector := workflow.NewSelector(ctx)

	selector.AddReceive(paymentResults, func(channel workflow.ReceiveChannel, _ bool) {
		var signal PaymentResultSignal
		if channel.ReceiveAsync(&signal) {
			batch.paymentResults = append(batch.paymentResults, signal)
		} else {
			batch.malformed++
		}
	})
	selector.AddReceive(planChanges, func(channel workflow.ReceiveChannel, _ bool) {
		var signal PlanChangeSignal
		if channel.ReceiveAsync(&signal) {
			batch.planChanges = append(batch.planChanges, signal)
		} else {
			batch.malformed++
		}
	})
	selector.AddReceive(cancelRequests, func(channel workflow.ReceiveChannel, _ bool) {
		var signal struct{}
		if channel.ReceiveAsync(&signal) {
			batch.cancel = true
		} else {
			batch.malformed++
		}
	})
	selector.AddFuture(deadline, func(workflow.Future) {
		batch.deadlineReady = true
	})
	if charge != nil {
		selector.AddFuture(charge, func(future workflow.Future) {
			batch.chargeReady = true
			batch.chargeErr = future.Get(ctx, nil)
		})
	}
	if retry != nil {
		selector.AddFuture(retry, func(workflow.Future) {
			batch.retryReady = true
		})
	}

	selector.Select(ctx)
	drainSignals(paymentResults, planChanges, cancelRequests, &batch)

	if deadline.IsReady() {
		batch.deadlineReady = true
	}
	chargeReadyOutsideSelector := charge != nil && charge.IsReady() && !batch.chargeReady
	if chargeReadyOutsideSelector {
		batch.chargeReady = true
		batch.chargeErr = charge.Get(ctx, nil)
	}
	if retry != nil && retry.IsReady() {
		batch.retryReady = true
	}
	return batch
}

func drainSignals(
	paymentResults workflow.ReceiveChannel,
	planChanges workflow.ReceiveChannel,
	cancelRequests workflow.ReceiveChannel,
	batch *eventBatch,
) {
	for remaining := paymentResults.Len(); remaining > 0; remaining-- {
		var signal PaymentResultSignal
		if paymentResults.ReceiveAsync(&signal) {
			batch.paymentResults = append(batch.paymentResults, signal)
		}
	}
	for remaining := planChanges.Len(); remaining > 0; remaining-- {
		var signal PlanChangeSignal
		if planChanges.ReceiveAsync(&signal) {
			batch.planChanges = append(batch.planChanges, signal)
		}
	}
	for remaining := cancelRequests.Len(); remaining > 0; remaining-- {
		var signal struct{}
		if cancelRequests.ReceiveAsync(&signal) {
			batch.cancel = true
		}
	}
}

func startCharge(
	ctx workflow.Context,
	attempt chargeAttempt,
	patientID string,
) (workflow.Future, workflow.CancelFunc) {
	activityContext, cancel := workflow.WithCancel(ctx)
	activityContext = workflow.WithActivityOptions(activityContext, workflow.ActivityOptions{
		ActivityID:             attempt.ID,
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    250 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Second,
			MaximumAttempts:    3,
		},
	})
	return workflow.ExecuteActivity(
		activityContext,
		AttemptChargeActivityName,
		patientID,
		attempt.AmountCents,
	), cancel
}

func startRetryTimer(ctx workflow.Context, delay time.Duration) (workflow.Future, workflow.CancelFunc) {
	timerContext, cancel := workflow.WithCancel(ctx)
	return workflow.NewTimer(timerContext, delay), cancel
}

func emitResolution(ctx workflow.Context, resolution *terminalResolution) error {
	activityContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          resolution.EventID,
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    0,
		},
	})

	if resolution.Payment != nil {
		if err := workflow.ExecuteActivity(activityContext, EmitPaymentActivityName, *resolution.Payment).Get(ctx, nil); err != nil {
			return fmt.Errorf("emit payment resolution: %w", err)
		}
		return nil
	}
	if err := workflow.ExecuteActivity(activityContext, EmitCancellationActivityName, *resolution.Cancellation).Get(ctx, nil); err != nil {
		return fmt.Errorf("emit cancellation resolution: %w", err)
	}
	return nil
}

func stopPending(cancelCharge, cancelRetry workflow.CancelFunc) {
	if cancelCharge != nil {
		cancelCharge()
	}
	if cancelRetry != nil {
		cancelRetry()
	}
}

func (d paymentDisposition) String() string {
	switch d {
	case paymentDuplicate:
		return "duplicate"
	case paymentConflict:
		return "conflict"
	case paymentOutOfState:
		return "out_of_state"
	case paymentSucceeded:
		return "succeeded"
	case paymentFailed:
		return "failed"
	default:
		return "invalid"
	}
}
