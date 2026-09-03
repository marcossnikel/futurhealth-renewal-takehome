package renewal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRenewalWorkflowHappyPath(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, PaymentResultSignal{
			Succeeded:          true,
			ProcessorTxnID:     "txn-happy",
			AmountChargedCents: 29900,
		})
	}, time.Second)

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, RenewalResult{Resolution: ResolutionPaid, Attempts: 1, AmountCents: 29900}, result)
	require.Equal(t, []int{29900}, processor.amounts())
	require.Len(t, sink.paymentEffects(), 1)
	require.Empty(t, sink.cancellationEffects())
	require.Equal(t, ResolutionEventID(testWorkflowID), sink.paymentEffects()[0].event.IdempotencyKey)
}

func TestRenewalWorkflowRetriesThenSucceeds(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, PaymentResultSignal{ProcessorTxnID: "txn-declined-1"})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, PaymentResultSignal{
			Succeeded:          true,
			ProcessorTxnID:     "txn-paid-2",
			AmountChargedCents: 29900,
		})
	}, 3*time.Minute)

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionPaid, result.Resolution)
	require.Equal(t, 2, result.Attempts)
	require.Equal(t, []int{29900, 29900}, processor.amounts())
	require.Len(t, sink.paymentEffects(), 1)
	require.Empty(t, sink.cancellationEffects())
}

func TestRenewalWorkflowExhaustsRetries(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{ProcessorTxnID: "txn-declined-1"})
	schedulePaymentResult(env, 3*time.Minute, PaymentResultSignal{ProcessorTxnID: "txn-declined-2"})
	schedulePaymentResult(env, 7*time.Minute, PaymentResultSignal{ProcessorTxnID: "txn-declined-3"})

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, RenewalResult{
		Resolution:         ResolutionCancelled,
		Attempts:           3,
		CancellationReason: CancellationReasonRetriesExhausted,
	}, result)
	require.Equal(t, []int{29900, 29900, 29900}, processor.amounts())
	require.Empty(t, sink.paymentEffects())
	require.Len(t, sink.cancellationEffects(), 1)
	require.Equal(t, CancellationReasonRetriesExhausted, sink.cancellationEffects()[0].event.Reason)
}

func TestRenewalWorkflowHandlesNearSimultaneousDuplicateAndConflictingResults(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	decline := PaymentResultSignal{ProcessorTxnID: "txn-one"}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, decline)
		env.SignalWorkflow(PaymentResultSignalName, decline)
		env.SignalWorkflow(PaymentResultSignalName, PaymentResultSignal{
			Succeeded:          true,
			ProcessorTxnID:     "txn-one",
			AmountChargedCents: 29900,
		})
	}, time.Second)
	schedulePaymentResult(env, 2*time.Minute, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-two",
		AmountChargedCents: 29900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionPaid, result.Resolution)
	require.Equal(t, 2, result.Attempts)
	require.Equal(t, []int{29900, 29900}, processor.amounts())
	require.Len(t, sink.paymentEffects(), 1)
	require.Empty(t, sink.cancellationEffects())
}

func TestRenewalWorkflowUsesPlanChangeForNextAttempt(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{ProcessorTxnID: "txn-declined"})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanChangeSignalName, PlanChangeSignal{NewAmountCents: 39900})
	}, 30*time.Second)
	schedulePaymentResult(env, 3*time.Minute, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-paid",
		AmountChargedCents: 39900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, 39900, result.AmountCents)
	require.Equal(t, []int{29900, 39900}, processor.amounts())
	require.Len(t, sink.paymentEffects(), 1)
}

func TestRenewalWorkflowCancelsDuringBackoff(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{ProcessorTxnID: "txn-declined"})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CancelRequestSignalName, struct{}{})
	}, 30*time.Second)

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionCancelled, result.Resolution)
	require.Equal(t, CancellationReasonRequested, result.CancellationReason)
	require.Equal(t, []int{29900}, processor.amounts())
	require.Empty(t, sink.paymentEffects())
	require.Len(t, sink.cancellationEffects(), 1)
}

func TestRenewalWorkflowTimesOutWithoutPaymentResult(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionCancelled, result.Resolution)
	require.Equal(t, CancellationReasonTimeout, result.CancellationReason)
	require.Equal(t, []int{29900}, processor.amounts())
	require.Len(t, sink.cancellationEffects(), 1)
}

func TestMalformedDirectSignalCannotBlockDeadline(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, "not-a-payment-result")
	}, time.Second)

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionCancelled, result.Resolution)
	require.Equal(t, CancellationReasonTimeout, result.CancellationReason)
	require.Equal(t, []int{29900}, processor.amounts())
	require.Len(t, sink.cancellationEffects(), 1)
}

func TestRenewalWorkflowIgnoresResultReceivedDuringBackoff(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{ProcessorTxnID: "txn-declined"})
	schedulePaymentResult(env, 30*time.Second, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-late",
		AmountChargedCents: 29900,
	})
	schedulePaymentResult(env, 3*time.Minute, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-current",
		AmountChargedCents: 29900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionPaid, result.Resolution)
	require.Equal(t, 2, result.Attempts)
	require.Equal(t, []int{29900, 29900}, processor.amounts())
	require.Len(t, sink.paymentEffects(), 1)
}

func TestActivityRetriesReuseChargeIdempotencyKey(t *testing.T) {
	env, processor, sink := newWorkflowEnvironment(t)
	processor.failAcknowledgements = 1
	input := workflowTestInput()
	schedulePaymentResult(env, time.Minute, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-paid",
		AmountChargedCents: 29900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	requireWorkflowResult(t, env)

	require.Equal(t, 2, processor.callCount())
	require.Equal(t, 1, processor.effectCount())
	require.Equal(t, []string{AttemptID(testWorkflowID, 1)}, processor.effectKeys())
	require.Len(t, sink.paymentEffects(), 1)
}

func TestResolutionActivityRetryProducesOneSinkEffect(t *testing.T) {
	env, _, sink := newWorkflowEnvironment(t)
	sink.failAcknowledgements = 1
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-paid",
		AmountChargedCents: 29900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	requireWorkflowResult(t, env)

	require.Equal(t, 2, sink.callCount())
	require.Len(t, sink.paymentEffects(), 1)
	require.Equal(t, ResolutionEventID(testWorkflowID), sink.paymentEffects()[0].key)
}

func TestCoObservedInputsUseDocumentedPriority(t *testing.T) {
	t.Parallel()
	require.Equal(t, batchSuccess, chooseBatchPriority(true, true, true, true))
	require.Equal(t, batchCancellation, chooseBatchPriority(false, true, true, true))
	require.Equal(t, batchFailure, chooseBatchPriority(false, false, true, true))
	require.Equal(t, batchDeadline, chooseBatchPriority(false, false, false, true))
	require.Equal(t, batchNoAction, chooseBatchPriority(false, false, false, false))
}

func TestRenewalWorkflowPrioritizesCoObservedSuccessOverCancellation(t *testing.T) {
	env, _, sink := newWorkflowEnvironment(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflowSkippingWorkflowTask(PaymentResultSignalName, PaymentResultSignal{
			Succeeded:          true,
			ProcessorTxnID:     "txn-paid",
			AmountChargedCents: 29900,
		})
		env.SignalWorkflow(CancelRequestSignalName, struct{}{})
	}, time.Second)

	env.ExecuteWorkflow(WorkflowName, workflowTestInput())
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionPaid, result.Resolution)
	require.Len(t, sink.paymentEffects(), 1)
	require.Empty(t, sink.cancellationEffects())
}

func TestLateCancellationCannotSwitchResolutionDuringEventRetry(t *testing.T) {
	env, _, sink := newWorkflowEnvironment(t)
	sink.failAcknowledgements = 1
	input := workflowTestInput()
	schedulePaymentResult(env, time.Second, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-paid",
		AmountChargedCents: 29900,
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CancelRequestSignalName, struct{}{})
	}, 1100*time.Millisecond)

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionPaid, result.Resolution)
	require.Equal(t, 2, sink.callCount())
	require.Len(t, sink.paymentEffects(), 1)
	require.Empty(t, sink.cancellationEffects())
}

func TestPaymentResultAfterResolutionCannotCreateAnotherEvent(t *testing.T) {
	env, _, sink := newWorkflowEnvironment(t)
	sink.failAcknowledgements = 1
	input := workflowTestInput()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CancelRequestSignalName, struct{}{})
	}, time.Second)
	schedulePaymentResult(env, 1100*time.Millisecond, PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-too-late",
		AmountChargedCents: 29900,
	})

	env.ExecuteWorkflow(WorkflowName, input)
	result := requireWorkflowResult(t, env)

	require.Equal(t, ResolutionCancelled, result.Resolution)
	require.Equal(t, CancellationReasonRequested, result.CancellationReason)
	require.Equal(t, 2, sink.callCount())
	require.Empty(t, sink.paymentEffects())
	require.Len(t, sink.cancellationEffects(), 1)
}

const testWorkflowID = "renewal-test"

func workflowTestInput() WorkflowInput {
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	return WorkflowInput{
		Renewal: RenewalInput{
			PatientID:       "patient-42",
			PlanAmountCents: 29900,
			CycleStart:      start,
			CycleEnd:        start.AddDate(0, 1, 0),
		},
		Policy: DunningPolicy{
			RetryDelays:      []time.Duration{time.Minute, 2 * time.Minute},
			ResolutionWindow: 10 * time.Minute,
		},
	}
}

func newWorkflowEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *recordingProcessor, *recordingSink) {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: testWorkflowID})
	env.RegisterWorkflowWithOptions(RenewalWorkflow, workflow.RegisterOptions{Name: WorkflowName})

	processor := &recordingProcessor{effects: make(map[string]ChargeRequest)}
	sink := &recordingSink{effects: make(map[string]recordedResolution)}
	activities := &Activities{Processor: processor, Sink: sink}
	env.RegisterActivityWithOptions(activities.AttemptCharge, activity.RegisterOptions{Name: AttemptChargeActivityName})
	env.RegisterActivityWithOptions(activities.EmitPaymentEvent, activity.RegisterOptions{Name: EmitPaymentActivityName})
	env.RegisterActivityWithOptions(activities.EmitCancellationEvent, activity.RegisterOptions{Name: EmitCancellationActivityName})
	return env, processor, sink
}

func requireWorkflowResult(t *testing.T, env *testsuite.TestWorkflowEnvironment) RenewalResult {
	t.Helper()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result RenewalResult
	require.NoError(t, env.GetWorkflowResult(&result))
	return result
}

func schedulePaymentResult(env *testsuite.TestWorkflowEnvironment, delay time.Duration, signal PaymentResultSignal) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PaymentResultSignalName, signal)
	}, delay)
}

type recordingProcessor struct {
	mu                   sync.Mutex
	calls                []ChargeRequest
	effects              map[string]ChargeRequest
	failAcknowledgements int
}

func (p *recordingProcessor) SubmitCharge(_ context.Context, request ChargeRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, request)
	if _, exists := p.effects[request.IdempotencyKey]; !exists {
		p.effects[request.IdempotencyKey] = request
	}
	if p.failAcknowledgements > 0 {
		p.failAcknowledgements--
		return errors.New("simulated lost acknowledgement")
	}
	return nil
}

func (p *recordingProcessor) amounts() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	amounts := make([]int, 0, len(p.effects))
	seen := make(map[string]struct{}, len(p.effects))
	for _, call := range p.calls {
		if _, exists := seen[call.IdempotencyKey]; exists {
			continue
		}
		seen[call.IdempotencyKey] = struct{}{}
		amounts = append(amounts, call.AmountCents)
	}
	return amounts
}

func (p *recordingProcessor) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *recordingProcessor) effectCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.effects)
}

func (p *recordingProcessor) effectKeys() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys := make([]string, 0, len(p.effects))
	seen := make(map[string]struct{}, len(p.effects))
	for _, call := range p.calls {
		if _, exists := seen[call.IdempotencyKey]; exists {
			continue
		}
		seen[call.IdempotencyKey] = struct{}{}
		keys = append(keys, call.IdempotencyKey)
	}
	return keys
}

type recordedCancellation struct {
	key   string
	event CancellationEvent
}

type recordedPayment struct {
	key   string
	event PaymentEvent
}

type recordedResolution struct {
	kind         Resolution
	payment      *PaymentEvent
	cancellation *CancellationEvent
}

type recordingSink struct {
	mu                   sync.Mutex
	calls                int
	effects              map[string]recordedResolution
	failAcknowledgements int
}

func (s *recordingSink) PublishPayment(_ context.Context, key string, event PaymentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	existing, found := s.effects[key]
	conflictsWithPayment := found && existing.kind != ResolutionPaid
	if conflictsWithPayment {
		return errors.New("resolution key already used for cancellation")
	}
	if !found {
		copy := event
		s.effects[key] = recordedResolution{kind: ResolutionPaid, payment: &copy}
	}
	return s.acknowledge()
}

func (s *recordingSink) PublishCancellation(_ context.Context, key string, event CancellationEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	existing, found := s.effects[key]
	conflictsWithCancellation := found && existing.kind != ResolutionCancelled
	if conflictsWithCancellation {
		return errors.New("resolution key already used for payment")
	}
	if !found {
		copy := event
		s.effects[key] = recordedResolution{kind: ResolutionCancelled, cancellation: &copy}
	}
	return s.acknowledge()
}

func (s *recordingSink) acknowledge() error {
	if s.failAcknowledgements == 0 {
		return nil
	}
	s.failAcknowledgements--
	return errors.New("simulated lost acknowledgement")
}

func (s *recordingSink) paymentEffects() []recordedPayment {
	s.mu.Lock()
	defer s.mu.Unlock()
	payments := make([]recordedPayment, 0, len(s.effects))
	for key, effect := range s.effects {
		if effect.payment != nil {
			payments = append(payments, recordedPayment{key: key, event: *effect.payment})
		}
	}
	return payments
}

func (s *recordingSink) cancellationEffects() []recordedCancellation {
	s.mu.Lock()
	defer s.mu.Unlock()
	cancellations := make([]recordedCancellation, 0, len(s.effects))
	for key, effect := range s.effects {
		if effect.cancellation != nil {
			cancellations = append(cancellations, recordedCancellation{key: key, event: *effect.cancellation})
		}
	}
	return cancellations
}

func (s *recordingSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}
