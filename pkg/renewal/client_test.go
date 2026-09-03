package renewal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	temporalclient "go.temporal.io/sdk/client"
	temporalmocks "go.temporal.io/sdk/mocks"
)

func TestNewServiceValidatesConfigurationAndCopiesPolicy(t *testing.T) {
	temporalClient := temporalmocks.NewClient(t)
	tests := []struct {
		name      string
		client    temporalclient.Client
		taskQueue string
		policy    DunningPolicy
		wantErr   string
	}{
		{"Temporal client", nil, TaskQueue, DefaultDunningPolicy(), "Temporal client is required"},
		{"task queue", temporalClient, "", DefaultDunningPolicy(), "task queue is required"},
		{"dunning policy", temporalClient, TaskQueue, DunningPolicy{}, "invalid dunning policy: resolution_window must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewService(test.client, test.taskQueue, test.policy)
			require.Nil(t, service)
			require.EqualError(t, err, test.wantErr)
		})
	}

	policy := DunningPolicy{
		RetryDelays:      []time.Duration{time.Minute, 2 * time.Minute},
		ResolutionWindow: 10 * time.Minute,
	}
	service, err := NewService(temporalClient, TaskQueue, policy)
	require.NoError(t, err)
	policy.RetryDelays[0] = 24 * time.Hour
	require.Equal(t, []time.Duration{time.Minute, 2 * time.Minute}, service.policy.RetryDelays)
}

func TestServiceStartRenewalUsesStableWorkflowContract(t *testing.T) {
	ctx := context.Background()
	temporalClient := temporalmocks.NewClient(t)
	policy := DunningPolicy{
		RetryDelays:      []time.Duration{time.Minute, 2 * time.Minute},
		ResolutionWindow: 10 * time.Minute,
	}
	service, err := NewService(temporalClient, "renewal-tests", policy)
	require.NoError(t, err)
	policy.RetryDelays[0] = 24 * time.Hour

	input := clientTestRenewalInput()
	workflowID, err := WorkflowID(input)
	require.NoError(t, err)
	run := temporalmocks.NewWorkflowRun(t)
	run.On("GetID").Return(workflowID).Once()
	run.On("GetRunID").Return("run-123").Once()

	var gotOptions temporalclient.StartWorkflowOptions
	var gotInput WorkflowInput
	temporalClient.On("ExecuteWorkflow", ctx, mock.Anything, WorkflowName, mock.Anything).
		Run(func(arguments mock.Arguments) {
			gotOptions = arguments.Get(1).(temporalclient.StartWorkflowOptions)
			gotInput = arguments.Get(3).(WorkflowInput)
		}).Return(run, nil).Once()

	started, err := service.StartRenewal(ctx, input)
	require.NoError(t, err)
	require.Equal(t, StartedRenewal{WorkflowID: workflowID, RunID: "run-123"}, started)
	require.Equal(t, temporalclient.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                "renewal-tests",
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		StaticSummary:                            "Subscription renewal and dunning",
	}, gotOptions)
	require.Equal(t, WorkflowInput{
		Renewal: input,
		Policy: DunningPolicy{
			RetryDelays:      []time.Duration{time.Minute, 2 * time.Minute},
			ResolutionWindow: 10 * time.Minute,
		},
	}, gotInput)

	gotInput.Policy.RetryDelays[0] = 48 * time.Hour
	require.Equal(t, []time.Duration{time.Minute, 2 * time.Minute}, service.policy.RetryDelays)
}

func TestServiceStartRenewalValidationAndDuplicateTranslation(t *testing.T) {
	t.Run("invalid input is not sent", func(t *testing.T) {
		service, _ := newClientTestService(t)
		input := clientTestRenewalInput()
		input.PatientID = ""

		started, err := service.StartRenewal(context.Background(), input)
		require.Equal(t, StartedRenewal{}, started)
		require.EqualError(t, err, "patient_id is required")
	})

	t.Run("duplicate returns the known workflow ID", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		input := clientTestRenewalInput()
		workflowID, err := WorkflowID(input)
		require.NoError(t, err)
		temporalClient.On("ExecuteWorkflow", ctx, mock.Anything, WorkflowName, mock.Anything).
			Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "request-1", "run-1")).Once()

		started, err := service.StartRenewal(ctx, input)
		require.Equal(t, StartedRenewal{WorkflowID: workflowID}, started)
		require.ErrorIs(t, err, ErrRenewalAlreadyExists)
		require.EqualError(t, err, "renewal already exists: "+workflowID)
	})

	t.Run("other Temporal errors are wrapped", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		workflowErr := errors.New("Temporal unavailable")
		temporalClient.On("ExecuteWorkflow", ctx, mock.Anything, WorkflowName, mock.Anything).
			Return(nil, workflowErr).Once()

		started, err := service.StartRenewal(ctx, clientTestRenewalInput())
		require.Equal(t, StartedRenewal{}, started)
		require.ErrorIs(t, err, workflowErr)
		require.ErrorContains(t, err, "start renewal workflow")
	})
}

func TestServiceSignalsValidateAndForward(t *testing.T) {
	ctx := context.Background()
	service, temporalClient := newClientTestService(t)
	payment := PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "txn-123",
		AmountChargedCents: 29900,
	}
	planChange := PlanChangeSignal{NewAmountCents: 39900}
	temporalClient.On("SignalWorkflow", ctx, "workflow-1", "", PaymentResultSignalName, payment).Return(nil).Once()
	temporalClient.On("SignalWorkflow", ctx, "workflow-1", "", PlanChangeSignalName, planChange).Return(nil).Once()
	temporalClient.On("SignalWorkflow", ctx, "workflow-1", "", CancelRequestSignalName, struct{}{}).Return(nil).Once()

	require.NoError(t, service.SendPaymentResult(ctx, "workflow-1", payment))
	require.NoError(t, service.ChangePlan(ctx, "workflow-1", planChange))
	require.NoError(t, service.CancelRenewal(ctx, "workflow-1"))

	invalid := []struct {
		name string
		call func() error
		want string
	}{
		{"payment workflow ID", func() error {
			return service.SendPaymentResult(ctx, "bad ID", payment)
		}, "workflow ID contains whitespace or control characters"},
		{"payment", func() error {
			return service.SendPaymentResult(ctx, "workflow-1", PaymentResultSignal{})
		}, "processor_txn_id is required"},
		{"plan workflow ID", func() error {
			return service.ChangePlan(ctx, "", planChange)
		}, "workflow ID is required"},
		{"plan change", func() error {
			return service.ChangePlan(ctx, "workflow-1", PlanChangeSignal{})
		}, "new_amount_cents must be positive"},
		{"cancellation", func() error {
			return service.CancelRenewal(ctx, "bad ID")
		}, "workflow ID contains whitespace or control characters"},
	}
	for _, test := range invalid {
		t.Run("invalid "+test.name, func(t *testing.T) {
			require.EqualError(t, test.call(), test.want)
		})
	}
}

func TestServiceRenewalStatus(t *testing.T) {
	t.Run("validates the workflow ID", func(t *testing.T) {
		service, _ := newClientTestService(t)

		status, err := service.RenewalStatus(context.Background(), "")
		require.Equal(t, RenewalStatus{}, status)
		require.EqualError(t, err, "workflow ID is required")
	})

	t.Run("decodes the status query", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		want := RenewalStatus{
			Phase:              PhaseAwaiting,
			Resolution:         ResolutionPending,
			Attempt:            2,
			MaxAttempts:        3,
			ActiveAmountCents:  39900,
			PendingAmountCents: 39900,
			Submission:         SubmissionAccepted,
			SeenPaymentResults: 1,
		}
		value := temporalmocks.NewEncodedValue(t)
		value.On("Get", mock.Anything).Run(func(arguments mock.Arguments) {
			*arguments.Get(0).(*RenewalStatus) = want
		}).Return(nil).Once()
		temporalClient.On("QueryWorkflow", ctx, "workflow-1", "", StatusQueryName).Return(value, nil).Once()

		got, err := service.RenewalStatus(ctx, "workflow-1")
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("translates not found", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		temporalClient.On("QueryWorkflow", ctx, "workflow-1", "", StatusQueryName).
			Return(nil, serviceerror.NewNotFound("missing workflow")).Once()

		status, err := service.RenewalStatus(ctx, "workflow-1")
		require.Equal(t, RenewalStatus{}, status)
		require.ErrorIs(t, err, ErrRenewalNotFound)
	})

	t.Run("wraps decode errors", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		decodeErr := errors.New("invalid payload")
		value := temporalmocks.NewEncodedValue(t)
		value.On("Get", mock.Anything).Return(decodeErr).Once()
		temporalClient.On("QueryWorkflow", ctx, "workflow-1", "", StatusQueryName).Return(value, nil).Once()

		status, err := service.RenewalStatus(ctx, "workflow-1")
		require.Equal(t, RenewalStatus{}, status)
		require.ErrorIs(t, err, decodeErr)
		require.ErrorContains(t, err, "decode renewal status")
	})
}

func TestServiceWaitForResult(t *testing.T) {
	t.Run("decodes the result", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		want := RenewalResult{Resolution: ResolutionPaid, Attempts: 2, AmountCents: 39900}
		run := temporalmocks.NewWorkflowRun(t)
		run.On("Get", ctx, mock.Anything).Run(func(arguments mock.Arguments) {
			*arguments.Get(1).(*RenewalResult) = want
		}).Return(nil).Once()
		temporalClient.On("GetWorkflow", ctx, "workflow-1", "run-1").Return(run).Once()

		got, err := service.WaitForResult(ctx, "workflow-1", "run-1")
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("returns workflow errors", func(t *testing.T) {
		ctx := context.Background()
		service, temporalClient := newClientTestService(t)
		workflowErr := errors.New("workflow failed")
		run := temporalmocks.NewWorkflowRun(t)
		run.On("Get", ctx, mock.Anything).Return(workflowErr).Once()
		temporalClient.On("GetWorkflow", ctx, "workflow-1", "run-1").Return(run).Once()

		result, err := service.WaitForResult(ctx, "workflow-1", "run-1")
		require.Equal(t, RenewalResult{}, result)
		require.ErrorIs(t, err, workflowErr)
	})
}

func newClientTestService(t *testing.T) (*Service, *temporalmocks.Client) {
	t.Helper()
	temporalClient := temporalmocks.NewClient(t)
	service, err := NewService(temporalClient, TaskQueue, DefaultDunningPolicy())
	require.NoError(t, err)
	return service, temporalClient
}

func clientTestRenewalInput() RenewalInput {
	cycleStart := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	return RenewalInput{
		PatientID:       "patient-42",
		PlanAmountCents: 29900,
		CycleStart:      cycleStart,
		CycleEnd:        cycleStart.AddDate(0, 1, 0),
	}
}
