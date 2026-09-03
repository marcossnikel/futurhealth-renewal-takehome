package renewal

import (
	"context"
	"errors"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	temporalclient "go.temporal.io/sdk/client"
)

var (
	ErrRenewalAlreadyExists = errors.New("renewal already exists")
	ErrRenewalNotFound      = errors.New("renewal not found")
)

type StartedRenewal struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

type Service struct {
	client    temporalclient.Client
	taskQueue string
	policy    DunningPolicy
}

func NewService(client temporalclient.Client, taskQueue string, policy DunningPolicy) (*Service, error) {
	if client == nil {
		return nil, errors.New("Temporal client is required")
	}
	if taskQueue == "" {
		return nil, errors.New("task queue is required")
	}
	if err := ValidateDunningPolicy(policy); err != nil {
		return nil, fmt.Errorf("invalid dunning policy: %w", err)
	}
	policy.RetryDelays = slices.Clone(policy.RetryDelays)
	return &Service{client: client, taskQueue: taskQueue, policy: policy}, nil
}

func (s *Service) StartRenewal(ctx context.Context, input RenewalInput) (StartedRenewal, error) {
	workflowID, err := WorkflowID(input)
	if err != nil {
		return StartedRenewal{}, err
	}

	options := temporalclient.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                s.taskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		StaticSummary:                            "Subscription renewal and dunning",
	}
	run, err := s.client.ExecuteWorkflow(ctx, options, WorkflowName, WorkflowInput{
		Renewal: input,
		Policy: DunningPolicy{
			RetryDelays:      slices.Clone(s.policy.RetryDelays),
			ResolutionWindow: s.policy.ResolutionWindow,
		},
	})
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return StartedRenewal{WorkflowID: workflowID}, fmt.Errorf("%w: %s", ErrRenewalAlreadyExists, workflowID)
		}
		return StartedRenewal{}, fmt.Errorf("start renewal workflow: %w", err)
	}
	return StartedRenewal{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (s *Service) SendPaymentResult(ctx context.Context, workflowID string, signal PaymentResultSignal) error {
	if err := ValidateWorkflowID(workflowID); err != nil {
		return err
	}
	if err := ValidatePaymentResultSignal(signal); err != nil {
		return err
	}
	return translateTemporalError(s.client.SignalWorkflow(ctx, workflowID, "", PaymentResultSignalName, signal))
}

func (s *Service) ChangePlan(ctx context.Context, workflowID string, signal PlanChangeSignal) error {
	if err := ValidateWorkflowID(workflowID); err != nil {
		return err
	}
	if err := ValidatePlanChangeSignal(signal); err != nil {
		return err
	}
	return translateTemporalError(s.client.SignalWorkflow(ctx, workflowID, "", PlanChangeSignalName, signal))
}

func (s *Service) CancelRenewal(ctx context.Context, workflowID string) error {
	if err := ValidateWorkflowID(workflowID); err != nil {
		return err
	}
	return translateTemporalError(s.client.SignalWorkflow(ctx, workflowID, "", CancelRequestSignalName, struct{}{}))
}

func (s *Service) RenewalStatus(ctx context.Context, workflowID string) (RenewalStatus, error) {
	if err := ValidateWorkflowID(workflowID); err != nil {
		return RenewalStatus{}, err
	}
	value, err := s.client.QueryWorkflow(ctx, workflowID, "", StatusQueryName)
	if err != nil {
		return RenewalStatus{}, translateTemporalError(err)
	}
	var status RenewalStatus
	if err := value.Get(&status); err != nil {
		return RenewalStatus{}, fmt.Errorf("decode renewal status: %w", err)
	}
	return status, nil
}

func (s *Service) WaitForResult(ctx context.Context, workflowID, runID string) (RenewalResult, error) {
	var result RenewalResult
	if err := s.client.GetWorkflow(ctx, workflowID, runID).Get(ctx, &result); err != nil {
		return RenewalResult{}, translateTemporalError(err)
	}
	return result, nil
}

func translateTemporalError(err error) error {
	if err == nil {
		return nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %v", ErrRenewalNotFound, err)
	}
	return err
}
