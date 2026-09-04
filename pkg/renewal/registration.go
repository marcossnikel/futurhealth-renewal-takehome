package renewal

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// RegisterWorker registers the renewal workflow and its Activities on a worker.
func RegisterWorker(registry worker.Registry, activities *Activities) {
	registry.RegisterWorkflowWithOptions(RenewalWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	registry.RegisterActivityWithOptions(activities.AttemptCharge, activity.RegisterOptions{Name: AttemptChargeActivityName})
	registry.RegisterActivityWithOptions(activities.EmitPaymentEvent, activity.RegisterOptions{Name: EmitPaymentActivityName})
	registry.RegisterActivityWithOptions(activities.EmitCancellationEvent, activity.RegisterOptions{Name: EmitCancellationActivityName})
}
