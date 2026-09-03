package main

import (
	"log/slog"
	"os"

	"github.com/marcossnikel/futurhealth-renewal-takehome/internal/simulated"
	"github.com/marcossnikel/futurhealth-renewal-takehome/pkg/renewal"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	client, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  environment("TEMPORAL_ADDRESS", temporalclient.DefaultHostPort),
		Namespace: environment("TEMPORAL_NAMESPACE", temporalclient.DefaultNamespace),
	})
	if err != nil {
		logger.Error("connect to Temporal", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	processor := simulated.NewProcessor(logger)
	sink := simulated.NewSink(logger)
	activities := &renewal.Activities{Processor: processor, Sink: sink}

	renewalWorker := worker.New(client, renewal.TaskQueue, worker.Options{})
	renewalWorker.RegisterWorkflowWithOptions(renewal.RenewalWorkflow, workflow.RegisterOptions{Name: renewal.WorkflowName})
	renewalWorker.RegisterActivityWithOptions(activities.AttemptCharge, activity.RegisterOptions{Name: renewal.AttemptChargeActivityName})
	renewalWorker.RegisterActivityWithOptions(activities.EmitPaymentEvent, activity.RegisterOptions{Name: renewal.EmitPaymentActivityName})
	renewalWorker.RegisterActivityWithOptions(activities.EmitCancellationEvent, activity.RegisterOptions{Name: renewal.EmitCancellationActivityName})

	logger.Info("renewal worker started", "task_queue", renewal.TaskQueue)
	if err := renewalWorker.Run(worker.InterruptCh()); err != nil {
		logger.Error("run renewal worker", "error", err)
		os.Exit(1)
	}
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
