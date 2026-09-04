package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/marcossnikel/futurhealth-renewal-takehome/internal/simulated"
	"github.com/marcossnikel/futurhealth-renewal-takehome/pkg/renewal"
	temporalclient "go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

const (
	temporalCLIVersion = "v1.8.3"
	initialAmountCents = 29_900
	updatedAmountCents = 39_900
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, output, errorOutput io.Writer) error {
	uiPort, err := freeLocalPort()
	if err != nil {
		return fmt.Errorf("select Temporal UI port: %w", err)
	}
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		return fmt.Errorf("locate user cache: %w", err)
	}
	temporalCache := filepath.Join(cacheRoot, "futurhealth-renewal-takehome", "temporal")
	if err := os.MkdirAll(temporalCache, 0o755); err != nil {
		return fmt.Errorf("create Temporal CLI cache: %w", err)
	}

	fmt.Fprintf(output, "Preparing Temporal CLI %s (the first run downloads and caches it)...\n", temporalCLIVersion)
	temporalLogger := temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(errorOutput, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{
			Version: temporalCLIVersion,
			DestDir: temporalCache,
		},
		ClientOptions: &temporalclient.Options{
			Namespace: temporalclient.DefaultNamespace,
			Logger:    temporalLogger,
		},
		EnableUI: true,
		UIPort:   uiPort,
		LogLevel: "error",
		Stdout:   io.Discard,
		Stderr:   errorOutput,
	})
	if err != nil {
		return fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() { _ = server.Stop() }()

	client := server.Client()
	defer client.Close()

	logger := slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: slog.LevelInfo}))
	activities := &renewal.Activities{
		Processor: simulated.NewProcessor(logger),
		Sink:      simulated.NewSink(logger),
	}
	renewalWorker := worker.New(client, renewal.TaskQueue, worker.Options{})
	renewal.RegisterWorker(renewalWorker, activities)
	if err := renewalWorker.Start(); err != nil {
		return fmt.Errorf("start renewal worker: %w", err)
	}
	defer renewalWorker.Stop()

	service, err := renewal.NewService(client, renewal.TaskQueue, renewal.DunningPolicy{
		RetryDelays:      []time.Duration{3 * time.Second},
		ResolutionWindow: 30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create renewal service: %w", err)
	}

	now := time.Now().UTC()
	cycleStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	input := renewal.RenewalInput{
		PatientID:       fmt.Sprintf("demo-patient-%d", now.UnixNano()),
		PlanAmountCents: initialAmountCents,
		CycleStart:      cycleStart,
		CycleEnd:        cycleStart.AddDate(0, 1, 0),
	}

	scenarioCtx, cancelScenario := context.WithTimeout(ctx, time.Minute)
	defer cancelScenario()
	started, err := service.StartRenewal(scenarioCtx, input)
	if err != nil {
		return fmt.Errorf("start demo renewal: %w", err)
	}

	baseURL := "http://127.0.0.1:" + uiPort
	workflowURL := fmt.Sprintf(
		"%s/namespaces/%s/workflows/%s/%s/history",
		baseURL,
		url.PathEscape(temporalclient.DefaultNamespace),
		url.PathEscape(started.WorkflowID),
		url.PathEscape(started.RunID),
	)
	fmt.Fprintf(output, "\nTemporal UI:  %s\n", baseURL)
	fmt.Fprintf(output, "Workflow:     %s\n", workflowURL)
	fmt.Fprintf(output, "Workflow ID:  %s\n", started.WorkflowID)
	fmt.Fprintf(output, "Run ID:       %s\n\n", started.RunID)
	fmt.Fprintln(output, "Scenario: decline -> plan change during backoff -> retry -> success")

	result, err := demonstrateRenewal(scenarioCtx, output, service, started)
	if err != nil {
		return err
	}
	if result.Resolution != renewal.ResolutionPaid || result.Attempts != 2 || result.AmountCents != updatedAmountCents {
		return fmt.Errorf("unexpected result: %+v", result)
	}

	fmt.Fprintf(output, "\nCompleted: resolution=%s attempts=%d amount_cents=%d\n", result.Resolution, result.Attempts, result.AmountCents)
	fmt.Fprintln(output, "The UI will remain available until you press Ctrl+C.")
	<-ctx.Done()
	return nil
}

func demonstrateRenewal(
	ctx context.Context,
	output io.Writer,
	service *renewal.Service,
	started renewal.StartedRenewal,
) (renewal.RenewalResult, error) {
	status, err := waitForStatus(ctx, service, started.WorkflowID, "first charge submission", func(status renewal.RenewalStatus) bool {
		return status.Attempt == 1 && status.Phase == renewal.PhaseAwaiting && status.Submission == renewal.SubmissionAccepted
	})
	if err != nil {
		return renewal.RenewalResult{}, err
	}
	fmt.Fprintf(output, "1. First charge submitted:  attempt=%d amount_cents=%d\n", status.Attempt, status.ActiveAmountCents)

	if err := service.SendPaymentResult(ctx, started.WorkflowID, renewal.PaymentResultSignal{
		Succeeded:      false,
		ProcessorTxnID: "demo-declined-1",
	}); err != nil {
		return renewal.RenewalResult{}, fmt.Errorf("send declined payment: %w", err)
	}
	status, err = waitForStatus(ctx, service, started.WorkflowID, "retry backoff", func(status renewal.RenewalStatus) bool {
		return status.Attempt == 1 && status.Phase == renewal.PhaseBackingOff
	})
	if err != nil {
		return renewal.RenewalResult{}, err
	}
	fmt.Fprintf(output, "2. Payment declined:        phase=%s\n", status.Phase)

	if err := service.ChangePlan(ctx, started.WorkflowID, renewal.PlanChangeSignal{NewAmountCents: updatedAmountCents}); err != nil {
		return renewal.RenewalResult{}, fmt.Errorf("send plan change: %w", err)
	}
	status, err = waitForStatus(ctx, service, started.WorkflowID, "second charge submission", func(status renewal.RenewalStatus) bool {
		return status.Attempt == 2 &&
			status.Phase == renewal.PhaseAwaiting &&
			status.Submission == renewal.SubmissionAccepted &&
			status.ActiveAmountCents == updatedAmountCents
	})
	if err != nil {
		return renewal.RenewalResult{}, err
	}
	fmt.Fprintf(output, "3. Plan changed for retry:  attempt=%d amount_cents=%d\n", status.Attempt, status.ActiveAmountCents)

	if err := service.SendPaymentResult(ctx, started.WorkflowID, renewal.PaymentResultSignal{
		Succeeded:          true,
		ProcessorTxnID:     "demo-approved-2",
		AmountChargedCents: updatedAmountCents,
	}); err != nil {
		return renewal.RenewalResult{}, fmt.Errorf("send successful payment: %w", err)
	}
	result, err := service.WaitForResult(ctx, started.WorkflowID, started.RunID)
	if err != nil {
		return renewal.RenewalResult{}, fmt.Errorf("wait for renewal result: %w", err)
	}
	fmt.Fprintln(output, "4. Retry payment approved and terminal event emitted.")
	return result, nil
}

func waitForStatus(
	ctx context.Context,
	service *renewal.Service,
	workflowID string,
	description string,
	matches func(renewal.RenewalStatus) bool,
) (renewal.RenewalStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	var lastStatus renewal.RenewalStatus
	var queried bool
	for {
		status, err := service.RenewalStatus(waitCtx, workflowID)
		if err == nil && matches(status) {
			return status, nil
		}
		if err == nil {
			lastStatus = status
			lastErr = nil
			queried = true
		} else {
			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return renewal.RenewalStatus{}, fmt.Errorf("wait for %s: %w (last query error: %v)", description, waitCtx.Err(), lastErr)
			}
			if queried {
				return renewal.RenewalStatus{}, fmt.Errorf("wait for %s: %w (last status: %+v)", description, waitCtx.Err(), lastStatus)
			}
			return renewal.RenewalStatus{}, fmt.Errorf("wait for %s: %w", description, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func freeLocalPort() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(address)
	return port, err
}
