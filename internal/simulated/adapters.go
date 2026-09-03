package simulated

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/marcossnikel/futurhealth-renewal-takehome/pkg/renewal"
)

type Processor struct {
	logger   *slog.Logger
	mu       sync.Mutex
	requests map[string]renewal.ChargeRequest
}

func NewProcessor(logger *slog.Logger) *Processor {
	return &Processor{logger: logger, requests: make(map[string]renewal.ChargeRequest)}
}

func (p *Processor) SubmitCharge(_ context.Context, request renewal.ChargeRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if previous, exists := p.requests[request.IdempotencyKey]; exists {
		parametersConflict := previous.PatientID != request.PatientID || previous.AmountCents != request.AmountCents
		if parametersConflict {
			return errors.New("charge idempotency key reused with different parameters")
		}
		p.logger.Info("charge submission deduplicated", "attempt_id", request.IdempotencyKey)
		return nil
	}
	p.requests[request.IdempotencyKey] = request
	p.logger.Info("charge submitted to mock processor",
		"attempt_id", request.IdempotencyKey,
		"amount_cents", request.AmountCents,
	)
	return nil
}

type Sink struct {
	logger      *slog.Logger
	mu          sync.Mutex
	resolutions map[string]renewal.Resolution
}

func NewSink(logger *slog.Logger) *Sink {
	return &Sink{logger: logger, resolutions: make(map[string]renewal.Resolution)}
}

func (s *Sink) PublishPayment(_ context.Context, eventID string, event renewal.PaymentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reserve(eventID, renewal.ResolutionPaid); err != nil {
		return err
	}
	s.logger.Info("payment event accepted",
		"event_id", eventID,
		"amount_cents", event.AmountCents,
		"occurred_at", event.EmittedAt,
	)
	return nil
}

func (s *Sink) PublishCancellation(_ context.Context, eventID string, event renewal.CancellationEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reserve(eventID, renewal.ResolutionCancelled); err != nil {
		return err
	}
	s.logger.Info("cancellation event accepted",
		"event_id", eventID,
		"reason", event.Reason,
		"occurred_at", event.EmittedAt,
	)
	return nil
}

func (s *Sink) reserve(eventID string, resolution renewal.Resolution) error {
	if existing, found := s.resolutions[eventID]; found {
		if existing != resolution {
			return errors.New("resolution event ID reused for conflicting event types")
		}
		return nil
	}
	s.resolutions[eventID] = resolution
	return nil
}
