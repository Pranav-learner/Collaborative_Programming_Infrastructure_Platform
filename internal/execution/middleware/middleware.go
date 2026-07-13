// Package middleware provides composable decorators over the orchestrator's
// request-facing surface (the Submitter). Middleware adds cross-cutting concerns
// — structured logging, panic recovery, tracing — without the orchestrator
// depending on any of them. It mirrors the familiar net/http middleware pattern:
// each decorator wraps a next Submitter and returns a Submitter.
package middleware

import (
	stdctx "context"
	"fmt"
	"log/slog"
	"time"

	"cpip/internal/execution/job"
	execlog "cpip/internal/execution/logger"
)

// Submitter is the request-facing surface of the orchestrator that middleware
// decorates. The concrete *orchestrator.Orchestrator satisfies it structurally.
type Submitter interface {
	SubmitExecution(ctx stdctx.Context, req job.Request) (job.Job, error)
	Cancel(ctx stdctx.Context, jobID string) error
	Retry(ctx stdctx.Context, jobID string) error
}

// Middleware wraps a Submitter, returning a decorated Submitter.
type Middleware func(Submitter) Submitter

// Chain composes middleware left-to-right around a base Submitter: the first
// middleware in the list is the outermost layer.
func Chain(base Submitter, mws ...Middleware) Submitter {
	for i := len(mws) - 1; i >= 0; i-- {
		base = mws[i](base)
	}
	return base
}

// --- Logging -----------------------------------------------------------------

// Logging logs the outcome and latency of each request-facing call.
func Logging(base *slog.Logger) Middleware {
	log := execlog.Named(base, "middleware")
	return func(next Submitter) Submitter {
		return &loggingSubmitter{next: next, log: log}
	}
}

type loggingSubmitter struct {
	next Submitter
	log  *slog.Logger
	clk  func() time.Time
}

func (s *loggingSubmitter) now() time.Time {
	if s.clk != nil {
		return s.clk()
	}
	return time.Now()
}

func (s *loggingSubmitter) SubmitExecution(ctx stdctx.Context, req job.Request) (job.Job, error) {
	start := s.now()
	j, err := s.next.SubmitExecution(ctx, req)
	if err != nil {
		s.log.Warn("submit rejected", "request_id", req.RequestID, "language", req.Language, "err", err)
		return j, err
	}
	s.log.Info("submit accepted", "job_id", j.ID, "language", j.Language,
		"priority", j.Priority.String(), "latency_ms", s.since(start))
	return j, nil
}

func (s *loggingSubmitter) Cancel(ctx stdctx.Context, jobID string) error {
	err := s.next.Cancel(ctx, jobID)
	if err != nil {
		s.log.Warn("cancel failed", "job_id", jobID, "err", err)
	} else {
		s.log.Info("cancel accepted", "job_id", jobID)
	}
	return err
}

func (s *loggingSubmitter) Retry(ctx stdctx.Context, jobID string) error {
	err := s.next.Retry(ctx, jobID)
	if err != nil {
		s.log.Warn("retry failed", "job_id", jobID, "err", err)
	} else {
		s.log.Info("retry accepted", "job_id", jobID)
	}
	return err
}

func (s *loggingSubmitter) since(start time.Time) int64 { return s.now().Sub(start).Milliseconds() }

// --- Recovery ----------------------------------------------------------------

// Recovery converts a panic in the wrapped Submitter into an error, so a single
// malformed request can never crash the orchestrator process.
func Recovery(base *slog.Logger) Middleware {
	log := execlog.Named(base, "middleware")
	return func(next Submitter) Submitter {
		return &recoverySubmitter{next: next, log: log}
	}
}

type recoverySubmitter struct {
	next Submitter
	log  *slog.Logger
}

func (s *recoverySubmitter) SubmitExecution(ctx stdctx.Context, req job.Request) (j job.Job, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in submit", "request_id", req.RequestID, "panic", r)
			err = fmt.Errorf("%w: panic: %v", job.ErrInvalidRequest, r)
		}
	}()
	return s.next.SubmitExecution(ctx, req)
}

func (s *recoverySubmitter) Cancel(ctx stdctx.Context, jobID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in cancel", "job_id", jobID, "panic", r)
			err = fmt.Errorf("cancel panic: %v", r)
		}
	}()
	return s.next.Cancel(ctx, jobID)
}

func (s *recoverySubmitter) Retry(ctx stdctx.Context, jobID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in retry", "job_id", jobID, "panic", r)
			err = fmt.Errorf("retry panic: %v", r)
		}
	}()
	return s.next.Retry(ctx, jobID)
}
