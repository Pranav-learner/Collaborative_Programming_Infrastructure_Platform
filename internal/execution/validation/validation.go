// Package validation implements a pluggable validation pipeline for execution
// requests. Each validator is an independent, testable unit; the Pipeline runs
// them in order and aggregates their verdicts. Validators never mutate the
// request — they only accept or reject — so the pipeline is safe to run
// concurrently for distinct requests.
package validation

import (
	stdctx "context"
	"fmt"
	"time"

	"cpip/internal/execution/job"
	"cpip/internal/execution/metrics"
)

// Validator is a single stage in the validation pipeline. Name identifies the
// stage in errors and metrics; Validate returns nil to accept, or an error
// (wrapping a job sentinel) to reject.
type Validator interface {
	Name() string
	Validate(ctx stdctx.Context, req *job.Request) error
}

// Failure records a single validator's rejection.
type Failure struct {
	Validator string
	Err       error
}

// Result is the aggregate verdict of a pipeline run.
type Result struct {
	Failures []Failure
	Duration time.Duration
}

// OK reports whether the request passed every validator.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// Err collapses the failures into a single error wrapping job.ErrValidationFailed,
// or nil when the result is OK. The first failure's sentinel is preserved so
// callers can errors.Is against specific causes (e.g. ErrUnsupportedLanguage).
func (r Result) Err() error {
	if r.OK() {
		return nil
	}
	first := r.Failures[0]
	// Wrap so both ErrValidationFailed and the specific sentinel match errors.Is.
	return fmt.Errorf("%w: %s: %w", job.ErrValidationFailed, first.Validator, first.Err)
}

// Reason returns a short human-readable description of the first failure.
func (r Result) Reason() string {
	if r.OK() {
		return ""
	}
	return fmt.Sprintf("%s: %v", r.Failures[0].Validator, r.Failures[0].Err)
}

// Pipeline runs an ordered list of validators. It is immutable after
// construction and safe for concurrent use.
type Pipeline struct {
	validators []Validator
	metrics    metrics.Recorder
	// stopOnFirst short-circuits on the first failure when true (the default);
	// when false every validator runs and all failures are collected.
	stopOnFirst bool
}

// Option customizes a Pipeline.
type Option func(*Pipeline)

// WithMetrics injects a metrics recorder.
func WithMetrics(m metrics.Recorder) Option {
	return func(p *Pipeline) {
		if m != nil {
			p.metrics = m
		}
	}
}

// CollectAll makes the pipeline run every validator and collect all failures
// rather than stopping at the first.
func CollectAll() Option {
	return func(p *Pipeline) { p.stopOnFirst = false }
}

// NewPipeline constructs a Pipeline from an ordered set of validators.
func NewPipeline(validators []Validator, opts ...Option) *Pipeline {
	p := &Pipeline{
		validators:  validators,
		metrics:     metrics.NewNoop(),
		stopOnFirst: true,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Validators returns the ordered validator names, for introspection.
func (p *Pipeline) Validators() []string {
	names := make([]string, len(p.validators))
	for i, v := range p.validators {
		names[i] = v.Name()
	}
	return names
}

// clock is overridable in tests; production uses time.Now.
var clock = time.Now

// Validate runs the request through the pipeline and returns the aggregate
// result. A nil request is itself a validation failure.
func (p *Pipeline) Validate(ctx stdctx.Context, req *job.Request) Result {
	start := clock()
	var res Result

	if req == nil {
		res.Failures = append(res.Failures, Failure{Validator: "request", Err: job.ErrInvalidRequest})
		res.Duration = clock().Sub(start)
		return res
	}

	for _, v := range p.validators {
		if err := ctx.Err(); err != nil {
			res.Failures = append(res.Failures, Failure{Validator: v.Name(), Err: err})
			break
		}
		if err := v.Validate(ctx, req); err != nil {
			res.Failures = append(res.Failures, Failure{Validator: v.Name(), Err: err})
			p.metrics.ValidationFailed(v.Name())
			if p.stopOnFirst {
				break
			}
		}
	}

	res.Duration = clock().Sub(start)
	return res
}

// ValidatorFunc adapts a function to the Validator interface, for custom
// validators supplied by callers.
type ValidatorFunc struct {
	Name_     string
	Validate_ func(ctx stdctx.Context, req *job.Request) error
}

// Name implements Validator.
func (f ValidatorFunc) Name() string { return f.Name_ }

// Validate implements Validator.
func (f ValidatorFunc) Validate(ctx stdctx.Context, req *job.Request) error {
	return f.Validate_(ctx, req)
}

// asRejection wraps a sentinel as a rejection with a human-readable detail,
// preserving the sentinel for errors.Is matching by callers.
func asRejection(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
