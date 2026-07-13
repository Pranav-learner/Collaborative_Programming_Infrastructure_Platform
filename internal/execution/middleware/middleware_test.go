package middleware

import (
	stdctx "context"
	"errors"
	"testing"

	"cpip/internal/execution/job"
	"cpip/internal/execution/logger"
)

// fakeSubmitter records calls and can be made to panic or error.
type fakeSubmitter struct {
	submitCalls int
	panicOn     bool
	err         error
}

func (f *fakeSubmitter) SubmitExecution(_ stdctx.Context, req job.Request) (job.Job, error) {
	f.submitCalls++
	if f.panicOn {
		panic("boom")
	}
	if f.err != nil {
		return job.Job{}, f.err
	}
	return job.Job{ID: "j1", Language: req.Language}, nil
}
func (f *fakeSubmitter) Cancel(stdctx.Context, string) error { return f.err }
func (f *fakeSubmitter) Retry(stdctx.Context, string) error  { return f.err }

func TestChainInvokesBase(t *testing.T) {
	base := &fakeSubmitter{}
	s := Chain(base, Recovery(logger.Discard()), Logging(logger.Discard()))
	j, err := s.SubmitExecution(stdctx.Background(), job.Request{Language: "go"})
	if err != nil || j.Language != "go" {
		t.Fatalf("submit through chain: %+v err %v", j, err)
	}
	if base.submitCalls != 1 {
		t.Fatalf("base called %d times", base.submitCalls)
	}
}

func TestRecoveryConvertsPanicToError(t *testing.T) {
	base := &fakeSubmitter{panicOn: true}
	s := Chain(base, Recovery(logger.Discard()))
	_, err := s.SubmitExecution(stdctx.Background(), job.Request{})
	if !errors.Is(err, job.ErrInvalidRequest) {
		t.Fatalf("panic not converted: %v", err)
	}
}

func TestLoggingPassesThroughError(t *testing.T) {
	sentinel := errors.New("nope")
	base := &fakeSubmitter{err: sentinel}
	s := Chain(base, Logging(logger.Discard()))
	if err := s.Cancel(stdctx.Background(), "j1"); !errors.Is(err, sentinel) {
		t.Fatalf("error not propagated: %v", err)
	}
}
