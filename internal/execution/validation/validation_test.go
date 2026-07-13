package validation

import (
	stdctx "context"
	"errors"
	"testing"
	"time"

	"cpip/internal/execution/config"
	"cpip/internal/execution/job"
	"cpip/internal/execution/language"
)

func validReq() *job.Request {
	return &job.Request{
		Language: "go", Source: "package main", Priority: job.PriorityNormal,
		Authenticated: true, Timeout: 5 * time.Second,
	}
}

func TestIndividualValidators(t *testing.T) {
	ctx := stdctx.Background()
	langs := language.Default()

	tests := []struct {
		name    string
		v       Validator
		req     *job.Request
		wantErr error
	}{
		{"auth ok", AuthenticationValidator{Required: true}, validReq(), nil},
		{"auth missing", AuthenticationValidator{Required: true}, &job.Request{}, job.ErrUnauthenticated},
		{"auth not required", AuthenticationValidator{Required: false}, &job.Request{}, nil},
		{"language ok", LanguageValidator{Registry: langs}, validReq(), nil},
		{"language unknown", LanguageValidator{Registry: langs}, &job.Request{Language: "cobol"}, job.ErrUnsupportedLanguage},
		{"code ok", CodeSizeValidator{Max: 100}, validReq(), nil},
		{"code empty", CodeSizeValidator{Max: 100}, &job.Request{Source: ""}, job.ErrInvalidRequest},
		{"code too large", CodeSizeValidator{Max: 4}, &job.Request{Source: "toolong"}, job.ErrCodeTooLarge},
		{"stdin too large", InputSizeValidator{Max: 2}, &job.Request{Stdin: "abc"}, job.ErrStdinTooLarge},
		{"timeout over max", TimeoutValidator{Max: int64(time.Second)}, &job.Request{Timeout: time.Minute}, job.ErrInvalidTimeout},
		{"timeout negative", TimeoutValidator{Max: int64(time.Minute)}, &job.Request{Timeout: -1}, job.ErrInvalidTimeout},
		{"priority out of range", PriorityValidator{Min: job.PriorityLow, Max: job.PriorityHigh}, &job.Request{Priority: job.PriorityCritical}, job.ErrInvalidPriority},
		{"metadata too many", MetadataValidator{MaxEntries: 1, MaxKeyLen: 10, MaxValueLen: 10}, &job.Request{Metadata: map[string]string{"a": "1", "b": "2"}}, job.ErrInvalidMetadata},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.v.Validate(ctx, tt.req)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want wrapping %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemoryProfileValidator(t *testing.T) {
	langs := language.Default()
	v := MemoryProfileValidator{Max: 1 << 30, Registry: langs}
	over := &job.Request{Language: "python3", Resources: &job.ResourceProfile{MemoryBytes: 1 << 40}}
	if err := v.Validate(stdctx.Background(), over); !errors.Is(err, job.ErrInvalidResourceProfile) {
		t.Fatalf("err = %v, want ErrInvalidResourceProfile", err)
	}
	// Nil resources inherit the language default and must pass.
	if err := v.Validate(stdctx.Background(), &job.Request{Language: "python3"}); err != nil {
		t.Fatalf("nil resources should pass: %v", err)
	}
}

func TestPipelineStopsOnFirst(t *testing.T) {
	cfg := config.Default()
	langs := language.Default()
	p := NewPipeline(DefaultValidators(cfg, langs, AllowAll))

	// Unauthenticated AND unknown language: stop-on-first returns exactly one failure.
	res := p.Validate(stdctx.Background(), &job.Request{Language: "cobol"})
	if res.OK() {
		t.Fatal("expected failure")
	}
	if len(res.Failures) != 1 {
		t.Fatalf("failures = %d, want 1 (stop-on-first)", len(res.Failures))
	}
	if !errors.Is(res.Err(), job.ErrValidationFailed) {
		t.Fatalf("Err() must wrap ErrValidationFailed: %v", res.Err())
	}
}

func TestPipelineCollectAll(t *testing.T) {
	cfg := config.Default()
	cfg.RequireAuthentication = true
	langs := language.Default()
	p := NewPipeline(DefaultValidators(cfg, langs, AllowAll), CollectAll())

	res := p.Validate(stdctx.Background(), &job.Request{Language: "cobol", Source: ""})
	if res.OK() || len(res.Failures) < 2 {
		t.Fatalf("collect-all expected multiple failures, got %d", len(res.Failures))
	}
}

func TestPipelineHappyPath(t *testing.T) {
	cfg := config.Default()
	p := NewPipeline(DefaultValidators(cfg, language.Default(), AllowAll))
	res := p.Validate(stdctx.Background(), validReq())
	if !res.OK() {
		t.Fatalf("valid request rejected: %s", res.Reason())
	}
}

func TestCustomValidator(t *testing.T) {
	cfg := config.Default()
	sentinel := errors.New("banned token")
	custom := ValidatorFunc{
		Name_: "banned_tokens",
		Validate_: func(_ stdctx.Context, req *job.Request) error {
			if req.Source == "rm -rf" {
				return sentinel
			}
			return nil
		},
	}
	p := NewPipeline(DefaultValidators(cfg, language.Default(), AllowAll, custom))
	res := p.Validate(stdctx.Background(), &job.Request{Language: "go", Source: "rm -rf", Authenticated: true})
	if res.OK() || !errors.Is(res.Failures[len(res.Failures)-1].Err, sentinel) {
		t.Fatalf("custom validator not enforced: %+v", res.Failures)
	}
}

func TestNilRequestRejected(t *testing.T) {
	p := NewPipeline(nil)
	if res := p.Validate(stdctx.Background(), nil); res.OK() {
		t.Fatal("nil request must be rejected")
	}
}
