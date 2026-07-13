package validation

import (
	stdctx "context"

	"cpip/internal/execution/config"
	"cpip/internal/execution/job"
	"cpip/internal/execution/language"
)

// Authorizer decides whether a principal may execute a request. It is injected
// so the authorization policy lives outside the orchestrator.
type Authorizer interface {
	Authorize(ctx stdctx.Context, req *job.Request) (bool, error)
}

// AuthorizerFunc adapts a function to the Authorizer interface.
type AuthorizerFunc func(ctx stdctx.Context, req *job.Request) (bool, error)

// Authorize implements Authorizer.
func (f AuthorizerFunc) Authorize(ctx stdctx.Context, req *job.Request) (bool, error) {
	return f(ctx, req)
}

// AllowAll is an Authorizer that authorizes every request.
var AllowAll Authorizer = AuthorizerFunc(func(stdctx.Context, *job.Request) (bool, error) {
	return true, nil
})

// --- Authentication ---

// AuthenticationValidator rejects requests that are not authenticated when the
// configuration requires authentication.
type AuthenticationValidator struct{ Required bool }

func (v AuthenticationValidator) Name() string { return "authentication" }

func (v AuthenticationValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if v.Required && !req.Authenticated {
		return job.ErrUnauthenticated
	}
	return nil
}

// --- Authorization ---

// AuthorizationValidator delegates to an injected Authorizer.
type AuthorizationValidator struct {
	Enabled    bool
	Authorizer Authorizer
}

func (v AuthorizationValidator) Name() string { return "authorization" }

func (v AuthorizationValidator) Validate(ctx stdctx.Context, req *job.Request) error {
	if !v.Enabled {
		return nil
	}
	authz := v.Authorizer
	if authz == nil {
		authz = AllowAll
	}
	ok, err := authz.Authorize(ctx, req)
	if err != nil {
		return asRejection(job.ErrUnauthorized, "%v", err)
	}
	if !ok {
		return job.ErrUnauthorized
	}
	return nil
}

// --- Language ---

// LanguageValidator ensures the requested language is registered and runnable.
type LanguageValidator struct{ Registry *language.Registry }

func (v LanguageValidator) Name() string { return "language" }

func (v LanguageValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if req.Language == "" {
		return asRejection(job.ErrUnsupportedLanguage, "no language specified")
	}
	if _, err := v.Registry.Runnable(req.Language); err != nil {
		return asRejection(job.ErrUnsupportedLanguage, "%q", req.Language)
	}
	return nil
}

// --- Code size ---

// CodeSizeValidator enforces the maximum source-code size.
type CodeSizeValidator struct{ Max int64 }

func (v CodeSizeValidator) Name() string { return "code_size" }

func (v CodeSizeValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if req.Source == "" {
		return asRejection(job.ErrInvalidRequest, "empty source")
	}
	if int64(len(req.Source)) > v.Max {
		return asRejection(job.ErrCodeTooLarge, "%d > %d bytes", len(req.Source), v.Max)
	}
	return nil
}

// --- Input size ---

// InputSizeValidator enforces the maximum standard-input size.
type InputSizeValidator struct{ Max int64 }

func (v InputSizeValidator) Name() string { return "input_size" }

func (v InputSizeValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if int64(len(req.Stdin)) > v.Max {
		return asRejection(job.ErrStdinTooLarge, "%d > %d bytes", len(req.Stdin), v.Max)
	}
	return nil
}

// --- Timeout ---

// TimeoutValidator enforces the timeout bounds. A zero timeout is accepted here
// and resolved to the default at job construction; a negative or over-limit
// timeout is rejected.
type TimeoutValidator struct{ Max, Default int64 } // nanoseconds

func (v TimeoutValidator) Name() string { return "timeout" }

func (v TimeoutValidator) Validate(_ stdctx.Context, req *job.Request) error {
	t := req.Timeout.Nanoseconds()
	if t < 0 {
		return asRejection(job.ErrInvalidTimeout, "negative timeout")
	}
	if t > v.Max {
		return asRejection(job.ErrInvalidTimeout, "%s > max", req.Timeout)
	}
	return nil
}

// --- Memory / resource profile ---

// MemoryProfileValidator ensures a requested resource profile stays within the
// global ceiling and the language's own limits.
type MemoryProfileValidator struct {
	Max      int64
	Registry *language.Registry
}

func (v MemoryProfileValidator) Name() string { return "memory_profile" }

func (v MemoryProfileValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if req.Resources == nil {
		return nil // will inherit the language default
	}
	rp := req.Resources
	if rp.MemoryBytes < 0 || rp.CPUMillicores < 0 || rp.PidsLimit < 0 || rp.TmpfsBytes < 0 {
		return asRejection(job.ErrInvalidResourceProfile, "negative resource value")
	}
	if rp.MemoryBytes > v.Max {
		return asRejection(job.ErrInvalidResourceProfile, "memory %d > max %d", rp.MemoryBytes, v.Max)
	}
	if lang, err := v.Registry.Get(req.Language); err == nil && !lang.Limits.IsZero() {
		if rp.MemoryBytes > lang.Limits.MemoryBytes && lang.Limits.MemoryBytes > 0 {
			return asRejection(job.ErrInvalidResourceProfile, "memory over language limit")
		}
		if rp.CPUMillicores > lang.Limits.CPUMillicores && lang.Limits.CPUMillicores > 0 {
			return asRejection(job.ErrInvalidResourceProfile, "cpu over language limit")
		}
	}
	return nil
}

// --- Priority ---

// PriorityValidator enforces the configured priority range.
type PriorityValidator struct{ Min, Max job.Priority }

func (v PriorityValidator) Name() string { return "priority" }

func (v PriorityValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if req.Priority < v.Min || req.Priority > v.Max {
		return asRejection(job.ErrInvalidPriority, "%d not in [%d,%d]", req.Priority, v.Min, v.Max)
	}
	return nil
}

// --- Metadata ---

// MetadataValidator enforces metadata cardinality and key/value length limits.
type MetadataValidator struct {
	MaxEntries  int
	MaxKeyLen   int
	MaxValueLen int
}

func (v MetadataValidator) Name() string { return "metadata" }

func (v MetadataValidator) Validate(_ stdctx.Context, req *job.Request) error {
	if len(req.Metadata) > v.MaxEntries {
		return asRejection(job.ErrInvalidMetadata, "%d entries > max %d", len(req.Metadata), v.MaxEntries)
	}
	for k, val := range req.Metadata {
		if k == "" {
			return asRejection(job.ErrInvalidMetadata, "empty metadata key")
		}
		if len(k) > v.MaxKeyLen {
			return asRejection(job.ErrInvalidMetadata, "metadata key too long")
		}
		if len(val) > v.MaxValueLen {
			return asRejection(job.ErrInvalidMetadata, "metadata value too long for %q", k)
		}
	}
	return nil
}

// DefaultValidators builds the standard validator chain from configuration, the
// language registry, and an authorizer. Additional custom validators are
// appended after the standard ones. The order is intentional: cheap identity
// checks first, payload-size checks next, resource checks last.
func DefaultValidators(cfg config.Config, langs *language.Registry, authz Authorizer, custom ...Validator) []Validator {
	base := []Validator{
		AuthenticationValidator{Required: cfg.RequireAuthentication},
		AuthorizationValidator{Enabled: cfg.EnableAuthorization, Authorizer: authz},
		LanguageValidator{Registry: langs},
		CodeSizeValidator{Max: cfg.MaxCodeSize},
		InputSizeValidator{Max: cfg.MaxStdinSize},
		TimeoutValidator{Max: cfg.MaxTimeout.Nanoseconds(), Default: cfg.DefaultTimeout.Nanoseconds()},
		MemoryProfileValidator{Max: cfg.MaxMemoryBytes, Registry: langs},
		PriorityValidator{Min: cfg.MinPriority, Max: cfg.MaxPriority},
		MetadataValidator{MaxEntries: cfg.MaxMetadataEntries, MaxKeyLen: cfg.MaxMetadataKeyLen, MaxValueLen: cfg.MaxMetadataValueLen},
	}
	return append(base, custom...)
}
