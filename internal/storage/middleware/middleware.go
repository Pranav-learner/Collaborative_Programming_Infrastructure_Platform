// Package middleware provides cross-cutting helpers for the storage module:
// context propagation of caller identity and correlation IDs, a reusable
// ownership-based Authorizer for the download pipeline, and a tracing-hook seam.
// These are small, dependency-light utilities the API/service layers compose;
// they impose no framework and hold no global state.
package middleware

import (
	"context"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/download"
)

type ctxKey int

const (
	ctxOwner ctxKey = iota
	ctxRequestID
	ctxRoles
)

// WithOwner attaches the calling principal (user/service id) to ctx.
func WithOwner(ctx context.Context, owner string) context.Context {
	return context.WithValue(ctx, ctxOwner, owner)
}

// OwnerFrom returns the principal attached to ctx, or "" if none.
func OwnerFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxOwner).(string)
	return s
}

// WithRequestID attaches a correlation/request id to ctx for tracing.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxRequestID, id)
}

// RequestIDFrom returns the correlation id attached to ctx, or "" if none.
func RequestIDFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxRequestID).(string)
	return s
}

// WithRoles attaches the caller's roles to ctx (e.g. {"admin"}).
func WithRoles(ctx context.Context, roles ...string) context.Context {
	return context.WithValue(ctx, ctxRoles, roles)
}

// RolesFrom returns the caller's roles from ctx.
func RolesFrom(ctx context.Context) []string {
	r, _ := ctx.Value(ctxRoles).([]string)
	return r
}

// HasRole reports whether the caller in ctx holds role.
func HasRole(ctx context.Context, role string) bool {
	for _, r := range RolesFrom(ctx) {
		if r == role {
			return true
		}
	}
	return false
}

// OwnershipAuthorizer returns a download.Authorizer that permits a read when the
// artifact has no owner (public), when the caller owns it, or when the caller
// holds the admin role. It reads identity from the request context, so callers
// must populate it via WithOwner/WithRoles upstream. adminRole names the role
// that bypasses the ownership check (e.g. "admin").
func OwnershipAuthorizer(adminRole string) download.Authorizer {
	return func(ctx context.Context, a *artifacts.Artifact) error {
		if a.Owner == "" {
			return nil // public artifact
		}
		if adminRole != "" && HasRole(ctx, adminRole) {
			return nil
		}
		if OwnerFrom(ctx) == a.Owner {
			return nil
		}
		return artifacts.ErrUnauthorized
	}
}

// Tracer is the tracing-hook seam. A real implementation bridges to
// OpenTelemetry; the default is a no-op. Start returns a finish function invoked
// (deferred) when the operation completes.
type Tracer interface {
	Start(ctx context.Context, operation string) (context.Context, func(err error))
}

// NoopTracer is a Tracer that does nothing.
type NoopTracer struct{}

// Start implements Tracer.
func (NoopTracer) Start(ctx context.Context, _ string) (context.Context, func(error)) {
	return ctx, func(error) {}
}

var _ Tracer = NoopTracer{}
