package middleware

import (
	"context"
	"net/http"

	"cpip/internal/reliability/sdk"
)

// DecorateFunc wraps an execution function with a reliability policy using Background context.
func DecorateFunc(client *sdk.Client, policyName string, next func() error) func() error {
	return func() error {
		return client.Protect(context.Background(), policyName, next)
	}
}

// HTTPMiddleware intercepts HTTP transactions and injects rate limits, circuit breakers, and retries.
func HTTPMiddleware(client *sdk.Client, policyName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := client.Protect(r.Context(), policyName, func() error {
			next.ServeHTTP(w, r)
			return nil
		})

		if err != nil {
			// Handle standard resilience failures with correct HTTP status codes
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error": "service temporarily unavailable", "detail": "` + err.Error() + `"}`))
		}
	})
}
