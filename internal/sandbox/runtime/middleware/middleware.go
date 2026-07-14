package middleware

import (
	"context"
	"io"
	"time"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/runtime/logger"
	"cpip/internal/sandbox/runtime/sdk"
)

// LoggingMiddleware wraps any sdk.ExecutionAPI to inject structured trace logging.
type LoggingMiddleware struct {
	next sdk.ExecutionAPI
}

// NewLoggingMiddleware instantiates the logging decorator.
func NewLoggingMiddleware(next sdk.ExecutionAPI) sdk.ExecutionAPI {
	return &LoggingMiddleware{next: next}
}

func (m *LoggingMiddleware) CreateSandbox(ctx context.Context, sandboxID string, cfg runtime.ContainerConfig) (string, error) {
	start := time.Now()
	logger.Info("Creating sandbox", "sandbox_id", sandboxID, "image", cfg.Image)
	id, err := m.next.CreateSandbox(ctx, sandboxID, cfg)
	logger.Info("Sandbox created", "sandbox_id", sandboxID, "container_id", id, "duration", time.Since(start).String(), "error", err)
	return id, err
}

func (m *LoggingMiddleware) StartSandbox(ctx context.Context, sandboxID string) error {
	start := time.Now()
	logger.Info("Starting sandbox", "sandbox_id", sandboxID)
	err := m.next.StartSandbox(ctx, sandboxID)
	logger.Info("Sandbox started", "sandbox_id", sandboxID, "duration", time.Since(start).String(), "error", err)
	return err
}

func (m *LoggingMiddleware) StopSandbox(ctx context.Context, sandboxID string, timeout time.Duration) error {
	start := time.Now()
	logger.Info("Stopping sandbox", "sandbox_id", sandboxID)
	err := m.next.StopSandbox(ctx, sandboxID, timeout)
	logger.Info("Sandbox stopped", "sandbox_id", sandboxID, "duration", time.Since(start).String(), "error", err)
	return err
}

func (m *LoggingMiddleware) DestroySandbox(ctx context.Context, sandboxID string) error {
	start := time.Now()
	logger.Info("Destroying sandbox", "sandbox_id", sandboxID)
	err := m.next.DestroySandbox(ctx, sandboxID)
	logger.Info("Sandbox destroyed", "sandbox_id", sandboxID, "duration", time.Since(start).String(), "error", err)
	return err
}

func (m *LoggingMiddleware) PrepareWorkspace(sandboxID string) (string, error) {
	return m.next.PrepareWorkspace(sandboxID)
}

func (m *LoggingMiddleware) CopyFiles(ctx context.Context, sandboxID string, files map[string]string) error {
	return m.next.CopyFiles(ctx, sandboxID, files)
}

func (m *LoggingMiddleware) CollectLogs(ctx context.Context, sandboxID string, stdout, stderr io.Writer) error {
	return m.next.CollectLogs(ctx, sandboxID, stdout, stderr)
}

func (m *LoggingMiddleware) Cleanup(sandboxID string) error {
	return m.next.Cleanup(sandboxID)
}
