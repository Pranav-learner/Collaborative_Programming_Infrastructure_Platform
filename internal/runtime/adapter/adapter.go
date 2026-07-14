package adapter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// CompileRequest defines parameters for compile step.
type CompileRequest struct {
	SessionID string
	Source    string
	Options   []string
}

// CompileResult carries outcomes of the compilation.
type CompileResult struct {
	ExecutablePath string
	Duration       time.Duration
	Output         string
	Success        bool
}

// RunInput carries input parameters for program execution.
type RunInput struct {
	SessionID      string
	ExecutablePath string
	Source         string
	Stdin          string
	Args           []string
	Env            map[string]string
	Stdout         io.Writer
	Stderr         io.Writer
	ShutdownGrace  time.Duration
	OnStart        func(pid int)
}

// RunResult carries outcomes of execution.
type RunResult struct {
	ExitCode int
	Duration time.Duration
	PID      int
}

// LanguageAdapter compiles, runs, and cleans up execution workloads.
type LanguageAdapter interface {
	Validate(ctx context.Context, source string) error
	Compile(ctx context.Context, req CompileRequest) (CompileResult, error)
	Run(ctx context.Context, input RunInput) (RunResult, error)
	Cleanup(ctx context.Context, sessionID string) error
}

// BaseAdapter implements common helper routines for all adapters.
type BaseAdapter struct{}

// GetWorkspaceDir resolves a session-specific workspace folder under the project.
func (b *BaseAdapter) GetWorkspaceDir(sessionID string) string {
	cwd, _ := os.Getwd()
	return filepath.Join(cwd, "runtime_workspaces", sessionID)
}

// CreateWorkspaceDir creates and returns the directory path.
func (b *BaseAdapter) CreateWorkspaceDir(sessionID string) (string, error) {
	dir := b.GetWorkspaceDir(sessionID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("failed to create workspace dir: %w", err)
	}
	return dir, nil
}

// CleanupWorkspace deletes the session directory.
func (b *BaseAdapter) CleanupWorkspace(sessionID string) error {
	dir := b.GetWorkspaceDir(sessionID)
	return os.RemoveAll(dir)
}

// RunCmd executes a process with graceful SIGTERM -> SIGKILL escalation on cancel/timeout.
func (b *BaseAdapter) RunCmd(
	ctx context.Context,
	name string,
	args []string,
	dir string,
	stdin string,
	stdout,
	stderr io.Writer,
	env map[string]string,
	grace time.Duration,
	onStart func(pid int),
) (RunResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// Writable environment
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	start := time.Now()
	// To support escalation, we handle the cancel function manually
	// We override CommandContext's default SIGKILL trigger by creating standard Cmd
	// but using a custom cancel callback, or simply starting standard Cmd and monitoring ctx.Done.
	// Since CommandContext by default sends SIGKILL immediately, let's use standard exec.Command
	// and run our own monitoring loop to support SIGTERM -> SIGKILL escalation.
	cmdEscalated := exec.Command(name, args...)
	cmdEscalated.Dir = dir
	if stdin != "" {
		cmdEscalated.Stdin = strings.NewReader(stdin)
	}
	cmdEscalated.Stdout = stdout
	cmdEscalated.Stderr = stderr
	cmdEscalated.Env = cmd.Env

	if err := cmdEscalated.Start(); err != nil {
		return RunResult{ExitCode: -1}, err
	}

	pid := cmdEscalated.Process.Pid
	if onStart != nil {
		onStart(pid)
	}
	done := make(chan error, 1)
	go func() {
		done <- cmdEscalated.Wait()
	}()

	select {
	case err := <-done:
		duration := time.Since(start)
		exitCode := 0
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
					exitCode = status.ExitStatus()
				} else {
					exitCode = -1
				}
			} else {
				exitCode = -1
			}
		}
		return RunResult{ExitCode: exitCode, Duration: duration, PID: pid}, nil

	case <-ctx.Done():
		// Context cancelled or timed out!
		// 1. Send SIGTERM
		_ = cmdEscalated.Process.Signal(syscall.SIGTERM)

		select {
		case <-done:
			// exited cleanly after SIGTERM
		case <-time.After(grace):
			// force termination via SIGKILL
			_ = cmdEscalated.Process.Signal(syscall.SIGKILL)
			<-done
		}

		duration := time.Since(start)
		return RunResult{
			ExitCode: -1,
			Duration: duration,
			PID:      pid,
		}, ctx.Err()
	}
}

// Registry stores all registered language adapters.
type AdapterRegistry struct {
	mu       sync.RWMutex
	adapters map[string]LanguageAdapter
}

// NewAdapterRegistry creates an AdapterRegistry with default host executors.
func NewAdapterRegistry() *AdapterRegistry {
	r := &AdapterRegistry{
		adapters: make(map[string]LanguageAdapter),
	}
	r.adapters["python3"] = &PythonAdapter{}
	r.adapters["go"] = &GoAdapter{}
	r.adapters["bash"] = &BashAdapter{}
	r.adapters["c"] = &CAdapter{}
	r.adapters["cpp"] = &CppAdapter{}
	r.adapters["java"] = &JavaAdapter{}
	return r
}

// Get retrieves the adapter for the given language.
func (ar *AdapterRegistry) Get(lang string) (LanguageAdapter, error) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	a, ok := ar.adapters[lang]
	if !ok {
		return nil, errors.New("unsupported language adapter: " + lang)
	}
	return a, nil
}

// Register registers a custom adapter.
func (ar *AdapterRegistry) Register(lang string, adapter LanguageAdapter) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.adapters[lang] = adapter
}
