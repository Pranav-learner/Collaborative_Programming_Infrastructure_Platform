package manager

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	execjob "cpip/internal/execution/job"
	execlang "cpip/internal/execution/language"
	"cpip/internal/runtime/config"
	"cpip/internal/runtime/events"
	"cpip/internal/runtime/metrics"
	"cpip/internal/runtime/types"
)

func setupTestManager(t *testing.T) (*Manager, *execlang.Registry) {
	cfg := config.Config{
		MaxStdoutSize:     1024 * 10, // 10KB
		MaxStderrSize:     1024 * 10,
		MaxOutputSize:     1024 * 15, // 15KB total
		CompileTimeout:    10 * time.Second,
		RunTimeout:        5 * time.Second,
		CleanupTimeout:    2 * time.Second,
		ShutdownGrace:     200 * time.Millisecond,
		StreamingInterval: 50 * time.Millisecond,
		BufferSize:        100,
	}

	langReg := execlang.NewRegistry()
	_ = langReg.Register(execlang.Language{
		ID:              "python3",
		CompileRequired: false,
		Status:          execlang.StatusStable,
	})
	_ = langReg.Register(execlang.Language{
		ID:              "go",
		CompileRequired: true,
		Status:          execlang.StatusStable,
	})
	_ = langReg.Register(execlang.Language{
		ID:              "bash",
		CompileRequired: false,
		Status:          execlang.StatusStable,
	})

	bus := events.NewBus(events.Options{})
	rec := metrics.NewInMemRecorder()
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mgr := NewManager(cfg, langReg, bus, rec, log)
	return mgr, langReg
}

func TestExecuteJob_Python3_Success(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-python",
		CorrelationID: "corr-123",
		Language:      "python3",
		Source:        `print("hello from python")`,
	}

	// Capture events
	var collectedEvents []events.Event
	var mu sync.Mutex
	ch := mgr.Events().Subscribe(100)
	defer mgr.Events().Unsubscribe(ch)

	go func() {
		for ev := range ch {
			mu.Lock()
			collectedEvents = append(collectedEvents, ev)
			mu.Unlock()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, streamMgr, err := mgr.ExecuteJob(ctx, job, "worker-1")
	if err != nil {
		t.Fatalf("ExecuteJob failed: %v", err)
	}

	if sess.State != types.StateCompleted {
		t.Errorf("expected StateCompleted, got %v", sess.State)
	}
	if sess.Runner.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", sess.Runner.ExitCode)
	}

	// Read stream output
	var stdout []byte
	var streamClosed bool
	for {
		select {
		case chunk, ok := <-streamMgr.Stdout():
			if !ok {
				streamClosed = true
				break
			}
			stdout = append(stdout, chunk...)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout reading from stdout channel")
		}
		if streamClosed {
			break
		}
	}

	if string(stdout) != "hello from python\n" {
		t.Errorf("expected stdout 'hello from python\\n', got %q", string(stdout))
	}

	// Verify events were published
	mu.Lock()
	defer mu.Unlock()
	hasStarted := false
	hasCompleted := false
	for _, ev := range collectedEvents {
		if ev.Type == events.ExecutionStarted {
			hasStarted = true
		}
		if ev.Type == events.ExecutionCompleted {
			hasCompleted = true
		}
	}
	if !hasStarted {
		t.Error("missing ExecutionStarted event")
	}
	if !hasCompleted {
		t.Error("missing ExecutionCompleted event")
	}
}

func TestExecuteJob_Go_Success(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-go",
		CorrelationID: "corr-456",
		Language:      "go",
		Source: `package main
import "fmt"
func main() {
	fmt.Println("hello from compiled go")
}`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sess, streamMgr, err := mgr.ExecuteJob(ctx, job, "worker-1")
	if err != nil {
		t.Fatalf("ExecuteJob failed: %v", err)
	}

	if sess.State != types.StateCompleted {
		t.Errorf("expected StateCompleted, got %v", sess.State)
	}

	// Read stream output
	var stdout []byte
	var streamClosed bool
	for {
		select {
		case chunk, ok := <-streamMgr.Stdout():
			if !ok {
				streamClosed = true
				break
			}
			stdout = append(stdout, chunk...)
		case <-time.After(3 * time.Second):
			t.Fatal("timeout reading from stdout channel")
		}
		if streamClosed {
			break
		}
	}

	if string(stdout) != "hello from compiled go\n" {
		t.Errorf("expected stdout 'hello from compiled go\\n', got %q", string(stdout))
	}
}

func TestExecuteJob_Timeout(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-timeout",
		CorrelationID: "corr-timeout",
		Language:      "python3",
		Source: `import time
time.sleep(2)
print("done")`,
		Timeout: 400 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, _, err := mgr.ExecuteJob(ctx, job, "worker-1")
	if !errors.Is(err, types.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got error: %v", err)
	}

	if sess.State != types.StateTimedOut {
		t.Errorf("expected StateTimedOut, got %v", sess.State)
	}
}

func TestExecuteJob_BufferOverflow(t *testing.T) {
	mgr, _ := setupTestManager(t)
	// We set MaxStdoutSize to 10KB. Let's output 20KB.
	job := execjob.Job{
		ID:            "test-job-overflow",
		CorrelationID: "corr-overflow",
		Language:      "python3",
		Source:        `print("A" * 20000)`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, _, err := mgr.ExecuteJob(ctx, job, "worker-1")
	if !errors.Is(err, types.ErrOutputLimitExceeded) {
		t.Fatalf("expected ErrOutputLimitExceeded, got error: %v", err)
	}

	if sess.State != types.StateFailed {
		t.Errorf("expected StateFailed, got %v", sess.State)
	}
}

func TestExecuteJob_Cancellation(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-cancel",
		CorrelationID: "corr-cancel",
		Language:      "python3",
		Source: `import time
time.sleep(5)
print("done")`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Cancel after 200ms
	go func() {
		time.Sleep(200 * time.Millisecond)
		ok := mgr.CancelSession(job.ID)
		if !ok {
			t.Errorf("failed to cancel session")
		}
	}()

	sess, _, err := mgr.ExecuteJob(ctx, job, "worker-1")
	if !errors.Is(err, types.ErrCancelled) {
		t.Fatalf("expected ErrCancelled, got error: %v", err)
	}

	if sess.State != types.StateCancelled {
		t.Errorf("expected StateCancelled, got %v", sess.State)
	}
}

func TestExecuteJob_InvalidLanguage(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-invalid",
		CorrelationID: "corr-invalid",
		Language:      "unsupported-lang",
		Source:        "some code",
	}

	_, _, err := mgr.ExecuteJob(context.Background(), job, "worker-1")
	if !errors.Is(err, types.ErrInvalidLanguage) {
		t.Fatalf("expected ErrInvalidLanguage, got: %v", err)
	}
}

func TestExecuteJob_CompilerError(t *testing.T) {
	mgr, _ := setupTestManager(t)
	job := execjob.Job{
		ID:            "test-job-compile-err",
		CorrelationID: "corr-compile-err",
		Language:      "go",
		Source: `package main
func main() {
	this_is_an_error
}`,
	}

	_, _, err := mgr.ExecuteJob(context.Background(), job, "worker-1")
	if !errors.Is(err, types.ErrCompilationFailed) {
		t.Fatalf("expected ErrCompilationFailed, got: %v", err)
	}
}
