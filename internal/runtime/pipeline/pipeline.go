package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	execjob "cpip/internal/execution/job"
	execlang "cpip/internal/execution/language"
	"cpip/internal/runtime/adapter"
	"cpip/internal/runtime/buffer"
	"cpip/internal/runtime/config"
	"cpip/internal/runtime/events"
	"cpip/internal/runtime/metrics"
	"cpip/internal/runtime/stream"
	"cpip/internal/runtime/types"
)

// Pipeline orchestrates the step-by-step execution lifecycle of a single job.
type Pipeline struct {
	cfg        config.Config
	adapters   *adapter.AdapterRegistry
	langReg    *execlang.Registry
	bus        *events.Bus
	metrics    metrics.Recorder
	log        *slog.Logger
	now        func() time.Time

	mu      sync.Mutex
	session types.Session
	stream  *stream.StreamManager
}

// NewPipeline creates a new Pipeline instance.
func NewPipeline(
	cfg config.Config,
	adapters *adapter.AdapterRegistry,
	langReg *execlang.Registry,
	bus *events.Bus,
	metrics metrics.Recorder,
	log *slog.Logger,
) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		adapters: adapters,
		langReg:  langReg,
		bus:      bus,
		metrics:  metrics,
		log:      log.With("subsystem", "pipeline"),
		now:      time.Now,
	}
}

// GetSession returns a snapshot of the runtime session.
func (p *Pipeline) GetSession() types.Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

// GetStreamManager returns the stream manager for this pipeline.
func (p *Pipeline) GetStreamManager() *stream.StreamManager {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stream
}

// Execute runs the job end-to-end: Init -> Load -> Compile -> Run -> Stream -> Stats -> Cleanup.
func (p *Pipeline) Execute(ctx context.Context, job execjob.Job, workerID string) (types.Session, error) {
	pCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ensure cleanup is always performed on exit
	defer func() {
		clCtx, clCancel := context.WithTimeout(context.Background(), p.cfg.CleanupTimeout)
		defer clCancel()
		if ad, err := p.adapters.Get(job.Language); err == nil {
			if cleanupErr := ad.Cleanup(clCtx, job.ID); cleanupErr != nil {
				p.log.Error("failed to cleanup runtime workspace", "session_id", job.ID, "err", cleanupErr)
			}
		}
		p.publishEvent(events.CleanupCompleted, nil)
	}()

	p.mu.Lock()
	p.stream = stream.NewStreamManager(job.ID, job.ID, job.CorrelationID, job.Language, p.bus, p.cfg.BufferSize)
	p.session = types.Session{
		ID:            job.ID, // use JobID as SessionID for simpler correlation
		JobID:         job.ID,
		WorkerID:      workerID,
		CorrelationID: job.CorrelationID,
		Language:      job.Language,
		State:         types.StateCreated,
		Resource: types.ResourceProfile{
			Tier:          job.Resources.Tier,
			MemoryBytes:   job.Resources.MemoryBytes,
			CPUMillicores: job.Resources.CPUMillicores,
			PidsLimit:     job.Resources.PidsLimit,
			TmpfsBytes:    job.Resources.TmpfsBytes,
			WallTimeout:   job.Resources.WallTimeout,
		},
		CreatedAt: p.now(),
		UpdatedAt: p.now(),
		Context:   pCtx,
		Cancel:    cancel,
	}
	p.mu.Unlock()

	p.publishEvent(events.RuntimeCreated, nil)

	// 1. Resolve Language Adapter
	ad, err := p.adapters.Get(job.Language)
	if err != nil {
		return p.fail(types.ErrInvalidLanguage, "invalid language: "+job.Language)
	}

	langMeta, err := p.langReg.Get(job.Language)
	if err != nil {
		// Fallback to adapter defaults if not in registry
		langMeta = execlang.Language{CompileRequired: false}
	}

	// 2. Validate code
	if err := ad.Validate(pCtx, job.Source); err != nil {
		return p.fail(err, "validation failed")
	}

	// 3. Prepare workspace / Compile
	executablePath := ""
	if langMeta.CompileRequired {
		p.transition(types.StateCompiling)
		p.publishEvent(events.CompilationStarted, nil)
	}

	cCtx, cCancel := context.WithTimeout(pCtx, p.cfg.CompileTimeout)
	compileStart := p.now()
	compileRes, err := ad.Compile(cCtx, adapter.CompileRequest{
		SessionID: job.ID,
		Source:    job.Source,
		Options:   job.CompilerOptions,
	})
	cCancel()
	compileDur := p.now().Sub(compileStart)

	if langMeta.CompileRequired {
		p.mu.Lock()
		p.session.Compiler = types.CompilerState{
			Compiled:      compileRes.Success,
			Duration:      compileDur,
			OutputSummary: compileRes.Output,
		}
		if err != nil {
			p.session.Compiler.Error = err.Error()
		}
		p.mu.Unlock()

		p.metrics.RecordCompilation(job.Language, compileDur, compileRes.Success)

		if err != nil || !compileRes.Success {
			p.publishEvent(events.CompilationFinished, p.session.Compiler)
			return p.fail(types.ErrCompilationFailed, "compilation failed: "+compileRes.Output)
		}

		p.publishEvent(events.CompilationFinished, p.session.Compiler)
	} else {
		if err != nil || !compileRes.Success {
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			} else {
				errMsg = "compile success false"
			}
			return p.fail(types.ErrExecutionFailed, "failed to write source code: "+errMsg)
		}
	}
	executablePath = compileRes.ExecutablePath

	// 4. Setup Streaming & Buffers
	p.transition(types.StateRunning)
	p.publishEvent(events.ExecutionStarted, nil)



	defer p.stream.Close()

	// Enforce strict limit-bounded buffering
	sharedCounter := buffer.NewSharedCounter()
	var overflowOnce sync.Once
	onOverflow := func() {
		overflowOnce.Do(func() {
			p.log.Warn("output buffer limit exceeded, cancelling session", "job_id", job.ID)
			cancel() // cancel pipeline context immediately to kill process
		})
	}

	stdoutBuf := buffer.NewLimitBuffer(p.cfg.MaxStdoutSize, p.cfg.MaxOutputSize, sharedCounter, onOverflow, func(chunk []byte) {
		p.stream.WriteStdout(chunk)
	})
	stderrBuf := buffer.NewLimitBuffer(p.cfg.MaxStderrSize, p.cfg.MaxOutputSize, sharedCounter, onOverflow, func(chunk []byte) {
		p.stream.WriteStderr(chunk)
	})

	// Run execution under wall-clock timeout
	runTimeout := job.Timeout
	if runTimeout <= 0 {
		runTimeout = p.cfg.RunTimeout
	}
	rCtx, rCancel := context.WithTimeout(pCtx, runTimeout)
	defer rCancel()

	// Start periodic progress reporting ticker
	progressDone := make(chan struct{})
	runStart := p.now()

	var runnerPID int
	var pidMu sync.Mutex

	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(p.cfg.StreamingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-rCtx.Done():
				return
			case <-ticker.C:
				pidMu.Lock()
				pid := runnerPID
				pidMu.Unlock()

				_, mem := getProcessMetrics(pid)
				p.stream.WriteProgress(stream.Progress{
					Timestamp: p.now(),
					Duration:  p.now().Sub(runStart),
					CPUUsage:  0.0,
					MemUsage:  mem,
				})
			}
		}
	}()

	runRes, err := ad.Run(rCtx, adapter.RunInput{
		SessionID:      job.ID,
		ExecutablePath: executablePath,
		Source:         job.Source,
		Stdin:          job.Stdin,
		Args:           job.ExecutionOptions.Args,
		Env:            job.ExecutionOptions.Env,
		Stdout:         stdoutBuf,
		Stderr:         stderrBuf,
		ShutdownGrace:  p.cfg.ShutdownGrace,
		OnStart: func(pid int) {
			pidMu.Lock()
			runnerPID = pid
			pidMu.Unlock()
		},
	})

	// Capture the context error before triggering cancel
	runCtxErr := rCtx.Err()

	// Stop progress collection
	rCancel()
	<-progressDone

	runDur := p.now().Sub(runStart)

	p.mu.Lock()
	p.session.Runner = types.RunnerState{
		PID:      runRes.PID,
		ExitCode: runRes.ExitCode,
		Duration: runDur,
	}
	if err != nil {
		p.session.Runner.Error = err.Error()
	}

	p.session.Stats = types.SessionStats{
		CompileTime:       p.session.Compiler.Duration,
		ExecutionTime:     runDur,
		TotalTime:         p.now().Sub(p.session.CreatedAt),
		BytesStdoutStream: stdoutBuf.Len(),
		BytesStderrStream: stderrBuf.Len(),
	}
	p.mu.Unlock()

	// 6. Evaluate outcome
	var terminalState types.SessionState
	var finalErr error

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(runCtxErr, context.DeadlineExceeded) {
		terminalState = types.StateTimedOut
		finalErr = types.ErrTimeout
		p.metrics.RecordTimeout(job.Language, "run")
	} else if errors.Is(err, context.Canceled) || errors.Is(runCtxErr, context.Canceled) {
		// Distinguish overflow vs cancellation vs user cancel
		if sharedCounter.Value() >= p.cfg.MaxOutputSize || stdoutBuf.Len() >= p.cfg.MaxStdoutSize || stderrBuf.Len() >= p.cfg.MaxStderrSize {
			terminalState = types.StateFailed
			finalErr = types.ErrOutputLimitExceeded
		} else {
			terminalState = types.StateCancelled
			finalErr = types.ErrCancelled
			p.metrics.RecordCancellation(job.Language, "context")
		}
	} else if err != nil {
		terminalState = types.StateFailed
		finalErr = fmt.Errorf("%w: %v", types.ErrExecutionFailed, err)
	} else if runRes.ExitCode != 0 {
		terminalState = types.StateFailed
		finalErr = fmt.Errorf("%w: exit code %d", types.ErrExecutionFailed, runRes.ExitCode)
	} else {
		terminalState = types.StateCompleted
	}

	p.transition(terminalState)
	p.metrics.RecordExecution(job.Language, runDur, string(terminalState))
	p.metrics.RecordBytesStreamed("stdout", stdoutBuf.Len())
	p.metrics.RecordBytesStreamed("stderr", stderrBuf.Len())

	if finalErr != nil {
		p.publishEvent(events.ExecutionFailed, finalErr.Error())
	} else {
		p.publishEvent(events.ExecutionCompleted, nil)
	}

	p.mu.Lock()
	sessSnapshot := p.session
	p.mu.Unlock()

	return sessSnapshot, finalErr
}

func (p *Pipeline) transition(state types.SessionState) {
	p.mu.Lock()
	p.session.State = state
	p.session.UpdatedAt = p.now()
	p.mu.Unlock()
}

func (p *Pipeline) publishEvent(t events.Type, payload any) {
	p.mu.Lock()
	sess := p.session
	p.mu.Unlock()

	if p.bus != nil {
		p.bus.Publish(events.Event{
			Type:          t,
			SessionID:     sess.ID,
			JobID:         sess.JobID,
			CorrelationID: sess.CorrelationID,
			Language:      sess.Language,
			Payload:       payload,
		})
	}
}

func (p *Pipeline) fail(err error, reason string) (types.Session, error) {
	p.transition(types.StateFailed)
	p.publishEvent(events.ExecutionFailed, reason+": "+err.Error())
	p.mu.Lock()
	sess := p.session
	p.mu.Unlock()
	return sess, err
}

func getProcessMetrics(pid int) (float64, int64) {
	if pid <= 0 {
		return 0, 0
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err == nil {
		var size, rss int64
		_, err = fmt.Sscanf(string(data), "%d %d", &size, &rss)
		if err == nil {
			return 0.0, rss * 4096
		}
	}
	return 0.0, 0
}
