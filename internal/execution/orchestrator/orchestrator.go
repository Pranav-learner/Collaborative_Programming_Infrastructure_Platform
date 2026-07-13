// Package orchestrator implements the execution orchestrator: the single source
// of truth for execution-request lifecycle management. It accepts requests,
// runs them through the validation pipeline, creates and registers jobs, builds
// their execution contexts, hands them to the scheduler, and drives every
// lifecycle transition — while never executing code itself.
//
// The orchestrator composes the registry, validation pipeline, context manager,
// scheduler, event bus, and metrics/logging seams. It is safe for concurrent
// use: all mutable job state lives in the registry behind its lock, and every
// state change flows through the registry's atomic Transition.
package orchestrator

import (
	stdctx "context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cpip/internal/execution/config"
	execctx "cpip/internal/execution/context"
	"cpip/internal/execution/events"
	"cpip/internal/execution/job"
	"cpip/internal/execution/language"
	execlog "cpip/internal/execution/logger"
	"cpip/internal/execution/metrics"
	"cpip/internal/execution/registry"
	"cpip/internal/execution/scheduler"
	"cpip/internal/execution/storage"
	"cpip/internal/execution/validation"
	"cpip/internal/id"
)

// Orchestrator coordinates the execution lifecycle.
type Orchestrator struct {
	cfg      config.Config
	reg      *registry.Registry
	langs    *language.Registry
	pipeline *validation.Pipeline
	ctxMgr   *execctx.Manager
	sched    scheduler.Scheduler
	bus      *events.Bus
	metrics  metrics.Recorder
	store    storage.Repository
	log      *slog.Logger
	now      func() time.Time
}

// Deps are the injected dependencies of the Orchestrator. All are required
// except Logger and NowFunc.
type Deps struct {
	Config    config.Config
	Registry  *registry.Registry
	Language  *language.Registry
	Pipeline  *validation.Pipeline
	Context   *execctx.Manager
	Scheduler scheduler.Scheduler
	Bus       *events.Bus
	Metrics   metrics.Recorder
	Store     storage.Repository
	Logger    *slog.Logger
	NowFunc   func() time.Time
}

// New constructs an Orchestrator from its dependencies.
func New(d Deps) *Orchestrator {
	if d.Metrics == nil {
		d.Metrics = metrics.NewNoop()
	}
	if d.NowFunc == nil {
		d.NowFunc = time.Now
	}
	return &Orchestrator{
		cfg:      d.Config,
		reg:      d.Registry,
		langs:    d.Language,
		pipeline: d.Pipeline,
		ctxMgr:   d.Context,
		sched:    d.Scheduler,
		bus:      d.Bus,
		metrics:  d.Metrics,
		store:    d.Store,
		log:      execlog.Named(d.Logger, "orchestrator"),
		now:      d.NowFunc,
	}
}

// SubmitExecution is the orchestrator's front door: it validates a request,
// creates and registers a job, builds its execution context, and schedules it.
// It returns an immutable snapshot of the created job, or an error wrapping the
// relevant job sentinel (e.g. job.ErrValidationFailed, job.ErrUnsupportedLanguage,
// job.ErrSchedulerUnavailable).
func (o *Orchestrator) SubmitExecution(ctx stdctx.Context, req job.Request) (job.Job, error) {
	o.metrics.ExecutionRequested()

	// Assign correlation identifiers up front so every event is traceable.
	if req.RequestID == "" {
		req.RequestID = id.NewWithPrefix("req")
	}
	if req.CorrelationID == "" {
		req.CorrelationID = id.NewWithPrefix("corr")
	}
	o.bus.Publish(events.Event{
		Type: events.ExecutionRequested, RequestID: req.RequestID, CorrelationID: req.CorrelationID,
		UserID: req.UserID, SessionID: req.SessionID, RoomID: req.RoomID, Language: req.Language,
	})

	// Validation pipeline.
	res := o.pipeline.Validate(ctx, &req)
	if !res.OK() {
		reason := res.Reason()
		o.metrics.ExecutionRejected(reason)
		o.bus.Publish(events.Event{
			Type: events.ExecutionRejected, RequestID: req.RequestID, CorrelationID: req.CorrelationID,
			UserID: req.UserID, RoomID: req.RoomID, Language: req.Language, Reason: reason,
		})
		return job.Job{}, res.Err()
	}
	o.metrics.ExecutionValidated(res.Duration.Milliseconds())
	o.bus.Publish(events.Event{
		Type: events.ExecutionValidated, RequestID: req.RequestID, CorrelationID: req.CorrelationID,
		UserID: req.UserID, RoomID: req.RoomID, Language: req.Language,
	})

	// Resolve language defaults for timeout and resource profile.
	now := o.now()
	defaults := o.resolveDefaults(req, now)
	j := job.New(req, defaults)

	if err := o.reg.Add(j); err != nil {
		return job.Job{}, fmt.Errorf("register job: %w", err)
	}
	o.metrics.JobCreated()
	o.publishJob(events.JobCreated, j, "")

	// Pending → Validated (bookkeeping; the ExecutionValidated event already fired).
	if _, err := o.reg.Transition(j.ID, job.StateValidated, nil); err != nil {
		return job.Job{}, fmt.Errorf("transition to validated: %w", err)
	}

	// Build the cancellable execution context.
	o.ctxMgr.Create(ctx, execctx.Spec{
		Job:     j,
		Tracing: execctx.Tracing{RequestID: req.RequestID, CorrelationID: req.CorrelationID},
		Security: execctx.SecurityMetadata{
			UserID: req.UserID, Roles: req.Roles, Authenticated: req.Authenticated,
			Authorized: true, NetworkAccess: req.ExecutionOptions.NetworkAccess,
		},
		Now: now,
	})

	// Validated → Queued, then schedule.
	scheduled, err := o.reg.Transition(j.ID, job.StateQueued, func(jj *job.Job) { jj.ScheduledAt = now })
	if err != nil {
		return job.Job{}, fmt.Errorf("transition to queued: %w", err)
	}
	_ = scheduled
	snap, _ := o.reg.Get(j.ID)

	if err := o.sched.Schedule(ctx, snap); err != nil {
		o.metrics.ScheduleFailed()
		o.failScheduling(ctx, j.ID, err)
		return job.Job{}, fmt.Errorf("schedule job: %w", err)
	}

	o.metrics.JobQueued()
	o.publishJob(events.JobQueued, snap, "")
	o.persist(ctx, snap)
	o.updateGauges()
	return snap, nil
}

// resolveDefaults computes the job defaults from config and the language registry.
func (o *Orchestrator) resolveDefaults(req job.Request, now time.Time) job.Defaults {
	timeout := req.Timeout
	var profile job.ResourceProfile
	if lang, err := o.langs.Get(req.Language); err == nil {
		profile = lang.Profile
		if timeout <= 0 && lang.DefaultTimeout > 0 {
			timeout = lang.DefaultTimeout
		}
	}
	if timeout <= 0 {
		timeout = o.cfg.DefaultTimeout
	}
	if timeout > o.cfg.MaxTimeout {
		timeout = o.cfg.MaxTimeout
	}
	return job.Defaults{
		ID:            id.NewWithPrefix("job"),
		RequestID:     req.RequestID,
		CorrelationID: req.CorrelationID,
		Now:           now,
		Timeout:       timeout,
		MaxRetries:    o.cfg.MaxRetries,
		Resources:     profile,
	}
}

// failScheduling rolls a job to Failed when the scheduler rejects it.
func (o *Orchestrator) failScheduling(ctx stdctx.Context, jobID string, cause error) {
	now := o.now()
	from, err := o.reg.Transition(jobID, job.StateFailed, func(j *job.Job) {
		j.Outcome = job.OutcomeFailure
		j.CompletedAt = now
	})
	if err != nil {
		o.log.Error("failed to mark job failed after schedule error", "job_id", jobID, "err", err)
		return
	}
	o.metrics.StateTransition(from.String(), job.StateFailed.String())
	o.metrics.JobFailed()
	snap, _ := o.reg.Get(jobID)
	o.publishJob(events.JobFailed, snap, cause.Error())
	o.ctxMgr.Release(jobID)
	o.persist(ctx, snap)
	o.updateGauges()
}

// Cancel cancels a job. It returns job.ErrJobNotFound if the job is unknown, or
// job.ErrCancellationConflict if the job has already finished.
func (o *Orchestrator) Cancel(ctx stdctx.Context, jobID string) error {
	now := o.now()
	from, err := o.reg.Transition(jobID, job.StateCancelled, func(j *job.Job) {
		j.Outcome = job.OutcomeCancelled
		j.CompletedAt = now
		j.CancelRequested = true
	})
	if err != nil {
		if errors.Is(err, job.ErrIllegalTransition) {
			o.metrics.IllegalTransition()
			return job.ErrCancellationConflict
		}
		return err
	}
	o.metrics.StateTransition(from.String(), job.StateCancelled.String())
	o.metrics.JobCancelled()

	o.ctxMgr.Cancel(jobID)
	_ = o.sched.Cancel(ctx, jobID)

	snap, _ := o.reg.Get(jobID)
	o.publishJob(events.JobCancelled, snap, "")
	o.ctxMgr.Release(jobID)
	o.persist(ctx, snap)
	o.updateGauges()
	return nil
}

// Retry re-enters a recoverable (failed or timed-out) job into the pipeline. It
// returns job.ErrRetryConflict if the job is not retryable, or
// job.ErrRetriesExhausted if it has reached its retry ceiling.
func (o *Orchestrator) Retry(ctx stdctx.Context, jobID string) error {
	snap, ok := o.reg.Get(jobID)
	if !ok {
		return job.ErrJobNotFound
	}
	if !snap.State.CanRetry() {
		return job.ErrRetryConflict
	}
	if snap.RetryCount >= snap.MaxRetries {
		return job.ErrRetriesExhausted
	}

	now := o.now()
	from, err := o.reg.Transition(jobID, job.StateRetrying, func(j *job.Job) {
		j.RetryCount++
		j.Outcome = job.OutcomeNone
		j.StartedAt = time.Time{}
		j.CompletedAt = time.Time{}
	})
	if err != nil {
		if errors.Is(err, job.ErrIllegalTransition) {
			o.metrics.IllegalTransition()
			return job.ErrRetryConflict
		}
		return err
	}
	o.metrics.StateTransition(from.String(), job.StateRetrying.String())
	o.metrics.JobRetried()

	retrying, _ := o.reg.Get(jobID)
	o.publishJob(events.JobRetried, retrying, "")

	// Fresh execution context (new deadline) for the retry attempt.
	o.ctxMgr.Create(ctx, execctx.Spec{
		Job:     retrying,
		Tracing: execctx.Tracing{RequestID: retrying.RequestID, CorrelationID: retrying.CorrelationID},
		Security: execctx.SecurityMetadata{
			UserID: retrying.UserID, Authenticated: true, Authorized: true,
			NetworkAccess: retrying.ExecutionOptions.NetworkAccess,
		},
		Now: now,
	})

	if _, err := o.reg.Transition(jobID, job.StateQueued, func(j *job.Job) { j.ScheduledAt = now }); err != nil {
		return fmt.Errorf("transition retry to queued: %w", err)
	}
	queued, _ := o.reg.Get(jobID)
	if err := o.sched.Retry(ctx, queued); err != nil {
		o.metrics.ScheduleFailed()
		o.failScheduling(ctx, jobID, err)
		return fmt.Errorf("reschedule job: %w", err)
	}
	o.metrics.JobQueued()
	o.publishJob(events.JobQueued, queued, "")
	o.persist(ctx, queued)
	o.updateGauges()
	return nil
}

// --- Lifecycle marks (called by future queue/worker/runtime modules) ---------

// MarkDispatched records that a worker claimed a queued job.
func (o *Orchestrator) MarkDispatched(ctx stdctx.Context, jobID, workerID string) error {
	o.ctxMgr.Assign(jobID, workerID, "")
	return o.advance(ctx, jobID, job.StateDispatched, events.JobDispatched, "", func(j *job.Job) {
		j.WorkerID = workerID
	}, o.metrics.JobDispatched)
}

// MarkStarted records that a dispatched job began executing.
func (o *Orchestrator) MarkStarted(ctx stdctx.Context, jobID string) error {
	now := o.now()
	return o.advance(ctx, jobID, job.StateRunning, events.JobStarted, "", func(j *job.Job) {
		j.StartedAt = now
	}, o.metrics.JobStarted)
}

// MarkStreaming records that a running job began streaming output.
func (o *Orchestrator) MarkStreaming(ctx stdctx.Context, jobID string) error {
	return o.advance(ctx, jobID, job.StateStreaming, 0, "", nil, nil)
}

// MarkCompleted records that a job finished successfully.
func (o *Orchestrator) MarkCompleted(ctx stdctx.Context, jobID string) error {
	now := o.now()
	err := o.advance(ctx, jobID, job.StateCompleted, events.JobCompleted, "", func(j *job.Job) {
		j.Outcome = job.OutcomeSuccess
		j.CompletedAt = now
	}, nil)
	if err == nil {
		if snap, ok := o.reg.Get(jobID); ok {
			o.metrics.JobCompleted(snap.Statistics().ExecTime.Milliseconds())
		}
		o.ctxMgr.Release(jobID)
		o.updateGauges()
	}
	return err
}

// MarkFailed records that a job failed, with a human-readable reason.
func (o *Orchestrator) MarkFailed(ctx stdctx.Context, jobID, reason string) error {
	now := o.now()
	err := o.advance(ctx, jobID, job.StateFailed, events.JobFailed, reason, func(j *job.Job) {
		j.Outcome = job.OutcomeFailure
		j.CompletedAt = now
	}, o.metrics.JobFailed)
	if err == nil {
		o.updateGauges()
	}
	return err
}

// MarkTimedOut records that a job exceeded its deadline.
func (o *Orchestrator) MarkTimedOut(ctx stdctx.Context, jobID string) error {
	now := o.now()
	err := o.advance(ctx, jobID, job.StateTimedOut, events.JobTimedOut, "", func(j *job.Job) {
		j.Outcome = job.OutcomeTimeout
		j.CompletedAt = now
	}, o.metrics.JobTimedOut)
	if err == nil {
		o.ctxMgr.Cancel(jobID)
		o.updateGauges()
	}
	return err
}

// advance is the shared transition helper for lifecycle marks: it performs the
// registry transition, records the state-transition metric, invokes an optional
// metric hook, publishes the event (when evt != 0 or is a real type), and
// persists the snapshot. A zero evt with a nil hook publishes no event.
func (o *Orchestrator) advance(ctx stdctx.Context, jobID string, to job.State, evt events.Type, reason string, mutate func(*job.Job), hook func()) error {
	from, err := o.reg.Transition(jobID, to, mutate)
	if err != nil {
		if errors.Is(err, job.ErrIllegalTransition) {
			o.metrics.IllegalTransition()
		}
		return err
	}
	o.metrics.StateTransition(from.String(), to.String())
	if hook != nil {
		hook()
	}
	snap, _ := o.reg.Get(jobID)
	// evt == ExecutionRequested (0) is never a lifecycle mark, so it doubles as
	// the "no event" sentinel for MarkStreaming.
	if evt != events.ExecutionRequested {
		o.publishJob(evt, snap, reason)
	}
	o.persist(ctx, snap)
	return nil
}

// --- Queries -----------------------------------------------------------------

// Status returns an immutable snapshot of a job.
func (o *Orchestrator) Status(jobID string) (job.Job, error) {
	j, ok := o.reg.Get(jobID)
	if !ok {
		return job.Job{}, job.ErrJobNotFound
	}
	return j, nil
}

// Statistics returns a job's derived timing statistics.
func (o *Orchestrator) Statistics(jobID string) (job.Statistics, error) {
	j, ok := o.reg.Get(jobID)
	if !ok {
		return job.Statistics{}, job.ErrJobNotFound
	}
	return j.Statistics(), nil
}

// ByUser, ByRoom, BySession, ByState, ByLanguage expose the registry indexes.
func (o *Orchestrator) ByUser(userID string) []job.Job   { return o.reg.ByUser(userID) }
func (o *Orchestrator) ByRoom(roomID string) []job.Job   { return o.reg.ByRoom(roomID) }
func (o *Orchestrator) BySession(sid string) []job.Job   { return o.reg.BySession(sid) }
func (o *Orchestrator) ByState(s job.State) []job.Job    { return o.reg.ByState(s) }
func (o *Orchestrator) ByLanguage(lang string) []job.Job { return o.reg.ByLanguage(lang) }

// Stats returns registry statistics.
func (o *Orchestrator) Stats() registry.Stats { return o.reg.Stats() }

// ExecutionContext exposes a job's live execution context to future modules.
func (o *Orchestrator) ExecutionContext(jobID string) (*execctx.ExecutionContext, bool) {
	return o.ctxMgr.Get(jobID)
}

// --- Archival ----------------------------------------------------------------

// Archive transitions a finished job to Archived, removes it from the live
// registry, and hands the snapshot to the archive sink. It returns the archived
// snapshot. Unknown or non-finished jobs yield an error.
func (o *Orchestrator) Archive(ctx stdctx.Context, jobID string, sink storage.Archive) (job.Job, error) {
	from, err := o.reg.Transition(jobID, job.StateArchived, nil)
	if err != nil {
		if errors.Is(err, job.ErrIllegalTransition) {
			o.metrics.IllegalTransition()
		}
		return job.Job{}, err
	}
	o.metrics.StateTransition(from.String(), job.StateArchived.String())
	snap, _ := o.reg.Remove(jobID)
	o.ctxMgr.Release(jobID)
	if sink != nil {
		if err := sink.Archive(ctx, snap, o.now()); err != nil {
			o.log.Error("archive sink failed", "job_id", jobID, "err", err)
		}
	}
	_ = o.store.Delete(ctx, jobID)
	o.metrics.JobArchived()
	o.publishJob(events.ExecutionArchived, snap, "")
	o.updateGauges()
	return snap, nil
}

// ArchiveFinished sweeps finished jobs older than the retention window into the
// archive sink and returns the number archived.
func (o *Orchestrator) ArchiveFinished(ctx stdctx.Context, sink storage.Archive) int {
	cutoff := o.now().Add(-o.cfg.ArchiveRetention)
	n := 0
	for _, j := range o.reg.FinishedBefore(cutoff) {
		if _, err := o.Archive(ctx, j.ID, sink); err != nil {
			o.log.Warn("archive sweep skipped job", "job_id", j.ID, "err", err)
			continue
		}
		n++
	}
	return n
}

// --- helpers -----------------------------------------------------------------

func (o *Orchestrator) publishJob(t events.Type, j job.Job, reason string) {
	o.bus.Publish(events.Event{
		Type: t, JobID: j.ID, RequestID: j.RequestID, CorrelationID: j.CorrelationID,
		UserID: j.UserID, SessionID: j.SessionID, RoomID: j.RoomID, Language: j.Language,
		State: j.State, Reason: reason, Payload: j,
	})
}

func (o *Orchestrator) persist(ctx stdctx.Context, j job.Job) {
	if o.store == nil {
		return
	}
	if err := o.store.Save(ctx, j); err != nil {
		o.log.Warn("persist job failed", "job_id", j.ID, "err", err)
	}
}

func (o *Orchestrator) updateGauges() {
	s := o.reg.Stats()
	o.metrics.ActiveJobs(s.ActiveJobs)
}
