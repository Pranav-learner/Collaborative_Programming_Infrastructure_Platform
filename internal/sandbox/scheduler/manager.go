package scheduler

import (
	"context"
	"sync"
	"time"

	"cpip/internal/sandbox/registry"
)

// IntervalConfig holds duration intervals for driving timing loops.
type IntervalConfig struct {
	WatchInterval   time.Duration `json:"watch_interval"`
	HealthInterval  time.Duration `json:"health_interval"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	TimeoutInterval time.Duration `json:"timeout_interval"`
}

// SandboxScheduler coordinates timing and execution of active background checks.
type SandboxScheduler struct {
	mu        sync.RWMutex
	reg       *registry.SandboxRegistry
	intervals IntervalConfig
	stopChan  chan struct{}
	wg        sync.WaitGroup
	watchFn   func(ctx context.Context)
	healthFn  func(ctx context.Context)
	cleanupFn func(ctx context.Context)
	timeoutFn func(ctx context.Context)
}

// NewSandboxScheduler creates a new SandboxScheduler.
func NewSandboxScheduler(reg *registry.SandboxRegistry, cfg IntervalConfig) *SandboxScheduler {
	// Set default intervals if not provided
	if cfg.WatchInterval == 0 {
		cfg.WatchInterval = 500 * time.Millisecond
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 1 * time.Second
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = 5 * time.Second
	}
	if cfg.TimeoutInterval == 0 {
		cfg.TimeoutInterval = 1 * time.Second
	}

	return &SandboxScheduler{
		reg:       reg,
		intervals: cfg,
		stopChan:  make(chan struct{}),
	}
}

// RegisterWatchTask registers passive resource monitoring checks.
func (s *SandboxScheduler) RegisterWatchTask(fn func(ctx context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchFn = fn
}

// RegisterHealthTask registers passive health probing checks.
func (s *SandboxScheduler) RegisterHealthTask(fn func(ctx context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthFn = fn
}

// RegisterCleanupTask registers passive garbage collection checks.
func (s *SandboxScheduler) RegisterCleanupTask(fn func(ctx context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupFn = fn
}

// RegisterTimeoutTask registers passive deadline timeout checks.
func (s *SandboxScheduler) RegisterTimeoutTask(fn func(ctx context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timeoutFn = fn
}

// Start spawns the tickers for registered loops.
func (s *SandboxScheduler) Start(ctx context.Context) {
	s.mu.RLock()
	intervals := s.intervals
	watch := s.watchFn
	health := s.healthFn
	cleanup := s.cleanupFn
	timeout := s.timeoutFn
	s.mu.RUnlock()

	runLoop := func(interval time.Duration, task func(ctx context.Context)) {
		if task == nil || interval <= 0 {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					task(ctx)
				case <-s.stopChan:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	runLoop(intervals.WatchInterval, watch)
	runLoop(intervals.HealthInterval, health)
	runLoop(intervals.CleanupInterval, cleanup)
	runLoop(intervals.TimeoutInterval, timeout)
}

// Stop halts all active timing goroutines.
func (s *SandboxScheduler) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}
