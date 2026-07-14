package watcher

import (
	"context"
	"os"
	"sync"
	"time"

	"cpip/internal/configuration/logger"
)

// ReloadCallback is triggered when a change is detected in a watched file.
type ReloadCallback func(path string)

// Watcher monitors file modification times and invokes callbacks.
type Watcher struct {
	mu        sync.RWMutex
	interval  time.Duration
	files     map[string]time.Time
	callbacks map[string][]ReloadCallback
	logger    *logger.Logger
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewWatcher creates a file modification Watcher.
func NewWatcher(interval time.Duration, log *logger.Logger) *Watcher {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Watcher{
		interval:  interval,
		files:     make(map[string]time.Time),
		callbacks: make(map[string][]ReloadCallback),
		logger:    log,
	}
}

// Watch registers a file to be monitored.
func (w *Watcher) Watch(path string, cb ReloadCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.callbacks[path] = append(w.callbacks[path], cb)

	// Record current modification time
	info, err := os.Stat(path)
	if err == nil {
		w.files[path] = info.ModTime()
	} else {
		// File might not exist yet, record zero time
		w.files[path] = time.Time{}
	}
}

// Start spawns the background monitoring loop.
func (w *Watcher) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancel != nil {
		return // already running
	}

	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.wg.Add(1)
	go w.pollLoop(watchCtx)
}

// Stop stops the watcher.
func (w *Watcher) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()

	if cancel != nil {
		cancel()
		w.wg.Wait()
		w.mu.Lock()
		w.cancel = nil
		w.mu.Unlock()
	}
}

func (w *Watcher) pollLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll()
		}
	}
}

func (w *Watcher) poll() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for path, lastMod := range w.files {
		info, err := os.Stat(path)
		if err != nil {
			continue // skip temporary failures (e.g. file swap during writes)
		}

		currentMod := info.ModTime()
		if !lastMod.IsZero() && currentMod.After(lastMod) {
			if w.logger != nil {
				w.logger.Info("File change detected", "path", path, "last_mod", lastMod, "current_mod", currentMod)
			}
			w.files[path] = currentMod

			// Trigger all callbacks for this file
			for _, cb := range w.callbacks[path] {
				go cb(path)
			}
		} else if lastMod.IsZero() {
			// First time seeing the file exist
			w.files[path] = currentMod
		}
	}
}
