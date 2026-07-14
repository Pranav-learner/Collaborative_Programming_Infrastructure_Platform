package audit

import (
	"log/slog"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
)

type AuditEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Action    string         `json:"action"` // e.g. "policy_applied", "violation_detected", "execution_denied"
	SandboxID string         `json:"sandbox_id"`
	JobID     string         `json:"job_id,omitempty"`
	Details   string         `json:"details"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type AuditLogger struct {
	mu      sync.Mutex
	entries []AuditEntry
	bus     *events.Bus
	logger  *slog.Logger
}

func NewAuditLogger(bus *events.Bus, logger *slog.Logger) *AuditLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditLogger{
		bus:    bus,
		logger: logger,
	}
}

func (al *AuditLogger) Record(action string, sandboxID string, jobID string, details string, metadata map[string]any) {
	al.mu.Lock()
	defer al.mu.Unlock()

	entry := AuditEntry{
		Timestamp: time.Now(),
		Action:    action,
		SandboxID: sandboxID,
		JobID:     jobID,
		Details:   details,
		Metadata:  metadata,
	}

	al.entries = append(al.entries, entry)

	al.logger.Info("Security Audit Record",
		"action", action,
		"sandbox_id", sandboxID,
		"job_id", jobID,
		"details", details,
		"metadata", metadata,
	)

	if al.bus != nil {
		al.bus.Publish(events.Event{
			Type:      events.AuditRecorded,
			SandboxID: sandboxID,
			JobID:     jobID,
			Timestamp: entry.Timestamp,
			Payload:   entry,
		})
	}
}

func (al *AuditLogger) ListEntries() []AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	copied := make([]AuditEntry, len(al.entries))
	copy(copied, al.entries)
	return copied
}
