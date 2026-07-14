package audit

import (
	"log/slog"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
)

// Category defines the operational area of the audit entry.
type Category string

const (
	CategoryOperational Category = "Operational"
	CategorySecurity    Category = "Security"
	CategoryRecovery    Category = "Recovery"
	CategoryLifecycle   Category = "Lifecycle"
	CategoryCleanup     Category = "Cleanup"
	CategoryPolicy      Category = "Policy"
	CategoryStatistics  Category = "Statistics"
)

// AuditEntry holds categorized details about a system execution or state change.
type AuditEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Category  Category       `json:"category"`
	Action    string         `json:"action"` // e.g. "policy_applied", "violation_detected", "execution_denied"
	SandboxID string         `json:"sandbox_id"`
	JobID     string         `json:"job_id,omitempty"`
	Details   string         `json:"details"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// AuditLogger thread-safely logs events and records entries for querying/compliance.
type AuditLogger struct {
	mu      sync.Mutex
	entries []AuditEntry
	bus     *events.Bus
	logger  *slog.Logger
}

// NewAuditLogger initializes a new AuditLogger.
func NewAuditLogger(bus *events.Bus, logger *slog.Logger) *AuditLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditLogger{
		bus:    bus,
		logger: logger,
	}
}

// RecordCategorized logs and stores an entry with an explicit category.
func (al *AuditLogger) RecordCategorized(category Category, action string, sandboxID string, jobID string, details string, metadata map[string]any) {
	al.mu.Lock()
	defer al.mu.Unlock()

	entry := AuditEntry{
		Timestamp: time.Now(),
		Category:  category,
		Action:    action,
		SandboxID: sandboxID,
		JobID:     jobID,
		Details:   details,
		Metadata:  metadata,
	}

	al.entries = append(al.entries, entry)

	al.logger.Info("Categorized Audit Record",
		"category", category,
		"action", action,
		"sandbox_id", sandboxID,
		"job_id", jobID,
		"details", details,
		"metadata", metadata,
	)

	if al.bus != nil {
		al.bus.Publish(events.Event{
			Type:           events.AuditRecorded,
			SandboxID:      sandboxID,
			JobID:          jobID,
			Timestamp:      entry.Timestamp,
			LifecycleState: string(category),
			Severity:       "Info",
			Origin:         "audit",
			Payload:        entry,
		})
	}
}

// Record acts as a backward compatible method mapping to the Security category.
func (al *AuditLogger) Record(action string, sandboxID string, jobID string, details string, metadata map[string]any) {
	al.RecordCategorized(CategorySecurity, action, sandboxID, jobID, details, metadata)
}

// ListEntries returns a copy of all audit logs.
func (al *AuditLogger) ListEntries() []AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	copied := make([]AuditEntry, len(al.entries))
	copy(copied, al.entries)
	return copied
}

// Filter filters audit logs by category.
func (al *AuditLogger) Filter(category Category) []AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	var filtered []AuditEntry
	for _, entry := range al.entries {
		if entry.Category == category {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
