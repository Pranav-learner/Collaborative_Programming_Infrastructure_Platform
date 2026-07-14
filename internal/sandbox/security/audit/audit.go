package audit

import (
	"log/slog"

	"cpip/internal/sandbox/audit"
	"cpip/internal/sandbox/events"
)

// AuditEntry aliases cpip/internal/sandbox/audit.AuditEntry for backward compatibility.
type AuditEntry = audit.AuditEntry

// AuditLogger aliases cpip/internal/sandbox/audit.AuditLogger for backward compatibility.
type AuditLogger = audit.AuditLogger

// NewAuditLogger aliases cpip/internal/sandbox/audit.NewAuditLogger for backward compatibility.
func NewAuditLogger(bus *events.Bus, logger *slog.Logger) *AuditLogger {
	return audit.NewAuditLogger(bus, logger)
}
