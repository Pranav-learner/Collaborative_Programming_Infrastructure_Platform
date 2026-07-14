package audit

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Executor represents the subset of database operations needed by audit logging.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// LogEntry details one mutation audit event.
type LogEntry struct {
	ID         string
	EntityName string
	EntityID   string
	Action     string // 'CREATE', 'UPDATE', 'DELETE', 'RESTORE'
	ActorID    string
	Payload    any
	Timestamp  time.Time
}

// Record persists a log entry using the provided executor.
func Record(ctx context.Context, exec Executor, entry LogEntry) error {
	if entry.ID == "" {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		entry.ID = hex.EncodeToString(b)
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	payloadJSON, err := json.Marshal(entry.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal audit payload: %w", err)
	}

	query := `
		INSERT INTO audit_logs (id, entity_name, entity_id, action, actor_id, payload, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err = exec.ExecContext(ctx, query,
		entry.ID,
		entry.EntityName,
		entry.EntityID,
		entry.Action,
		entry.ActorID,
		payloadJSON,
		entry.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to write audit log: %w", err)
	}

	return nil
}
