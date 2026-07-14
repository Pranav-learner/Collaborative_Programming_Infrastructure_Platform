package repository

import (
	"context"
	"time"

	"cpip/internal/persistence/query"
)

// ExecutionEntity maps the database executions table.
type ExecutionEntity struct {
	ID        string
	SandboxID string
	Language  string
	Status    string
	ExitCode  int
	Stdout    string
	Stderr    string
	CreatedAt time.Time
	Version   int64
}

// ExecutionRepository outlines repository methods for Execution logs.
type ExecutionRepository interface {
	Create(ctx context.Context, exec *ExecutionEntity) error
	GetByID(ctx context.Context, id string) (*ExecutionEntity, error)
	List(ctx context.Context, params query.Params) ([]*ExecutionEntity, error)
}
