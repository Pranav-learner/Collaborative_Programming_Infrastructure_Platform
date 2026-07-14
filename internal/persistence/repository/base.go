package repository

import (
	"context"
	"database/sql"
	"errors"
)

var (
	// ErrNotFound is returned when an entity is not found in the persistence layer.
	ErrNotFound = errors.New("entity not found")
)

// Executor defines database operations that can be performed inside or outside a transaction.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
