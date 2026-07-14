// Package middleware provides decorators that wrap repository.Executor to add
// cross-cutting concerns (logging, metrics, audit) without modifying the
// repository implementations.
package middleware

import (
	"context"
	"database/sql"
	"time"

	"cpip/internal/persistence/logger"
	"cpip/internal/persistence/metrics"
)

// LoggingExecutor wraps an Executor and logs every operation.
type LoggingExecutor struct {
	inner  Executor
	log    *logger.Logger
	entity string
}

// Executor is the subset of database operations that middleware wraps.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// NewLoggingExecutor creates a logging decorator around an Executor.
func NewLoggingExecutor(inner Executor, log *logger.Logger, entity string) *LoggingExecutor {
	return &LoggingExecutor{inner: inner, log: log, entity: entity}
}

func (l *LoggingExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	l.log.QueryStart(ctx, "exec", l.entity)
	start := time.Now()
	res, err := l.inner.ExecContext(ctx, query, args...)
	l.log.QueryEnd(ctx, "exec", l.entity, time.Since(start), err)
	return res, err
}

func (l *LoggingExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	l.log.QueryStart(ctx, "query", l.entity)
	start := time.Now()
	rows, err := l.inner.QueryContext(ctx, query, args...)
	l.log.QueryEnd(ctx, "query", l.entity, time.Since(start), err)
	return rows, err
}

func (l *LoggingExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	l.log.QueryStart(ctx, "query_row", l.entity)
	return l.inner.QueryRowContext(ctx, query, args...)
}

func (l *LoggingExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return l.inner.PrepareContext(ctx, query)
}

// MetricsExecutor wraps an Executor and records query metrics.
type MetricsExecutor struct {
	inner    Executor
	recorder metrics.Recorder
	entity   string
}

// NewMetricsExecutor creates a metrics decorator around an Executor.
func NewMetricsExecutor(inner Executor, recorder metrics.Recorder, entity string) *MetricsExecutor {
	return &MetricsExecutor{inner: inner, recorder: recorder, entity: entity}
}

func (m *MetricsExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := m.inner.ExecContext(ctx, query, args...)
	metrics.ObserveQuery(m.recorder, m.entity, "exec", time.Since(start))
	return res, err
}

func (m *MetricsExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := m.inner.QueryContext(ctx, query, args...)
	metrics.ObserveQuery(m.recorder, m.entity, "query", time.Since(start))
	return rows, err
}

func (m *MetricsExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	start := time.Now()
	row := m.inner.QueryRowContext(ctx, query, args...)
	metrics.ObserveQuery(m.recorder, m.entity, "query_row", time.Since(start))
	return row
}

func (m *MetricsExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return m.inner.PrepareContext(ctx, query)
}
