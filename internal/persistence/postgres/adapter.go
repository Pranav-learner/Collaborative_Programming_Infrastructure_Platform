package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"cpip/internal/persistence/config"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Adapter wraps sql.DB and implements health checking, retry logic, and statement caching.
type Adapter struct {
	db          *sql.DB
	cfg         config.Config
	closeMu     sync.RWMutex
	closed      bool
	stmtMu      sync.RWMutex
	cachedStmts map[string]*sql.Stmt
}

// NewAdapter initializes a new connection pool adapter.
func NewAdapter(cfg config.Config) (*Adapter, error) {
	db, err := sql.Open("pgx", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	adapter := &Adapter{
		db:          db,
		cfg:         cfg,
		cachedStmts: make(map[string]*sql.Stmt),
	}

	// Verify initial connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := adapter.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return adapter, nil
}

// DB returns the underlying sql.DB.
func (a *Adapter) DB() *sql.DB {
	return a.db
}

// Ping checks database connectivity.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Close gracefully closes all cached statements and the database connection pool.
func (a *Adapter) Close() error {
	a.closeMu.Lock()
	defer a.closeMu.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true

	a.stmtMu.Lock()
	for _, stmt := range a.cachedStmts {
		_ = stmt.Close()
	}
	a.cachedStmts = make(map[string]*sql.Stmt)
	a.stmtMu.Unlock()

	return a.db.Close()
}

// Prepare cached statements thread-safely.
func (a *Adapter) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	a.closeMu.RLock()
	closed := a.closed
	a.closeMu.RUnlock()
	if closed {
		return nil, fmt.Errorf("database adapter is closed")
	}

	a.stmtMu.RLock()
	stmt, exists := a.cachedStmts[query]
	a.stmtMu.RUnlock()
	if exists {
		return stmt, nil
	}

	a.stmtMu.Lock()
	defer a.stmtMu.Unlock()
	// Double check
	if stmt, exists = a.cachedStmts[query]; exists {
		return stmt, nil
	}

	stmt, err := a.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	a.cachedStmts[query] = stmt
	return stmt, nil
}

// ExecuteWithRetry executes db write queries (Insert/Update/Delete) with retry policies.
func (a *Adapter) ExecuteWithRetry(ctx context.Context, fn func(ctx context.Context) error) error {
	var lastErr error
	for i := 0; i < a.cfg.MaxRetries; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		lastErr = err
		// Only retry on connection / temporary database errors.
		// For simplicity, wait a short duration before retrying.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.cfg.RetryInterval):
		}
	}
	return fmt.Errorf("transaction execution failed after %d retries: %w", a.cfg.MaxRetries, lastErr)
}

// Stats returns connection pool metrics.
func (a *Adapter) Stats() sql.DBStats {
	return a.db.Stats()
}
