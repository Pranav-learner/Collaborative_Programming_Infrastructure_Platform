package transactions

import (
	"context"
	"database/sql"
	"fmt"

	"cpip/internal/persistence/events"
)

type contextKey struct{}

var txKey = contextKey{}

// InjectTx puts an active transaction into the context.
func InjectTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// ExtractTx returns the active transaction from the context, or nil if absent.
func ExtractTx(ctx context.Context) *sql.Tx {
	if tx, ok := ctx.Value(txKey).(*sql.Tx); ok {
		return tx
	}
	return nil
}

// TransactionManager orchestrates transaction boundaries.
type TransactionManager struct {
	db *sql.DB
}

// NewTransactionManager constructs a transaction manager.
func NewTransactionManager(db *sql.DB) *TransactionManager {
	return &TransactionManager{db: db}
}

// ExecuteInTx executes a function inside a transaction block, handling commit and automatic rollback.
func (tm *TransactionManager) ExecuteInTx(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context) error) (err error) {
	// If a transaction is already active in this context, reuse it (nested transaction propagation).
	if activeTx := ExtractTx(ctx); activeTx != nil {
		return fn(ctx)
	}

	tx, err := tm.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	events.Publish(events.NewPersistenceEvent(events.TransactionStarted, "", "", "", nil))

	// Ensure rollback on panic or error
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			events.Publish(events.NewPersistenceEvent(events.TransactionRolledBack, "", "", "", nil))
			panic(p) // re-throw panic after rollback
		} else if err != nil {
			_ = tx.Rollback()
			events.Publish(events.NewPersistenceEvent(events.TransactionRolledBack, "", "", "", nil))
		}
	}()

	ctxWithTx := InjectTx(ctx, tx)
	err = fn(ctxWithTx)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	events.Publish(events.NewPersistenceEvent(events.TransactionCommitted, "", "", "", nil))
	return nil
}
