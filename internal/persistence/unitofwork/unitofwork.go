package unitofwork

import (
	"context"
	"database/sql"

	"cpip/internal/persistence/repository"
	postgresRepos "cpip/internal/persistence/repository/postgres"
	"cpip/internal/persistence/transactions"
)

// RepositoryProvider gives access to all domain repositories resolved in the current
// transaction context. Each accessor returns a repository backed by the active sql.Tx
// (if inside a UoW transaction) or by the raw sql.DB connection pool (if not).
type RepositoryProvider interface {
	Rooms() repository.RoomRepository
	Documents() repository.DocumentRepository
	Executions() repository.ExecutionRepository
	Sandboxes() repository.SandboxRepository
	Sessions() repository.UserSessionRepository
	Artifacts() repository.ArtifactMetadataRepository
}

// UnitOfWork defines execution boundaries for transactional operations.
// Execute runs the provided function inside a database transaction that is
// automatically committed on success and rolled back on error or panic.
type UnitOfWork interface {
	// Execute runs fn inside a transaction with default options (ReadCommitted, read-write).
	Execute(ctx context.Context, fn func(ctx context.Context, provider RepositoryProvider) error) error

	// ExecuteWithOptions runs fn inside a transaction with custom isolation and read-only settings.
	ExecuteWithOptions(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, provider RepositoryProvider) error) error

	// ExecuteReadOnly is a convenience wrapper that runs fn in a read-only transaction.
	ExecuteReadOnly(ctx context.Context, fn func(ctx context.Context, provider RepositoryProvider) error) error
}

// ctxProvider implements RepositoryProvider by resolving the Executor from context.
// When the context carries a sql.Tx (injected by the TransactionManager), all
// repositories participate in the same transaction. Otherwise they fall back to
// the connection pool.
type ctxProvider struct {
	db  *sql.DB
	ctx context.Context
}

func (cp *ctxProvider) resolve() repository.Executor {
	if tx := transactions.ExtractTx(cp.ctx); tx != nil {
		return tx
	}
	return cp.db
}

func (cp *ctxProvider) Rooms() repository.RoomRepository {
	return postgresRepos.NewRoomRepository(cp.resolve())
}

func (cp *ctxProvider) Documents() repository.DocumentRepository {
	return postgresRepos.NewDocumentRepository(cp.resolve())
}

func (cp *ctxProvider) Executions() repository.ExecutionRepository {
	return postgresRepos.NewExecutionRepository(cp.resolve())
}

func (cp *ctxProvider) Sandboxes() repository.SandboxRepository {
	return postgresRepos.NewSandboxRepository(cp.resolve())
}

func (cp *ctxProvider) Sessions() repository.UserSessionRepository {
	return postgresRepos.NewUserSessionRepository(cp.resolve())
}

func (cp *ctxProvider) Artifacts() repository.ArtifactMetadataRepository {
	return postgresRepos.NewArtifactMetadataRepository(cp.resolve())
}

// uowImpl implements UnitOfWork.
type uowImpl struct {
	db        *sql.DB
	txManager *transactions.TransactionManager
}

// NewUnitOfWork creates a production Unit of Work tied to the given connection pool.
func NewUnitOfWork(db *sql.DB) UnitOfWork {
	return &uowImpl{
		db:        db,
		txManager: transactions.NewTransactionManager(db),
	}
}

func (u *uowImpl) Execute(ctx context.Context, fn func(ctx context.Context, provider RepositoryProvider) error) error {
	return u.ExecuteWithOptions(ctx, &sql.TxOptions{}, fn)
}

func (u *uowImpl) ExecuteWithOptions(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, provider RepositoryProvider) error) error {
	return u.txManager.ExecuteInTx(ctx, opts, func(txCtx context.Context) error {
		provider := &ctxProvider{
			db:  u.db,
			ctx: txCtx,
		}
		return fn(txCtx, provider)
	})
}

func (u *uowImpl) ExecuteReadOnly(ctx context.Context, fn func(ctx context.Context, provider RepositoryProvider) error) error {
	return u.ExecuteWithOptions(ctx, &sql.TxOptions{ReadOnly: true}, fn)
}
