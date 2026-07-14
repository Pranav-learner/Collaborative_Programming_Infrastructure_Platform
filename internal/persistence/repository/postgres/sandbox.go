package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/repository"
)

type SandboxRepository struct {
	exec repository.Executor
}

func NewSandboxRepository(exec repository.Executor) *SandboxRepository {
	return &SandboxRepository{exec: exec}
}

func (r *SandboxRepository) Create(ctx context.Context, sb *repository.SandboxEntity) error {
	if sb.Version == 0 {
		sb.Version = 1
	}

	queryStr := `
		INSERT INTO sandboxes (id, runtime_id, status, ip, created_at, updated_at, version, deleted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := r.exec.ExecContext(ctx, queryStr,
		sb.ID, sb.RuntimeID, sb.Status, sb.IP, sb.CreatedAt, sb.UpdatedAt, sb.Version, sb.DeletedAt,
	)
	return err
}

func (r *SandboxRepository) Update(ctx context.Context, sb *repository.SandboxEntity) error {
	oldVersion := sb.Version
	sb.Version++

	queryStr := `
		UPDATE sandboxes SET
			runtime_id = $1, status = $2, ip = $3, updated_at = $4, version = $5
		WHERE id = $6 AND version = $7 AND deleted_at IS NULL
	`

	res, err := r.exec.ExecContext(ctx, queryStr,
		sb.RuntimeID, sb.Status, sb.IP, sb.UpdatedAt, sb.Version, sb.ID, oldVersion,
	)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		sb.Version = oldVersion
		return locking.ErrOptimisticLockConflict
	}

	return nil
}

func (r *SandboxRepository) GetByID(ctx context.Context, id string) (*repository.SandboxEntity, error) {
	queryStr := `
		SELECT id, runtime_id, status, ip, created_at, updated_at, version, deleted_at
		FROM sandboxes
		WHERE id = $1 AND deleted_at IS NULL
	`

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var sb repository.SandboxEntity
	err := row.Scan(&sb.ID, &sb.RuntimeID, &sb.Status, &sb.IP, &sb.CreatedAt, &sb.UpdatedAt, &sb.Version, &sb.DeletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &sb, nil
}

func (r *SandboxRepository) Delete(ctx context.Context, id string) error {
	queryStr := "UPDATE sandboxes SET deleted_at = $1 WHERE id = $2 AND deleted_at IS NULL"
	res, err := r.exec.ExecContext(ctx, queryStr, time.Now(), id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return repository.ErrNotFound
	}
	return nil
}
