package postgres

import (
	"context"
	"database/sql"
	"errors"

	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/repository"
)

type ArtifactMetadataRepository struct {
	exec repository.Executor
}

func NewArtifactMetadataRepository(exec repository.Executor) *ArtifactMetadataRepository {
	return &ArtifactMetadataRepository{exec: exec}
}

func (r *ArtifactMetadataRepository) Create(ctx context.Context, art *repository.ArtifactMetadataEntity) error {
	if art.Version == 0 {
		art.Version = 1
	}

	queryStr := `
		INSERT INTO artifact_metadata (id, name, type, path, size, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := r.exec.ExecContext(ctx, queryStr,
		art.ID, art.Name, art.Type, art.Path, art.Size, art.Version, art.CreatedAt, art.UpdatedAt,
	)
	return err
}

func (r *ArtifactMetadataRepository) Update(ctx context.Context, art *repository.ArtifactMetadataEntity) error {
	oldVersion := art.Version
	art.Version++

	queryStr := `
		UPDATE artifact_metadata SET
			name = $1, type = $2, path = $3, size = $4, version = $5, updated_at = $6
		WHERE id = $7 AND version = $8
	`

	res, err := r.exec.ExecContext(ctx, queryStr,
		art.Name, art.Type, art.Path, art.Size, art.Version, art.UpdatedAt, art.ID, oldVersion,
	)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		art.Version = oldVersion
		return locking.ErrOptimisticLockConflict
	}

	return nil
}

func (r *ArtifactMetadataRepository) GetByID(ctx context.Context, id string) (*repository.ArtifactMetadataEntity, error) {
	queryStr := `
		SELECT id, name, type, path, size, version, created_at, updated_at
		FROM artifact_metadata
		WHERE id = $1
	`

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var art repository.ArtifactMetadataEntity
	err := row.Scan(&art.ID, &art.Name, &art.Type, &art.Path, &art.Size, &art.Version, &art.CreatedAt, &art.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &art, nil
}

func (r *ArtifactMetadataRepository) Delete(ctx context.Context, id string) error {
	queryStr := "DELETE FROM artifact_metadata WHERE id = $1"
	res, err := r.exec.ExecContext(ctx, queryStr, id)
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
