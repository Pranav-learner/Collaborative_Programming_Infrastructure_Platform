package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/repository"
)

type DocumentRepository struct {
	exec repository.Executor
}

func NewDocumentRepository(exec repository.Executor) *DocumentRepository {
	return &DocumentRepository{exec: exec}
}

func (r *DocumentRepository) Create(ctx context.Context, doc *repository.DocumentEntity) error {
	if doc.Version == 0 {
		doc.Version = 1
	}

	queryStr := `
		INSERT INTO documents (id, room_id, content, version, created_at, updated_at, deleted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := r.exec.ExecContext(ctx, queryStr,
		doc.ID, doc.RoomID, doc.Content, doc.Version, doc.CreatedAt, doc.UpdatedAt, doc.DeletedAt,
	)
	return err
}

func (r *DocumentRepository) Update(ctx context.Context, doc *repository.DocumentEntity) error {
	oldVersion := doc.Version
	doc.Version++

	queryStr := `
		UPDATE documents SET
			content = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5 AND deleted_at IS NULL
	`

	res, err := r.exec.ExecContext(ctx, queryStr,
		doc.Content, doc.Version, doc.UpdatedAt, doc.ID, oldVersion,
	)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		doc.Version = oldVersion
		return locking.ErrOptimisticLockConflict
	}

	return nil
}

func (r *DocumentRepository) GetByID(ctx context.Context, id string) (*repository.DocumentEntity, error) {
	queryStr := `
		SELECT id, room_id, content, version, created_at, updated_at, deleted_at
		FROM documents
		WHERE id = $1 AND deleted_at IS NULL
	`

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var doc repository.DocumentEntity
	err := row.Scan(&doc.ID, &doc.RoomID, &doc.Content, &doc.Version, &doc.CreatedAt, &doc.UpdatedAt, &doc.DeletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

func (r *DocumentRepository) GetByRoomID(ctx context.Context, roomID string) (*repository.DocumentEntity, error) {
	queryStr := `
		SELECT id, room_id, content, version, created_at, updated_at, deleted_at
		FROM documents
		WHERE room_id = $1 AND deleted_at IS NULL
	`

	row := r.exec.QueryRowContext(ctx, queryStr, roomID)

	var doc repository.DocumentEntity
	err := row.Scan(&doc.ID, &doc.RoomID, &doc.Content, &doc.Version, &doc.CreatedAt, &doc.UpdatedAt, &doc.DeletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

func (r *DocumentRepository) Delete(ctx context.Context, id string) error {
	queryStr := "UPDATE documents SET deleted_at = $1 WHERE id = $2 AND deleted_at IS NULL"
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
