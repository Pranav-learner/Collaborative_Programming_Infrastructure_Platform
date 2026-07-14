package postgres

import (
	"context"
	"database/sql"
	"errors"

	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/repository"
)

type UserSessionRepository struct {
	exec repository.Executor
}

func NewUserSessionRepository(exec repository.Executor) *UserSessionRepository {
	return &UserSessionRepository{exec: exec}
}

func (r *UserSessionRepository) Create(ctx context.Context, sess *repository.UserSessionEntity) error {
	if sess.Version == 0 {
		sess.Version = 1
	}

	queryStr := `
		INSERT INTO user_sessions (id, user_id, token, expires_at, created_at, version)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := r.exec.ExecContext(ctx, queryStr,
		sess.ID, sess.UserID, sess.Token, sess.ExpiresAt, sess.CreatedAt, sess.Version,
	)
	return err
}

func (r *UserSessionRepository) Update(ctx context.Context, sess *repository.UserSessionEntity) error {
	oldVersion := sess.Version
	sess.Version++

	queryStr := `
		UPDATE user_sessions SET
			user_id = $1, token = $2, expires_at = $3, version = $4
		WHERE id = $5 AND version = $6
	`

	res, err := r.exec.ExecContext(ctx, queryStr,
		sess.UserID, sess.Token, sess.ExpiresAt, sess.Version, sess.ID, oldVersion,
	)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		sess.Version = oldVersion
		return locking.ErrOptimisticLockConflict
	}

	return nil
}

func (r *UserSessionRepository) GetByID(ctx context.Context, id string) (*repository.UserSessionEntity, error) {
	queryStr := `
		SELECT id, user_id, token, expires_at, created_at, version
		FROM user_sessions
		WHERE id = $1
	`

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var sess repository.UserSessionEntity
	err := row.Scan(&sess.ID, &sess.UserID, &sess.Token, &sess.ExpiresAt, &sess.CreatedAt, &sess.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &sess, nil
}

func (r *UserSessionRepository) GetByToken(ctx context.Context, token string) (*repository.UserSessionEntity, error) {
	queryStr := `
		SELECT id, user_id, token, expires_at, created_at, version
		FROM user_sessions
		WHERE token = $1
	`

	row := r.exec.QueryRowContext(ctx, queryStr, token)

	var sess repository.UserSessionEntity
	err := row.Scan(&sess.ID, &sess.UserID, &sess.Token, &sess.ExpiresAt, &sess.CreatedAt, &sess.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &sess, nil
}

func (r *UserSessionRepository) Delete(ctx context.Context, id string) error {
	queryStr := "DELETE FROM user_sessions WHERE id = $1"
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
