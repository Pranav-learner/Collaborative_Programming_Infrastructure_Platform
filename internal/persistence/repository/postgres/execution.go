package postgres

import (
	"context"
	"database/sql"
	"errors"

	"cpip/internal/persistence/query"
	"cpip/internal/persistence/repository"
)

type ExecutionRepository struct {
	exec repository.Executor
}

func NewExecutionRepository(exec repository.Executor) *ExecutionRepository {
	return &ExecutionRepository{exec: exec}
}

func (r *ExecutionRepository) Create(ctx context.Context, e *repository.ExecutionEntity) error {
	if e.Version == 0 {
		e.Version = 1
	}

	queryStr := `
		INSERT INTO executions (id, sandbox_id, language, status, exit_code, stdout, stderr, created_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := r.exec.ExecContext(ctx, queryStr,
		e.ID, e.SandboxID, e.Language, e.Status, e.ExitCode, e.Stdout, e.Stderr, e.CreatedAt, e.Version,
	)
	return err
}

func (r *ExecutionRepository) GetByID(ctx context.Context, id string) (*repository.ExecutionEntity, error) {
	queryStr := `
		SELECT id, sandbox_id, language, status, exit_code, stdout, stderr, created_at, version
		FROM executions
		WHERE id = $1
	`

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var e repository.ExecutionEntity
	err := row.Scan(&e.ID, &e.SandboxID, &e.Language, &e.Status, &e.ExitCode, &e.Stdout, &e.Stderr, &e.CreatedAt, &e.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (r *ExecutionRepository) List(ctx context.Context, params query.Params) ([]*repository.ExecutionEntity, error) {
	baseQuery := `
		SELECT id, sandbox_id, language, status, exit_code, stdout, stderr, created_at, version
		FROM executions
	`

	fullQuery, args := query.BuildQuery(baseQuery, params, 1)

	rows, err := r.exec.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*repository.ExecutionEntity
	for rows.Next() {
		var e repository.ExecutionEntity
		err := rows.Scan(&e.ID, &e.SandboxID, &e.Language, &e.Status, &e.ExitCode, &e.Stdout, &e.Stderr, &e.CreatedAt, &e.Version)
		if err != nil {
			return nil, err
		}
		list = append(list, &e)
	}

	return list, nil
}
