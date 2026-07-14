// Package schema provides utilities for inspecting and validating the current
// database schema state, independent of the migration runner. This is useful
// for health checks and application boot validation.
package schema

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// AppliedMigration represents a single migration that has been applied.
type AppliedMigration struct {
	Version   int64
	Name      string
	AppliedAt time.Time
}

// Inspector reads schema metadata from the database.
type Inspector struct {
	db *sql.DB
}

// NewInspector creates a schema Inspector.
func NewInspector(db *sql.DB) *Inspector {
	return &Inspector{db: db}
}

// CurrentVersion returns the latest applied migration version, or 0 if none.
func (i *Inspector) CurrentVersion(ctx context.Context) (int64, error) {
	var v sql.NullInt64
	err := i.db.QueryRowContext(ctx,
		"SELECT MAX(version) FROM schema_migrations;",
	).Scan(&v)
	if err != nil {
		// Table may not exist yet.
		return 0, nil
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

// ListApplied returns all applied migrations in order.
func (i *Inspector) ListApplied(ctx context.Context) ([]AppliedMigration, error) {
	rows, err := i.db.QueryContext(ctx,
		"SELECT version, name, applied_at FROM schema_migrations ORDER BY version ASC;",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list applied migrations: %w", err)
	}
	defer rows.Close()

	var out []AppliedMigration
	for rows.Next() {
		var m AppliedMigration
		if err := rows.Scan(&m.Version, &m.Name, &m.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// TableExists checks whether a given table exists in the public schema.
func (i *Inspector) TableExists(ctx context.Context, tableName string) (bool, error) {
	var exists bool
	err := i.db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1);",
		tableName,
	).Scan(&exists)
	return exists, err
}
