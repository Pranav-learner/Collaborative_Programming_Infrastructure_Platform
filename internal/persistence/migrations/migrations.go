package migrations

import (
	"context"
	"database/sql"
	"fmt"
)

// Migration represents a single schema migration step.
type Migration struct {
	Version int64
	Name    string
	Up      string
	Down    string
}

// Registry stores all registered migrations in order of version.
var Registry = []Migration{
	{
		Version: 202607140001,
		Name:    "create_audit_logs",
		Up: `
			CREATE TABLE IF NOT EXISTS audit_logs (
				id VARCHAR(64) PRIMARY KEY,
				entity_name VARCHAR(64) NOT NULL,
				entity_id VARCHAR(64) NOT NULL,
				action VARCHAR(32) NOT NULL,
				actor_id VARCHAR(64),
				payload JSONB,
				timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		`,
		Down: "DROP TABLE IF EXISTS audit_logs;",
	},
	{
		Version: 202607140002,
		Name:    "create_rooms_and_participants",
		Up: `
			CREATE TABLE IF NOT EXISTS rooms (
				id VARCHAR(64) PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				owner_id VARCHAR(64) NOT NULL,
				state VARCHAR(32) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				last_activity TIMESTAMP WITH TIME ZONE NOT NULL,
				max_participants INT NOT NULL,
				idle_timeout_ns BIGINT NOT NULL,
				expire_timeout_ns BIGINT NOT NULL,
				recovery_timeout_ns BIGINT NOT NULL,
				visibility VARCHAR(16) NOT NULL,
				metadata JSONB,
				version BIGINT NOT NULL DEFAULT 1,
				deleted_at TIMESTAMP WITH TIME ZONE
			);
			CREATE TABLE IF NOT EXISTS participants (
				room_id VARCHAR(64) REFERENCES rooms(id) ON DELETE CASCADE,
				user_id VARCHAR(64) NOT NULL,
				role VARCHAR(32) NOT NULL,
				session_id VARCHAR(64) NOT NULL,
				conn_id VARCHAR(64) NOT NULL,
				joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
				last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
				connected BOOLEAN NOT NULL,
				metadata JSONB,
				PRIMARY KEY (room_id, user_id)
			);
		`,
		Down: `
			DROP TABLE IF EXISTS participants;
			DROP TABLE IF EXISTS rooms;
		`,
	},
	{
		Version: 202607140003,
		Name:    "create_documents",
		Up: `
			CREATE TABLE IF NOT EXISTS documents (
				id VARCHAR(64) PRIMARY KEY,
				room_id VARCHAR(64) NOT NULL,
				content BYTEA,
				version BIGINT NOT NULL DEFAULT 1,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				deleted_at TIMESTAMP WITH TIME ZONE
			);
		`,
		Down: "DROP TABLE IF EXISTS documents;",
	},
	{
		Version: 202607140004,
		Name:    "create_executions",
		Up: `
			CREATE TABLE IF NOT EXISTS executions (
				id VARCHAR(64) PRIMARY KEY,
				sandbox_id VARCHAR(64) NOT NULL,
				language VARCHAR(32) NOT NULL,
				status VARCHAR(32) NOT NULL,
				exit_code INT,
				stdout TEXT,
				stderr TEXT,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				version BIGINT NOT NULL DEFAULT 1
			);
		`,
		Down: "DROP TABLE IF EXISTS executions;",
	},
	{
		Version: 202607140005,
		Name:    "create_sandboxes",
		Up: `
			CREATE TABLE IF NOT EXISTS sandboxes (
				id VARCHAR(64) PRIMARY KEY,
				runtime_id VARCHAR(64) NOT NULL,
				status VARCHAR(32) NOT NULL,
				ip VARCHAR(64) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				version BIGINT NOT NULL DEFAULT 1,
				deleted_at TIMESTAMP WITH TIME ZONE
			);
		`,
		Down: "DROP TABLE IF EXISTS sandboxes;",
	},
	{
		Version: 202607140006,
		Name:    "create_user_sessions",
		Up: `
			CREATE TABLE IF NOT EXISTS user_sessions (
				id VARCHAR(64) PRIMARY KEY,
				user_id VARCHAR(64) NOT NULL,
				token VARCHAR(255) NOT NULL,
				expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				version BIGINT NOT NULL DEFAULT 1
			);
		`,
		Down: "DROP TABLE IF EXISTS user_sessions;",
	},
	{
		Version: 202607140007,
		Name:    "create_artifact_metadata",
		Up: `
			CREATE TABLE IF NOT EXISTS artifact_metadata (
				id VARCHAR(64) PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				type VARCHAR(64) NOT NULL,
				path VARCHAR(512) NOT NULL,
				size BIGINT NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				version BIGINT NOT NULL DEFAULT 1
			);
		`,
		Down: "DROP TABLE IF EXISTS artifact_metadata;",
	},
}

// Manager orchestrates forward and backward schema migrations.
type Manager struct {
	db *sql.DB
}

// NewManager creates a migration manager instance.
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// MigrateUp executes all pending migrations.
func (m *Manager) MigrateUp(ctx context.Context) error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Advisory lock to serialize concurrent application migrations.
	// 7492104 is a CPIP-specific lock ID.
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(7492104);"); err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}

	// Create migrations table if missing.
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := tx.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to verify schema_migrations table: %w", err)
	}

	// Get applied migrations list.
	rows, err := tx.QueryContext(ctx, "SELECT version FROM schema_migrations ORDER BY version ASC;")
	if err != nil {
		return fmt.Errorf("failed to fetch schema_migrations details: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]bool)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}

	// Run pending.
	for _, migration := range Registry {
		if applied[migration.Version] {
			continue
		}

		// Run Up script.
		if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
			return fmt.Errorf("migration up failure on version %d (%s): %w", migration.Version, migration.Name, err)
		}

		// Log in table.
		recordQuery := "INSERT INTO schema_migrations (version, name) VALUES ($1, $2);"
		if _, err := tx.ExecContext(ctx, recordQuery, migration.Version, migration.Name); err != nil {
			return fmt.Errorf("failed to log migration version %d: %w", migration.Version, err)
		}
	}

	return tx.Commit()
}

// MigrateDown rolls back all applied migrations in reverse.
func (m *Manager) MigrateDown(ctx context.Context) error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin rollback transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(7492104);"); err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}

	// Read applied.
	rows, err := tx.QueryContext(ctx, "SELECT version FROM schema_migrations ORDER BY version DESC;")
	if err != nil {
		// Table doesn't exist, nothing to rollback.
		return nil
	}
	defer rows.Close()

	var appliedVersions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		appliedVersions = append(appliedVersions, v)
	}

	migrationsMap := make(map[int64]Migration)
	for _, migration := range Registry {
		migrationsMap[migration.Version] = migration
	}

	for _, v := range appliedVersions {
		migration, exists := migrationsMap[v]
		if !exists {
			continue
		}

		if _, err := tx.ExecContext(ctx, migration.Down); err != nil {
			return fmt.Errorf("migration rollback failure on version %d (%s): %w", migration.Version, migration.Name, err)
		}

		if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = $1;", v); err != nil {
			return fmt.Errorf("failed to delete migration entry %d: %w", v, err)
		}
	}

	return tx.Commit()
}
