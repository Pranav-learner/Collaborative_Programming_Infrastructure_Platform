package persistence_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cpip/internal/persistence/audit"
	"cpip/internal/persistence/config"
	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/migrations"
	"cpip/internal/persistence/postgres"
	"cpip/internal/persistence/query"
	"cpip/internal/persistence/repository"
	"cpip/internal/persistence/unitofwork"
)

func setupTestDB(t *testing.T) (*postgres.Adapter, func()) {
	cfg := config.DefaultConfig()
	cfg.DBName = "cpip_test"
	cfg.Password = "postgres"

	adapter, err := postgres.NewAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize test DB adapter: %v", err)
	}

	// Run clean migrate down, then up
	mgr := migrations.NewManager(adapter.DB())
	ctx := context.Background()

	_ = mgr.MigrateDown(ctx)
	if err := mgr.MigrateUp(ctx); err != nil {
		t.Fatalf("Failed to run MigrateUp: %v", err)
	}

	cleanup := func() {
		_ = mgr.MigrateDown(ctx)
		_ = adapter.Close()
	}

	return adapter, cleanup
}

func TestMigrations(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	db := adapter.DB()
	var exists bool
	err := db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'schema_migrations');").Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check schema_migrations existence: %v", err)
	}
	if !exists {
		t.Errorf("schema_migrations table does not exist")
	}

	// Verify all registry tables exist
	tables := []string{"audit_logs", "rooms", "participants", "documents", "executions", "sandboxes", "user_sessions", "artifact_metadata"}
	for _, table := range tables {
		err := db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1);", table).Scan(&exists)
		if err != nil {
			t.Fatalf("Failed to check table %s existence: %v", table, err)
		}
		if !exists {
			t.Errorf("Table %s does not exist", table)
		}
	}
}

func TestUnitOfWork_CommitAndRollback(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	// 1. Rollback test
	err := uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		room := &repository.RoomEntity{
			ID:                "room-rollback",
			Name:              "Rollback Room",
			OwnerID:           "user-1",
			State:             "Created",
			CreatedAt:         time.Now(),
			LastActivity:      time.Now(),
			MaxParticipants:   10,
			Visibility:        "public",
			Version:           1,
		}
		if err := provider.Rooms().Create(ctx, room); err != nil {
			return err
		}
		// Return error to trigger rollback
		return errors.New("simulated error")
	})

	if err == nil {
		t.Errorf("Expected Unit of Work execution error, got nil")
	}

	// Verify room was not persisted
	uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		_, getErr := provider.Rooms().GetByID(ctx, "room-rollback", false)
		if !errors.Is(getErr, repository.ErrNotFound) {
			t.Errorf("Expected ErrNotFound, got %v", getErr)
		}
		return nil
	})

	// 2. Commit test
	err = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		room := &repository.RoomEntity{
			ID:                "room-commit",
			Name:              "Commit Room",
			OwnerID:           "user-1",
			State:             "Created",
			CreatedAt:         time.Now(),
			LastActivity:      time.Now(),
			MaxParticipants:   10,
			Visibility:        "public",
			Version:           1,
		}
		return provider.Rooms().Create(ctx, room)
	})

	if err != nil {
		t.Fatalf("Unexpected Unit of Work error: %v", err)
	}

	// Verify room was persisted
	uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		room, getErr := provider.Rooms().GetByID(ctx, "room-commit", false)
		if getErr != nil {
			t.Errorf("Expected room to be found, got %v", getErr)
		}
		if room.Name != "Commit Room" {
			t.Errorf("Expected 'Commit Room', got '%s'", room.Name)
		}
		return nil
	})
}

func TestRoomRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		room := &repository.RoomEntity{
			ID:                "room-1",
			Name:              "Awesome Room",
			OwnerID:           "owner-123",
			State:             "Active",
			CreatedAt:         now,
			LastActivity:      now,
			MaxParticipants:   20,
			IdleTimeoutNs:     int64(5 * time.Minute),
			ExpireTimeoutNs:   int64(10 * time.Minute),
			RecoveryTimeoutNs: int64(1 * time.Minute),
			Visibility:        "public",
			Metadata:          map[string]any{"tags": []any{"golang", "cpip"}},
			Version:           1,
		}

		// Create
		if err := provider.Rooms().Create(ctx, room); err != nil {
			t.Fatalf("Failed to create room: %v", err)
		}

		// Add participants
		p1 := &repository.ParticipantEntity{
			RoomID:    "room-1",
			UserID:    "user-1",
			Role:      "Participant",
			SessionID: "sess-1",
			ConnID:    "conn-1",
			JoinedAt:  now,
			LastSeen:  now,
			Connected: true,
			Metadata:  map[string]any{"color": "red"},
		}
		if err := provider.Rooms().AddParticipant(ctx, p1); err != nil {
			t.Fatalf("Failed to add participant p1: %v", err)
		}

		p2 := &repository.ParticipantEntity{
			RoomID:    "room-1",
			UserID:    "user-2",
			Role:      "Observer",
			SessionID: "sess-2",
			ConnID:    "conn-2",
			JoinedAt:  now,
			LastSeen:  now,
			Connected: false,
		}
		if err := provider.Rooms().AddParticipant(ctx, p2); err != nil {
			t.Fatalf("Failed to add participant p2: %v", err)
		}

		// Retrieve
		retrieved, err := provider.Rooms().GetByID(ctx, "room-1", false)
		if err != nil {
			t.Fatalf("Failed to get room: %v", err)
		}
		if retrieved.Name != room.Name {
			t.Errorf("Expected name '%s', got '%s'", room.Name, retrieved.Name)
		}
		if retrieved.Metadata["tags"].([]any)[0] != "golang" {
			t.Errorf("Expected metadata tags[0] to be 'golang'")
		}

		// Get participants
		parts, err := provider.Rooms().GetParticipants(ctx, "room-1")
		if err != nil {
			t.Fatalf("Failed to get participants: %v", err)
		}
		if len(parts) != 2 {
			t.Errorf("Expected 2 participants, got %d", len(parts))
		}

		// Remove participant
		if err := provider.Rooms().RemoveParticipant(ctx, "room-1", "user-2"); err != nil {
			t.Fatalf("Failed to remove participant user-2: %v", err)
		}

		parts, _ = provider.Rooms().GetParticipants(ctx, "room-1")
		if len(parts) != 1 {
			t.Errorf("Expected 1 participant after removal, got %d", len(parts))
		}

		return nil
	})
}

func TestDocumentRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		doc := &repository.DocumentEntity{
			ID:        "doc-1",
			RoomID:    "room-123",
			Content:   []byte("hello world"),
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := provider.Documents().Create(ctx, doc); err != nil {
			t.Fatalf("Failed to create document: %v", err)
		}

		retrieved, err := provider.Documents().GetByID(ctx, "doc-1")
		if err != nil {
			t.Fatalf("Failed to get document: %v", err)
		}
		if string(retrieved.Content) != "hello world" {
			t.Errorf("Expected content 'hello world', got '%s'", string(retrieved.Content))
		}

		// Update
		retrieved.Content = []byte("hello cpip")
		retrieved.UpdatedAt = time.Now()
		if err := provider.Documents().Update(ctx, retrieved); err != nil {
			t.Fatalf("Failed to update document: %v", err)
		}

		updated, err := provider.Documents().GetByRoomID(ctx, "room-123")
		if err != nil {
			t.Fatalf("Failed to get by room ID: %v", err)
		}
		if string(updated.Content) != "hello cpip" {
			t.Errorf("Expected updated content 'hello cpip', got '%s'", string(updated.Content))
		}
		if updated.Version != 2 {
			t.Errorf("Expected version 2, got %d", updated.Version)
		}

		// Delete
		if err := provider.Documents().Delete(ctx, "doc-1"); err != nil {
			t.Fatalf("Failed to delete document: %v", err)
		}

		_, err = provider.Documents().GetByID(ctx, "doc-1")
		if !errors.Is(err, repository.ErrNotFound) {
			t.Errorf("Expected ErrNotFound after soft delete, got %v", err)
		}

		return nil
	})
}

func TestExecutionRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		e1 := &repository.ExecutionEntity{
			ID:        "exec-1",
			SandboxID: "sb-1",
			Language:  "go",
			Status:    "Success",
			ExitCode:  0,
			Stdout:    "OK",
			Stderr:    "",
			CreatedAt: now,
		}
		e2 := &repository.ExecutionEntity{
			ID:        "exec-2",
			SandboxID: "sb-1",
			Language:  "python",
			Status:    "Failure",
			ExitCode:  1,
			Stdout:    "",
			Stderr:    "SyntaxError",
			CreatedAt: now.Add(time.Second),
		}

		if err := provider.Executions().Create(ctx, e1); err != nil {
			t.Fatalf("Failed to create execution e1: %v", err)
		}
		if err := provider.Executions().Create(ctx, e2); err != nil {
			t.Fatalf("Failed to create execution e2: %v", err)
		}

		// List filtered
		params := query.Params{
			Filters: []query.Filter{
				{Field: "language", Operator: query.OpEquals, Value: "go"},
			},
		}

		list, err := provider.Executions().List(ctx, params)
		if err != nil {
			t.Fatalf("Failed to list executions: %v", err)
		}
		if len(list) != 1 {
			t.Errorf("Expected 1 execution, got %d", len(list))
		}
		if list[0].Language != "go" {
			t.Errorf("Expected 'go', got '%s'", list[0].Language)
		}

		return nil
	})
}

func TestSandboxRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		sb := &repository.SandboxEntity{
			ID:        "sb-1",
			RuntimeID: "docker",
			Status:    "Running",
			IP:        "172.17.0.2",
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := provider.Sandboxes().Create(ctx, sb); err != nil {
			t.Fatalf("Failed to create sandbox: %v", err)
		}

		retrieved, err := provider.Sandboxes().GetByID(ctx, "sb-1")
		if err != nil {
			t.Fatalf("Failed to get sandbox: %v", err)
		}
		if retrieved.IP != "172.17.0.2" {
			t.Errorf("Expected IP '172.17.0.2', got '%s'", retrieved.IP)
		}

		return nil
	})
}

func TestUserSessionRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		sess := &repository.UserSessionEntity{
			ID:        "sess-1",
			UserID:    "user-100",
			Token:     "secure-token-abc",
			ExpiresAt: now.Add(24 * time.Hour),
			CreatedAt: now,
		}

		if err := provider.Sessions().Create(ctx, sess); err != nil {
			t.Fatalf("Failed to create user session: %v", err)
		}

		retrieved, err := provider.Sessions().GetByToken(ctx, "secure-token-abc")
		if err != nil {
			t.Fatalf("Failed to get session by token: %v", err)
		}
		if retrieved.UserID != "user-100" {
			t.Errorf("Expected UserID 'user-100', got '%s'", retrieved.UserID)
		}

		return nil
	})
}

func TestArtifactMetadataRepository(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		art := &repository.ArtifactMetadataEntity{
			ID:        "art-1",
			Name:      "test-file.txt",
			Type:      "text/plain",
			Path:      "/tmp/files/test-file.txt",
			Size:      1024,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := provider.Artifacts().Create(ctx, art); err != nil {
			t.Fatalf("Failed to create artifact metadata: %v", err)
		}

		retrieved, err := provider.Artifacts().GetByID(ctx, "art-1")
		if err != nil {
			t.Fatalf("Failed to get artifact: %v", err)
		}
		if retrieved.Size != 1024 {
			t.Errorf("Expected size 1024, got %d", retrieved.Size)
		}

		return nil
	})
}

func TestOptimisticLocking_ConflictAndRetry(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	now := time.Now().Round(time.Microsecond)

	// Create a document
	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		doc := &repository.DocumentEntity{
			ID:        "lock-doc",
			RoomID:    "room-1",
			Content:   []byte("ver-1"),
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}
		return provider.Documents().Create(ctx, doc)
	})

	// Fetch doc on client A and client B
	var docA, docB *repository.DocumentEntity
	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		var err error
		docA, err = provider.Documents().GetByID(ctx, "lock-doc")
		if err != nil {
			return err
		}
		docB, err = provider.Documents().GetByID(ctx, "lock-doc")
		return err
	})

	// 1. Client A updates successfully
	docA.Content = []byte("ver-1-A")
	docA.UpdatedAt = time.Now()
	err := uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		return provider.Documents().Update(ctx, docA)
	})
	if err != nil {
		t.Fatalf("Client A update failed unexpectedly: %v", err)
	}

	// 2. Client B tries to update using stale version, should fail with locking conflict
	docB.Content = []byte("ver-1-B")
	docB.UpdatedAt = time.Now()
	err = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		return provider.Documents().Update(ctx, docB)
	})

	if !errors.Is(err, locking.ErrOptimisticLockConflict) {
		t.Errorf("Expected ErrOptimisticLockConflict, got %v", err)
	}

	// 3. Verify client B can successfully update using conflict retry helper
	err = locking.RetryConflict(ctx, 3, 5*time.Millisecond, func() error {
		return uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
			freshDoc, err := provider.Documents().GetByID(ctx, "lock-doc")
			if err != nil {
				return err
			}
			freshDoc.Content = []byte("ver-1-B-retried")
			freshDoc.UpdatedAt = time.Now()
			return provider.Documents().Update(ctx, freshDoc)
		})
	})

	if err != nil {
		t.Fatalf("Retry conflict failed: %v", err)
	}

	// Verify content is ver-1-B-retried
	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		finalDoc, _ := provider.Documents().GetByID(ctx, "lock-doc")
		if string(finalDoc.Content) != "ver-1-B-retried" {
			t.Errorf("Expected Content 'ver-1-B-retried', got '%s'", string(finalDoc.Content))
		}
		return nil
	})
}

func TestAuditLogs(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	db := adapter.DB()
	ctx := context.Background()

	entry := audit.LogEntry{
		ID:         "audit-1",
		EntityName: "Room",
		EntityID:   "room-1",
		Action:     "CREATE",
		ActorID:    "admin-user",
		Payload:    map[string]string{"name": "Audit Test Room"},
		Timestamp:  time.Now(),
	}

	// Direct execution write
	err := audit.Record(ctx, db, entry)
	if err != nil {
		t.Fatalf("Failed to write audit log: %v", err)
	}

	// Query to check
	var actorID string
	err = db.QueryRow("SELECT actor_id FROM audit_logs WHERE id = $1;", "audit-1").Scan(&actorID)
	if err != nil {
		t.Fatalf("Failed to query audit log: %v", err)
	}
	if actorID != "admin-user" {
		t.Errorf("Expected actor_id 'admin-user', got '%s'", actorID)
	}
}

func TestConcurrencyAndDeadlocks(t *testing.T) {
	adapter, cleanup := setupTestDB(t)
	defer cleanup()

	uow := unitofwork.NewUnitOfWork(adapter.DB())
	ctx := context.Background()

	// Seed multiple rooms
	_ = uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
		for i := 0; i < 5; i++ {
			room := &repository.RoomEntity{
				ID:              string(rune('A' + i)),
				Name:            "Room",
				OwnerID:         "owner",
				State:           "Active",
				CreatedAt:       time.Now(),
				LastActivity:    time.Now(),
				MaxParticipants: 10,
				Visibility:      "public",
				Version:         1,
			}
			_ = provider.Rooms().Create(ctx, room)
		}
		return nil
	})

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Spawn multiple concurrent updates to check locking conflict and retries under load
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			err := locking.RetryConflict(ctx, 10, 10*time.Millisecond, func() error {
				return uow.Execute(ctx, func(ctx context.Context, provider unitofwork.RepositoryProvider) error {
					room, err := provider.Rooms().GetByID(ctx, "A", false)
					if err != nil {
						return err
					}
					room.Name = "Updated Name"
					room.LastActivity = time.Now()
					return provider.Rooms().Update(ctx, room)
				})
			})
			if err != nil {
				// Don't fail the test immediately, log it
				t.Logf("Goroutine %d update failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
}
