package collaboration

import (
	"context"
	"sync"
	"testing"
	"time"

	"cpip/internal/collaboration/config"
	"cpip/internal/collaboration/events"
	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
)

func TestGetOrCreateDocument(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	cfg := config.Default()
	mgr := NewManager(Params{
		Config: cfg,
		Repo:   repo,
	})

	// Subscribe to events
	ch := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(ch)

	ctx := context.Background()
	docID := "doc-1"
	roomID := "room-1"
	filePath := "main.go"

	// 1. Create document
	doc, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to create document: %v", err)
	}

	if doc == nil {
		t.Fatal("expected document wrapper, got nil")
	}

	// Verify state in registry
	entry, ok := mgr.Registry().Get(docID)
	if !ok {
		t.Fatal("document not found in registry")
	}

	if entry.State != types.StateInitialized {
		t.Errorf("expected state to be Initialized, got %s", entry.State)
	}

	// Check events
	select {
	case ev := <-ch:
		if ev.Type != events.DocumentCreated {
			t.Errorf("expected event DocumentCreated, got %s", ev.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for DocumentCreated event")
	}

	select {
	case ev := <-ch:
		if ev.Type != events.DocumentInitialized {
			t.Errorf("expected event DocumentInitialized, got %s", ev.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for DocumentInitialized event")
	}

	// 2. Fetch again, should return cached
	doc2, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to get document: %v", err)
	}

	if doc != doc2 {
		t.Error("expected cached document instance to be returned")
	}
}

func TestDeltaSyncHandshake(t *testing.T) {
	repo1 := storage.NewInMemoryRepository()
	cfg := config.Default()
	mgr1 := NewManager(Params{Config: cfg, Repo: repo1})

	repo2 := storage.NewInMemoryRepository()
	mgr2 := NewManager(Params{Config: cfg, Repo: repo2})

	ctx := context.Background()

	// 1. Alice creates document and inserts text
	aliceDoc, err := mgr1.GetOrCreateDocument(ctx, "shared-doc", "room-1", "file.txt")
	if err != nil {
		t.Fatalf("failed to create Alice's doc: %v", err)
	}

	aliceDoc.InsertText(0, "Hello ")
	aliceDoc.InsertText(6, "World!")

	// 2. Bob creates a blank document with the same ID
	bobDoc, err := mgr2.GetOrCreateDocument(ctx, "shared-doc", "room-1", "file.txt")
	if err != nil {
		t.Fatalf("failed to create Bob's doc: %v", err)
	}

	// 3. Bob initiates Sync Step 1: gets state vector
	bobSV := bobDoc.EncodeStateVector()

	// 4. Alice handles Sync Step 1: parses Bob's SV, generates step 2 update
	aliceUpdate, err := mgr1.HandleSyncStep1(ctx, "shared-doc", bobSV)
	if err != nil {
		t.Fatalf("failed handling sync step 1: %v", err)
	}

	// 5. Bob applies Alice's update
	err = mgr2.ApplyIncrementalUpdate(ctx, "shared-doc", aliceUpdate)
	if err != nil {
		t.Fatalf("failed applying update: %v", err)
	}

	// 6. Assert Bob's text matches Alice's text
	if bobDoc.GetText() != "Hello World!" {
		t.Errorf("expected Bob's text to be 'Hello World!', got '%s'", bobDoc.GetText())
	}
}

func TestPersistenceSnapshotAndRecovery(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	cfg := config.Default()
	mgr := NewManager(Params{
		Config: cfg,
		Repo:   repo,
	})

	ctx := context.Background()
	docID := "persistent-doc"
	roomID := "room-1"
	filePath := "test.txt"

	// 1. Create document and make edits
	doc, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to create document: %v", err)
	}

	doc.InsertText(0, "Stage 1: Initial Text. ")
	err = mgr.ApplyIncrementalUpdate(ctx, docID, doc.EncodeStateAsUpdate(nil))
	if err != nil {
		t.Fatalf("failed applying update: %v", err)
	}

	// Save snapshot
	if err := mgr.SaveSnapshot(ctx, docID); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	// 2. Make more edits (incremental updates saved to DB)
	doc.InsertText(len(doc.GetText()), "Stage 2: Incremental Text.")
	err = mgr.ApplyIncrementalUpdate(ctx, docID, doc.EncodeStateAsUpdate(nil))
	if err != nil {
		t.Fatalf("failed applying update: %v", err)
	}

	// Verify registry is in Dirty state
	entry, ok := mgr.Registry().Get(docID)
	if !ok {
		t.Fatal("failed to find document in registry")
	}
	if entry.State != types.StateDirty {
		t.Errorf("expected state to be Dirty, got %s", entry.State)
	}

	// 3. Archive/Unload document
	if err := mgr.ArchiveDocument(ctx, docID); err != nil {
		t.Fatalf("failed to archive document: %v", err)
	}

	// Verify document is unloaded from registry cache
	if _, ok := mgr.Registry().Get(docID); ok {
		t.Fatal("expected document to be unregistered from cache after archiving")
	}

	// 4. Fetch document again (should trigger recovery)
	recoveredDoc, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to recover document: %v", err)
	}

	expectedText := "Stage 1: Initial Text. Stage 2: Incremental Text."
	if recoveredDoc.GetText() != expectedText {
		t.Errorf("expected recovered text to be '%s', got '%s'", expectedText, recoveredDoc.GetText())
	}
}

func TestBackgroundWorkers(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	cfg := config.Config{
		SnapshotInterval:       30 * time.Millisecond,
		SnapshotEditsThreshold: 1,
		BackgroundSaveInterval: 10 * time.Millisecond,
		IdleTimeout:            120 * time.Millisecond,
		MaxDocumentSize:        10 * 1024 * 1024,
		MaxPendingUpdatesLimit: 1000,
		RetentionCount:         5,
	}

	mgr := NewManager(Params{
		Config: cfg,
		Repo:   repo,
	})

	mgr.Start()
	defer mgr.Stop()

	ctx := context.Background()
	docID := "bg-doc"
	roomID := "room-1"
	filePath := "bg.txt"

	doc, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to create document: %v", err)
	}

	// Make edits to trigger dirty state
	doc.InsertText(0, "Edit 1")
	if err := mgr.ApplyIncrementalUpdate(ctx, docID, doc.EncodeStateAsUpdate(nil)); err != nil {
		t.Fatalf("failed applying update: %v", err)
	}

	// Wait for background save loop to snapshot the document (takes > 30ms)
	time.Sleep(60 * time.Millisecond)

	// Verify registry is updated to Persisted state using ListAll (does not update LastAccess)
	foundDoc := false
	isDirty := false
	for _, e := range mgr.Registry().ListAll() {
		if e.ID == docID {
			foundDoc = true
			isDirty = e.IsDirty
			break
		}
	}
	if foundDoc {
		if isDirty {
			t.Error("expected document to be saved and clean")
		}
	} else {
		t.Error("expected document to still be in registry")
	}

	// Wait for IdleTimeout (120ms) to archive/unload document
	time.Sleep(150 * time.Millisecond)

	// Verify document is unloaded using ListAll (does not update LastAccess)
	found := false
	for _, e := range mgr.Registry().ListAll() {
		if e.ID == docID {
			found = true
			break
		}
	}
	if found {
		t.Error("expected idle document to be archived and unloaded by background worker")
	}
}

func TestConcurrentEdits(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	cfg := config.Default()
	mgr := NewManager(Params{
		Config: cfg,
		Repo:   repo,
	})

	ctx := context.Background()
	docID := "concurrent-doc"
	roomID := "room-1"
	filePath := "concurrent.txt"

	doc, err := mgr.GetOrCreateDocument(ctx, docID, roomID, filePath)
	if err != nil {
		t.Fatalf("failed to create document: %v", err)
	}

	var wg sync.WaitGroup
	workers := 5
	editsPerWorker := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < editsPerWorker; j++ {
				// Thread-safe mutations
				doc.InsertText(0, "A")
				
				// Generate local updates
				up := doc.EncodeStateAsUpdate(nil)
				
				// Apply incremental update (concurrency-safe)
				_ = mgr.ApplyIncrementalUpdate(ctx, docID, up)
				
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Verify document text length is correct
	expectedLength := workers * editsPerWorker
	if len(doc.GetText()) != expectedLength {
		t.Errorf("expected text length to be %d, got %d", expectedLength, len(doc.GetText()))
	}
}
