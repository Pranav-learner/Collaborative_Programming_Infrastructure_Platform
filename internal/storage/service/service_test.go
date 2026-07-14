package service_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
	"cpip/internal/storage/download"
	"cpip/internal/storage/service"
	"cpip/internal/storage/upload"
)

// newTestService wires a full storage stack over a real filesystem backend and
// the in-memory metadata store — a genuine end-to-end integration harness.
func newTestService(t *testing.T) *service.Service {
	t.Helper()
	cfg := config.Default()
	cfg.Provider = config.ProviderFilesystem
	cfg.Backend.FilesystemRoot = t.TempDir()
	cfg.Cleanup.Enabled = false // drive the reaper manually in tests

	svc, err := service.New(service.Params{Config: cfg})
	if err != nil {
		t.Fatalf("service.New: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("service.Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	return svc
}

func mustUpload(t *testing.T, svc *service.Service, req upload.Request) *upload.Result {
	t.Helper()
	res, err := svc.Artifacts().Upload(context.Background(), req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	return res
}

func readAll(t *testing.T, out *download.Output) []byte {
	t.Helper()
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := out.Verify(); err != nil {
		t.Fatalf("integrity verify: %v", err)
	}
	return b
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("hello-artifact "), 500)

	res := mustUpload(t, svc, upload.Request{
		Data: payload, Type: artifacts.ExecutionLog, ContentType: "text/plain", Owner: "alice",
	})
	if res.Artifact.State != artifacts.Available {
		t.Fatalf("expected Available, got %s", res.Artifact.State)
	}
	if res.Artifact.Version != 1 {
		t.Fatalf("first version should be 1, got %d", res.Artifact.Version)
	}
	// Compressible log should be gzip-compressed on the backend.
	if res.Artifact.Compression.Algorithm != artifacts.Gzip {
		t.Fatalf("expected gzip compression, got %s", res.Artifact.Compression.Algorithm)
	}

	out, err := svc.Artifacts().Download(ctx, download.Request{ArtifactID: res.Artifact.ID})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got := readAll(t, out)
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d bytes want %d", len(got), len(payload))
	}
}

func TestVerifiedDownload(t *testing.T) {
	svc := newTestService(t)
	payload := []byte("verify-me-strongly")
	res := mustUpload(t, svc, upload.Request{Data: payload, Type: artifacts.UploadedFile, ContentType: "application/octet-stream"})

	out, err := svc.Artifacts().Download(context.Background(), download.Request{ArtifactID: res.Artifact.ID, Verify: true})
	if err != nil {
		t.Fatalf("verified download: %v", err)
	}
	defer out.Body.Close()
	got, _ := io.ReadAll(out.Body)
	if !bytes.Equal(got, payload) {
		t.Fatalf("verified round-trip mismatch")
	}
}

func TestContentDeduplication(t *testing.T) {
	svc := newTestService(t)
	payload := bytes.Repeat([]byte("dedup"), 1000)

	first := mustUpload(t, svc, upload.Request{Data: payload, Type: artifacts.ExecutionLog})
	if first.Deduplicated {
		t.Fatalf("first upload should not be deduplicated")
	}
	second := mustUpload(t, svc, upload.Request{Data: payload, Type: artifacts.ExecutionLog})
	if !second.Deduplicated {
		t.Fatalf("second identical upload should be deduplicated")
	}
	// Distinct artifacts, same physical object key.
	if first.Artifact.ID == second.Artifact.ID {
		t.Fatalf("dedup must still mint a distinct artifact record")
	}
	if first.Artifact.ObjectKey != second.Artifact.ObjectKey {
		t.Fatalf("deduplicated artifacts must share the object key")
	}
	if second.BytesStored != 0 {
		t.Fatalf("dedup should store no new bytes, stored %d", second.BytesStored)
	}
	// Both remain downloadable.
	out, _ := svc.Artifacts().Download(context.Background(), download.Request{ArtifactID: second.Artifact.ID})
	if !bytes.Equal(readAll(t, out), payload) {
		t.Fatalf("deduplicated artifact not downloadable")
	}
}

func TestVersioningAndRollback(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	v1 := mustUpload(t, svc, upload.Request{Data: []byte("v1-content"), Type: artifacts.SourceArchive})
	lineage := v1.Artifact.LineageID
	v2 := mustUpload(t, svc, upload.Request{Data: []byte("v2-content"), Type: artifacts.SourceArchive, LineageID: lineage})
	v3 := mustUpload(t, svc, upload.Request{Data: []byte("v3-content"), Type: artifacts.SourceArchive, LineageID: lineage})

	if v2.Artifact.Version != 2 || v3.Artifact.Version != 3 {
		t.Fatalf("versions not sequential: %d, %d", v2.Artifact.Version, v3.Artifact.Version)
	}
	latest, _ := svc.Artifacts().Latest(ctx, lineage)
	if latest.Version != 3 {
		t.Fatalf("latest should be v3, got %d", latest.Version)
	}
	hist, _ := svc.Artifacts().History(ctx, lineage)
	if len(hist) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(hist))
	}

	// Roll back to v1; head moves without destroying history.
	if _, err := svc.Artifacts().Rollback(ctx, lineage, 1); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	latest, _ = svc.Artifacts().Latest(ctx, lineage)
	if latest.Version != 1 {
		t.Fatalf("after rollback latest should be v1, got %d", latest.Version)
	}
	// Latest download returns v1 content.
	out, _ := svc.Artifacts().Download(ctx, download.Request{LineageID: lineage})
	if !bytes.Equal(readAll(t, out), []byte("v1-content")) {
		t.Fatalf("rolled-back latest content mismatch")
	}
}

func TestDeleteRestorePurge(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	res := mustUpload(t, svc, upload.Request{Data: []byte("deletable"), Type: artifacts.DebugBundle})
	id := res.Artifact.ID

	// Soft delete.
	if err := svc.Artifacts().Delete(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _ := svc.Artifacts().Get(ctx, id)
	if got.State != artifacts.Deleted {
		t.Fatalf("expected Deleted, got %s", got.State)
	}
	// Restore brings it back (bytes retained on soft delete).
	if err := svc.Artifacts().Restore(ctx, id); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, _ = svc.Artifacts().Get(ctx, id)
	if got.State != artifacts.Available {
		t.Fatalf("expected Available after restore, got %s", got.State)
	}
	// Purge removes bytes + metadata.
	if err := svc.Artifacts().Purge(ctx, id); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := svc.Artifacts().Get(ctx, id); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("expected not found after purge, got %v", err)
	}
}

func TestLegalHoldBlocksDeletion(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	res := mustUpload(t, svc, upload.Request{Data: []byte("held"), Type: artifacts.Template})
	id := res.Artifact.ID

	if err := svc.Artifacts().SetLegalHold(ctx, id, true); err != nil {
		t.Fatalf("set legal hold: %v", err)
	}
	if err := svc.Artifacts().Delete(ctx, id); !errors.Is(err, artifacts.ErrLegalHold) {
		t.Fatalf("expected legal hold to block delete, got %v", err)
	}
	// Releasing the hold permits deletion.
	_ = svc.Artifacts().SetLegalHold(ctx, id, false)
	if err := svc.Artifacts().Delete(ctx, id); err != nil {
		t.Fatalf("delete after hold release: %v", err)
	}
}

func TestRetentionCleanupExpires(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Hour)
	res := mustUpload(t, svc, upload.Request{
		Data: []byte("ephemeral"), Type: artifacts.ExecutionOutput,
		Retention: artifacts.RetentionPolicy{Mode: artifacts.RetainUntil, ExpireAt: &past},
	})
	rep, err := svc.Cleanup().RunOnce(ctx)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if rep.Purged != 1 {
		t.Fatalf("expected 1 purged, got %+v", rep)
	}
	if _, err := svc.Artifacts().Get(ctx, res.Artifact.ID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("expired artifact should be purged, got %v", err)
	}
}

func TestReconcileDetectsMissingObject(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	res := mustUpload(t, svc, upload.Request{Data: []byte("orphan-meta"), Type: artifacts.WorkspaceArchive})
	a := res.Artifact
	if err := svc.Artifacts().Reconcile(ctx, a.ID); err != nil {
		t.Fatalf("healthy artifact should reconcile: %v", err)
	}
	// Delete the physical object out from under the metadata.
	if err := svc.Objects().Delete(ctx, a.Bucket, a.ObjectKey); err != nil {
		t.Fatalf("backend delete: %v", err)
	}
	if err := svc.Artifacts().Reconcile(ctx, a.ID); !errors.Is(err, artifacts.ErrMetadataInconsistent) {
		t.Fatalf("expected metadata inconsistency, got %v", err)
	}
}

func TestConcurrentUploadsSameLineage(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	seed := mustUpload(t, svc, upload.Request{Data: []byte("seed"), Type: artifacts.ExecutionLog})
	lineage := seed.Artifact.LineageID

	const n = 30
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Artifacts().Upload(ctx, upload.Request{
				Data: []byte(fmt.Sprintf("payload-%d", i)), Type: artifacts.ExecutionLog,
				LineageID: lineage, DisableDedup: true,
			})
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent upload error: %v", err)
		}
	}
	hist, _ := svc.Artifacts().History(ctx, lineage)
	if len(hist) != n+1 {
		t.Fatalf("expected %d versions, got %d", n+1, len(hist))
	}
	heads := 0
	versions := map[int64]bool{}
	for _, h := range hist {
		if h.IsLatest {
			heads++
		}
		if versions[h.Version] {
			t.Fatalf("duplicate version %d under concurrency", h.Version)
		}
		versions[h.Version] = true
	}
	if heads != 1 {
		t.Fatalf("expected exactly one head, got %d", heads)
	}
}

func TestConcurrentDownloads(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("concurrent-read "), 200)
	res := mustUpload(t, svc, upload.Request{Data: payload, Type: artifacts.RuntimeLog})

	const n = 40
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := svc.Artifacts().Download(ctx, download.Request{ArtifactID: res.Artifact.ID})
			if err != nil {
				errCh <- err
				return
			}
			defer out.Body.Close()
			got, err := io.ReadAll(out.Body)
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(got, payload) {
				errCh <- errors.New("content mismatch under concurrency")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent download: %v", err)
		}
	}
}

func TestUploadRejectsOversized(t *testing.T) {
	cfg := config.Default()
	cfg.Provider = config.ProviderFilesystem
	cfg.Backend.FilesystemRoot = t.TempDir()
	cfg.MaxObjectSize = 16
	cfg.Cleanup.Enabled = false
	svc, err := service.New(service.Params{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	_, err = svc.Artifacts().Upload(context.Background(), upload.Request{
		Data: bytes.Repeat([]byte("x"), 64), Type: artifacts.ExecutionLog,
	})
	if !errors.Is(err, artifacts.ErrObjectTooLarge) {
		t.Fatalf("expected too-large error, got %v", err)
	}
}

func TestEventsArePublished(t *testing.T) {
	svc := newTestService(t)
	sub := svc.Events().Subscribe(64)

	_ = mustUpload(t, svc, upload.Request{Data: []byte("event-src"), Type: artifacts.ExecutionLog})

	// Drain briefly and assert the created/uploaded events fired.
	seen := map[string]bool{}
	timeout := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case e := <-sub:
			seen[string(e.Type)] = true
		case <-timeout:
			t.Fatalf("did not observe expected events, saw %v", seen)
		}
	}
}
