package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// BackupComponent represents isolated sub-system data.
type BackupComponent string

const (
	ComponentPostgres BackupComponent = "postgres"
	ComponentRedis    BackupComponent = "redis"
	ComponentArtifact BackupComponent = "artifacts"
	ComponentConfig   BackupComponent = "config"
)

// BackupMetadata details a single backup revision archive.
type BackupMetadata struct {
	ID         string            `json:"id"`
	Timestamp  time.Time         `json:"timestamp"`
	Components []BackupComponent `json:"components"`
	Checksum   string            `json:"checksum"`
	Path       string            `json:"path"`
	Size       int64             `json:"size"`
	Validated  bool              `json:"validated"`
}

// BackupManager orchestrates system exports, validations, and retention policies.
type BackupManager struct {
	mu        sync.Mutex
	backupDir string
	cfg       config.BackupPolicy
	catalog   []BackupMetadata
	bus       *events.Bus
	metrics   metrics.Recorder
}

// NewBackupManager constructs a BackupManager.
func NewBackupManager(backupDir string, cfg config.BackupPolicy, bus *events.Bus, rec metrics.Recorder) (*BackupManager, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup root: %w", err)
	}

	bm := &BackupManager{
		backupDir: backupDir,
		cfg:       cfg,
		catalog:   make([]BackupMetadata, 0),
		bus:       bus,
		metrics:   rec,
	}

	// Load existing catalog if it exists
	bm.loadCatalog()

	return bm, nil
}

// CreateBackup exports states of requested components, aggregates them, and records checksums.
func (bm *BackupManager) CreateBackup(ctx context.Context, components []BackupComponent) (BackupMetadata, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.metrics != nil {
		bm.metrics.Inc(metrics.MetricBackupRuns)
	}

	id := fmt.Sprintf("backup-%d", time.Now().UnixNano())
	backupFilePath := filepath.Join(bm.backupDir, id+".data")

	// Simulate system backup data export
	file, err := os.Create(backupFilePath)
	if err != nil {
		if bm.metrics != nil {
			bm.metrics.Inc(metrics.MetricBackupFailures)
		}
		return BackupMetadata{}, err
	}
	defer file.Close()

	// Write simulated data block
	hash := sha256.New()
	multiWriter := io.MultiWriter(file, hash)

	for _, comp := range components {
		select {
		case <-ctx.Done():
			os.Remove(backupFilePath)
			return BackupMetadata{}, ctx.Err()
		default:
			data := fmt.Sprintf("[%s state snapshot dump timestamp:%v]\n", comp, time.Now())
			if _, err := multiWriter.Write([]byte(data)); err != nil {
				return BackupMetadata{}, err
			}
		}
	}

	checksum := hex.EncodeToString(hash.Sum(nil))
	stat, _ := file.Stat()

	meta := BackupMetadata{
		ID:         id,
		Timestamp:  time.Now(),
		Components: components,
		Checksum:   checksum,
		Path:       backupFilePath,
		Size:       stat.Size(),
		Validated:  false,
	}

	if bm.cfg.ValidationEnabled {
		meta.Validated = bm.validateBackup(meta)
	}

	bm.catalog = append(bm.catalog, meta)
	bm.saveCatalog()

	if bm.bus != nil {
		bm.bus.Publish(events.Event{
			Type:      events.BackupCompleted,
			Timestamp: time.Now(),
			Detail:    fmt.Sprintf("Backup %q created successfully. Size: %d bytes. Checksum: %s", id, meta.Size, checksum),
		})
	}

	// Enforce Retention Limits
	if err := bm.pruneCatalog(); err != nil {
		// Log error but do not fail backup
		fmt.Printf("failed to prune backups: %v\n", err)
	}

	return meta, nil
}

// RestoreBackup verifies checksums and restores states to the respective databases.
func (bm *BackupManager) RestoreBackup(ctx context.Context, meta BackupMetadata) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Verify file existence
	if _, err := os.Stat(meta.Path); err != nil {
		return fmt.Errorf("backup file missing: %w", err)
	}

	// Verify integrity checksum
	if !bm.validateBackup(meta) {
		return fmt.Errorf("integrity check failed for backup %q; checksum mismatch", meta.ID)
	}

	// Simulate db restoring actions
	for range meta.Components {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Perform restoration steps
			time.Sleep(10 * time.Millisecond)
		}
	}

	if bm.bus != nil {
		bm.bus.Publish(events.Event{
			Type:      events.RestoreCompleted,
			Timestamp: time.Now(),
			Detail:    fmt.Sprintf("Backup %q restored successfully", meta.ID),
		})
	}

	return nil
}

func (bm *BackupManager) validateBackup(meta BackupMetadata) bool {
	file, err := os.Open(meta.Path)
	if err != nil {
		return false
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false
	}

	currentSum := hex.EncodeToString(hash.Sum(nil))
	return currentSum == meta.Checksum
}

func (bm *BackupManager) pruneCatalog() error {
	if len(bm.catalog) <= bm.cfg.RetentionLimit {
		return nil
	}

	// Sort oldest first
	limit := len(bm.catalog) - bm.cfg.RetentionLimit
	toPrune := bm.catalog[:limit]
	bm.catalog = bm.catalog[limit:]

	for _, meta := range toPrune {
		_ = os.Remove(meta.Path)
	}

	bm.saveCatalog()
	return nil
}

func (bm *BackupManager) loadCatalog() {
	path := filepath.Join(bm.backupDir, "catalog.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &bm.catalog)
}

func (bm *BackupManager) saveCatalog() {
	path := filepath.Join(bm.backupDir, "catalog.json")
	data, _ := json.MarshalIndent(bm.catalog, "", "  ")
	_ = os.WriteFile(path, data, 0644)
}

func (bm *BackupManager) History() []BackupMetadata {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return append([]BackupMetadata(nil), bm.catalog...)
}
