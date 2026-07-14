// Package filesystem implements the Storage SDK against the local filesystem.
// It is a fully-featured, dependency-free backend: the default for local
// development and the workhorse of the test suite. It stores object bytes under
// <root>/<bucket>/<key> and object metadata under a parallel <root>/.meta tree
// so listings never see sidecar files.
//
// Writes are atomic (temp file + rename) so concurrent uploads to the same key
// never expose a torn object, satisfying the module's concurrency requirements
// without a global lock.
package filesystem

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/sdk"
)

const (
	metaDir   = ".meta"
	tmpSuffix = ".cpiptmp"
)

// Store is a filesystem-backed ObjectStore.
type Store struct {
	root string
	mu   sync.Mutex // serializes bucket create/remove directory races
}

// New constructs a filesystem Store rooted at root, creating it if absent.
func New(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: filesystem root is empty", artifacts.ErrConfig)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("%w: create root: %v", artifacts.ErrBackendUnavailable, err)
	}
	return &Store{root: root}, nil
}

// Name implements sdk.ObjectStore.
func (s *Store) Name() string { return "filesystem" }

// --- path helpers with traversal protection ---

func safeKey(key string) (string, error) {
	clean := filepath.Clean("/" + key) // force absolute, collapse ../
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" || clean == "." {
		return "", fmt.Errorf("%w: empty object key", artifacts.ErrInvalidArtifact)
	}
	return filepath.FromSlash(clean), nil
}

func safeBucket(bucket string) (string, error) {
	if bucket == "" || strings.ContainsAny(bucket, "/\\") || bucket == metaDir {
		return "", fmt.Errorf("%w: invalid bucket %q", artifacts.ErrInvalidArtifact, bucket)
	}
	return bucket, nil
}

func (s *Store) objectPath(bucket, key string) (string, error) {
	b, err := safeBucket(bucket)
	if err != nil {
		return "", err
	}
	k, err := safeKey(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, b, k), nil
}

func (s *Store) metaPath(bucket, key string) (string, error) {
	b, err := safeBucket(bucket)
	if err != nil {
		return "", err
	}
	k, err := safeKey(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, metaDir, b, k+".json"), nil
}

// objectMeta is the on-disk sidecar record.
type objectMeta struct {
	ContentType  string            `json:"content_type"`
	ETag         string            `json:"etag"`
	Size         int64             `json:"size"`
	LastModified time.Time         `json:"last_modified"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// --- Bucket lifecycle ---

func (s *Store) EnsureBucket(_ context.Context, bucket string) error {
	b, err := safeBucket(bucket)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Join(s.root, b), 0o755); err != nil {
		return fmt.Errorf("%w: ensure bucket: %v", artifacts.ErrBackendUnavailable, err)
	}
	return os.MkdirAll(filepath.Join(s.root, metaDir, b), 0o755)
}

func (s *Store) BucketExists(_ context.Context, bucket string) (bool, error) {
	b, err := safeBucket(bucket)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(filepath.Join(s.root, b))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return info.IsDir(), nil
}

func (s *Store) RemoveBucket(_ context.Context, bucket string) error {
	b, err := safeBucket(bucket)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.RemoveAll(filepath.Join(s.root, metaDir, b))
	if err := os.RemoveAll(filepath.Join(s.root, b)); err != nil {
		return fmt.Errorf("%w: remove bucket: %v", artifacts.ErrBackendUnavailable, err)
	}
	return nil
}

func (s *Store) ListBuckets(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && e.Name() != metaDir {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// --- Object operations ---

func (s *Store) Upload(ctx context.Context, in sdk.PutInput) (sdk.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.PutResult{}, err
	}
	objPath, err := s.objectPath(in.Bucket, in.Key)
	if err != nil {
		return sdk.PutResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return sdk.PutResult{}, fmt.Errorf("%w: mkdir: %v", artifacts.ErrUploadFailed, err)
	}

	tmp := objPath + tmpSuffix + randomSuffix()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return sdk.PutResult{}, fmt.Errorf("%w: open temp: %v", artifacts.ErrUploadFailed, err)
	}
	hasher := md5.New()
	n, err := io.Copy(io.MultiWriter(f, hasher), in.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return sdk.PutResult{}, fmt.Errorf("%w: write: %v", artifacts.ErrUploadFailed, err)
	}
	etag := hex.EncodeToString(hasher.Sum(nil))

	// Atomic publish.
	if err := os.Rename(tmp, objPath); err != nil {
		_ = os.Remove(tmp)
		return sdk.PutResult{}, fmt.Errorf("%w: rename: %v", artifacts.ErrUploadFailed, err)
	}

	meta := objectMeta{
		ContentType:  in.ContentType,
		ETag:         etag,
		Size:         n,
		LastModified: time.Now().UTC(),
		Metadata:     in.Metadata,
	}
	if err := s.writeMeta(in.Bucket, in.Key, meta); err != nil {
		return sdk.PutResult{}, err
	}
	return sdk.PutResult{ETag: etag, Size: n}, nil
}

func (s *Store) writeMeta(bucket, key string, meta objectMeta) error {
	mp, err := s.metaPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(mp), 0o755); err != nil {
		return fmt.Errorf("%w: meta mkdir: %v", artifacts.ErrUploadFailed, err)
	}
	data, _ := json.Marshal(meta)
	tmp := mp + tmpSuffix + randomSuffix()
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("%w: meta write: %v", artifacts.ErrUploadFailed, err)
	}
	if err := os.Rename(tmp, mp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%w: meta rename: %v", artifacts.ErrUploadFailed, err)
	}
	return nil
}

func (s *Store) readMeta(bucket, key string) (objectMeta, error) {
	mp, err := s.metaPath(bucket, key)
	if err != nil {
		return objectMeta{}, err
	}
	data, err := os.ReadFile(mp)
	if err != nil {
		return objectMeta{}, err // caller synthesizes if missing
	}
	var m objectMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return objectMeta{}, err
	}
	return m, nil
}

func (s *Store) Download(ctx context.Context, ref sdk.ObjectRef, opts sdk.GetOptions) (*sdk.GetOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	obj, err := s.Stat(ctx, ref)
	if err != nil {
		return nil, err
	}
	objPath, _ := s.objectPath(ref.Bucket, ref.Key)
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s/%s", artifacts.ErrObjectNotFound, ref.Bucket, ref.Key)
		}
		return nil, fmt.Errorf("%w: open: %v", artifacts.ErrDownloadFailed, err)
	}
	var body io.ReadCloser = f
	if opts.Range != nil {
		start := opts.Range.Start
		end := opts.Range.End
		if end >= obj.Size {
			end = obj.Size - 1
		}
		if start < 0 || start > end {
			_ = f.Close()
			return nil, fmt.Errorf("%w: invalid range", artifacts.ErrDownloadFailed)
		}
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("%w: seek: %v", artifacts.ErrDownloadFailed, err)
		}
		length := end - start + 1
		body = &sectionReadCloser{Reader: io.LimitReader(f, length), closer: f}
		obj.Size = length
	}
	return &sdk.GetOutput{Body: body, Object: obj}, nil
}

func (s *Store) Stat(_ context.Context, ref sdk.ObjectRef) (sdk.Object, error) {
	objPath, err := s.objectPath(ref.Bucket, ref.Key)
	if err != nil {
		return sdk.Object{}, err
	}
	info, err := os.Stat(objPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sdk.Object{}, fmt.Errorf("%w: %s/%s", artifacts.ErrObjectNotFound, ref.Bucket, ref.Key)
		}
		return sdk.Object{}, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	obj := sdk.Object{
		Bucket:       ref.Bucket,
		Key:          ref.Key,
		Size:         info.Size(),
		LastModified: info.ModTime().UTC(),
	}
	if meta, err := s.readMeta(ref.Bucket, ref.Key); err == nil {
		obj.ETag = meta.ETag
		obj.ContentType = meta.ContentType
		obj.Metadata = meta.Metadata
		if meta.Size > 0 {
			obj.Size = meta.Size
		}
		if !meta.LastModified.IsZero() {
			obj.LastModified = meta.LastModified
		}
	}
	return obj, nil
}

func (s *Store) Exists(ctx context.Context, ref sdk.ObjectRef) (bool, error) {
	_, err := s.Stat(ctx, ref)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, artifacts.ErrObjectNotFound) {
		return false, nil
	}
	return false, err
}

func (s *Store) Delete(_ context.Context, ref sdk.ObjectRef) error {
	objPath, err := s.objectPath(ref.Bucket, ref.Key)
	if err != nil {
		return err
	}
	if err := os.Remove(objPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	if mp, err := s.metaPath(ref.Bucket, ref.Key); err == nil {
		_ = os.Remove(mp)
	}
	return nil
}

func (s *Store) List(_ context.Context, bucket string, opts sdk.ListOptions) (sdk.ListResult, error) {
	b, err := safeBucket(bucket)
	if err != nil {
		return sdk.ListResult{}, err
	}
	base := filepath.Join(s.root, b)
	var objs []sdk.Object
	walkErr := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.IsDir() || strings.Contains(path, tmpSuffix) {
			return nil
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}
		if opts.StartAfter != "" && key <= opts.StartAfter {
			return nil
		}
		objs = append(objs, sdk.Object{Bucket: bucket, Key: key, Size: info.Size(), LastModified: info.ModTime().UTC()})
		return nil
	})
	if walkErr != nil {
		return sdk.ListResult{}, fmt.Errorf("%w: walk: %v", artifacts.ErrBackendUnavailable, walkErr)
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })
	res := sdk.ListResult{}
	if opts.MaxKeys > 0 && len(objs) > opts.MaxKeys {
		res.NextMarker = objs[opts.MaxKeys-1].Key
		objs = objs[:opts.MaxKeys]
		res.IsTruncated = true
	}
	res.Objects = objs
	return res, nil
}

func (s *Store) Copy(ctx context.Context, src, dst sdk.ObjectRef) error {
	out, err := s.Download(ctx, src, sdk.GetOptions{})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	_, err = s.Upload(ctx, sdk.PutInput{
		Bucket:      dst.Bucket,
		Key:         dst.Key,
		Body:        out.Body,
		Size:        out.Object.Size,
		ContentType: out.Object.ContentType,
		Metadata:    out.Object.Metadata,
	})
	return err
}

func (s *Store) Move(ctx context.Context, src, dst sdk.ObjectRef) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

// GenerateSignedURL returns a file:// URL. The filesystem backend cannot mint
// HTTP-authenticated URLs; the URL is a local reference only (documented
// non-authoritative). Real presigning is provided by the S3/MinIO adapters.
func (s *Store) GenerateSignedURL(_ context.Context, ref sdk.ObjectRef, _ sdk.SignedURLOptions) (string, error) {
	objPath, err := s.objectPath(ref.Bucket, ref.Key)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(objPath)
	if err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(abs), nil
}

// Validate confirms the root directory is writable.
func (s *Store) Validate(_ context.Context) error {
	probe := filepath.Join(s.root, ".cpip-probe"+randomSuffix())
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("%w: root not writable: %v", artifacts.ErrBackendUnavailable, err)
	}
	_ = os.Remove(probe)
	return nil
}

// Close is a no-op for the filesystem backend.
func (s *Store) Close() error { return nil }

// --- helpers ---

type sectionReadCloser struct {
	io.Reader
	closer io.Closer
}

func (s *sectionReadCloser) Close() error { return s.closer.Close() }

// randomSuffix returns a short unique suffix for temp files. It uses the OS
// PID + a monotonic counter to avoid time-based collisions under concurrency.
var tmpCounter atomic.Int64

func randomSuffix() string {
	return fmt.Sprintf(".%d.%d", os.Getpid(), tmpCounter.Add(1))
}

var _ sdk.ObjectStore = (*Store)(nil)
