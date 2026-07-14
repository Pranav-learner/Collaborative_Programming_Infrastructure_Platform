package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/sdk"
)

// Options configures the S3 adapter.
type Options struct {
	// Endpoint is host[:port] of the S3 service. Empty → AWS ("s3.<region>.amazonaws.com").
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	// PathStyle puts the bucket in the URL path (MinIO, localhost). AWS uses
	// virtual-host style when false.
	PathStyle      bool
	RequestTimeout time.Duration
	// HTTPClient overrides the default client (tests inject httptest transports).
	HTTPClient *http.Client
	// name overrides the reported adapter name (MinIO adapter sets "minio").
	name string
}

// Store is an S3-compatible ObjectStore.
type Store struct {
	opts   Options
	creds  credentials
	http   *http.Client
	scheme string
	name   string
	now    func() time.Time
}

// NewNamed constructs an S3 adapter that reports a custom backend name (used by
// the MinIO adapter, which is an S3 store with MinIO-specific defaults).
func NewNamed(opts Options, name string) (*Store, error) {
	opts.name = name
	return New(opts)
}

// New constructs an S3 adapter.
func New(opts Options) (*Store, error) {
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: opts.RequestTimeout}
	}
	scheme := "http"
	if opts.UseSSL {
		scheme = "https"
	}
	name := opts.name
	if name == "" {
		name = "s3"
	}
	return &Store{
		opts:   opts,
		creds:  credentials{accessKey: opts.AccessKey, secretKey: opts.SecretKey, region: opts.Region},
		http:   client,
		scheme: scheme,
		name:   name,
		now:    time.Now,
	}, nil
}

// Name implements sdk.ObjectStore.
func (s *Store) Name() string { return s.name }

// host returns the host header/authority for a bucket given the addressing style.
func (s *Store) host(bucket string) string {
	ep := s.opts.Endpoint
	if ep == "" {
		ep = "s3." + s.opts.Region + ".amazonaws.com"
	}
	if bucket == "" || s.opts.PathStyle {
		return ep
	}
	return bucket + "." + ep
}

// buildURL constructs the request URL and its host for a bucket/key pair.
func (s *Store) buildURL(bucket, key string, query url.Values) (u *url.URL, host string) {
	host = s.host(bucket)
	path := "/"
	if s.opts.PathStyle {
		if bucket != "" {
			path = "/" + bucket
		}
		if key != "" {
			path += "/" + key
		}
	} else if key != "" {
		path = "/" + key
	}
	u = &url.URL{Scheme: s.scheme, Host: host, Path: path}
	if query != nil {
		u.RawQuery = query.Encode()
	}
	return u, host
}

// do signs and executes a request, returning the response for the caller to
// consume/close. reqHash is the payload hash (unsignedPayload for streams).
func (s *Store) do(ctx context.Context, method, bucket, key string, query url.Values, body io.Reader, size int64, header http.Header, payloadHash string) (*http.Response, error) {
	u, host := s.buildURL(bucket, key, query)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", artifacts.ErrBackendUnavailable, err)
	}
	req.Host = host
	if size >= 0 {
		req.ContentLength = size
	}
	for k, vals := range header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	s.creds.sign(req, payloadHash, s.now())

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return resp, nil
}

// errorFromResponse maps a non-2xx response to a canonical error, consuming body.
func errorFromResponse(resp *http.Response, bucket, key string) error {
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	switch resp.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s/%s", artifacts.ErrObjectNotFound, bucket, key)
	case http.StatusForbidden, http.StatusUnauthorized:
		return fmt.Errorf("%w: backend rejected credentials (%d)", artifacts.ErrBackendUnavailable, resp.StatusCode)
	}
	var se s3Error
	if err := xml.Unmarshal(data, &se); err == nil && se.Code != "" {
		if se.Code == "NoSuchKey" || se.Code == "NoSuchBucket" {
			return fmt.Errorf("%w: %s", artifacts.ErrObjectNotFound, se.Message)
		}
		return fmt.Errorf("%w: s3 %s: %s", artifacts.ErrBackendUnavailable, se.Code, se.Message)
	}
	return fmt.Errorf("%w: unexpected status %d", artifacts.ErrBackendUnavailable, resp.StatusCode)
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
}

// --- Bucket lifecycle ---

func (s *Store) EnsureBucket(ctx context.Context, bucket string) error {
	exists, err := s.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	resp, err := s.do(ctx, http.MethodPut, bucket, "", nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		// A concurrent create or pre-existing bucket owned by us is benign.
		if resp.StatusCode == http.StatusConflict {
			drain(resp)
			return nil
		}
		return errorFromResponse(resp, bucket, "")
	}
	drain(resp)
	return nil
}

func (s *Store) BucketExists(ctx context.Context, bucket string) (bool, error) {
	resp, err := s.do(ctx, http.MethodHead, bucket, "", nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return false, err
	}
	drain(resp)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	case http.StatusForbidden:
		// Bucket exists but not owned/accessible — report as unavailable.
		return false, fmt.Errorf("%w: bucket %q access forbidden", artifacts.ErrBackendUnavailable, bucket)
	}
	return false, fmt.Errorf("%w: head bucket status %d", artifacts.ErrBackendUnavailable, resp.StatusCode)
}

func (s *Store) RemoveBucket(ctx context.Context, bucket string) error {
	resp, err := s.do(ctx, http.MethodDelete, bucket, "", nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return errorFromResponse(resp, bucket, "")
	}
	drain(resp)
	return nil
}

func (s *Store) ListBuckets(ctx context.Context) ([]string, error) {
	resp, err := s.do(ctx, http.MethodGet, "", "", nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, errorFromResponse(resp, "", "")
	}
	defer resp.Body.Close()
	var res listAllBucketsResult
	if err := xml.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("%w: decode buckets: %v", artifacts.ErrBackendUnavailable, err)
	}
	out := make([]string, 0, len(res.Buckets.Bucket))
	for _, b := range res.Buckets.Bucket {
		out = append(out, b.Name)
	}
	return out, nil
}

// --- Object operations ---

func (s *Store) Upload(ctx context.Context, in sdk.PutInput) (sdk.PutResult, error) {
	if in.Size < 0 {
		return sdk.PutResult{}, fmt.Errorf("%w: negative size", artifacts.ErrInvalidArtifact)
	}
	header := http.Header{}
	if in.ContentType != "" {
		header.Set("Content-Type", in.ContentType)
	}
	for k, v := range in.Metadata {
		header.Set("X-Amz-Meta-"+k, v)
	}
	// Stream the body with an unsigned payload so large objects are not buffered
	// in memory just to hash them (multipart is a future stage).
	resp, err := s.do(ctx, http.MethodPut, in.Bucket, in.Key, nil, in.Body, in.Size, header, unsignedPayload)
	if err != nil {
		return sdk.PutResult{}, fmt.Errorf("%w: %v", artifacts.ErrUploadFailed, err)
	}
	if resp.StatusCode/100 != 2 {
		return sdk.PutResult{}, errorFromResponse(resp, in.Bucket, in.Key)
	}
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	drain(resp)
	return sdk.PutResult{ETag: etag, Size: in.Size, VersionID: resp.Header.Get("x-amz-version-id")}, nil
}

func (s *Store) Download(ctx context.Context, ref sdk.ObjectRef, opts sdk.GetOptions) (*sdk.GetOutput, error) {
	header := http.Header{}
	if opts.Range != nil {
		header.Set("Range", fmt.Sprintf("bytes=%d-%d", opts.Range.Start, opts.Range.End))
	}
	resp, err := s.do(ctx, http.MethodGet, ref.Bucket, ref.Key, nil, nil, 0, header, emptyPayloadHash)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrDownloadFailed, err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, errorFromResponse(resp, ref.Bucket, ref.Key)
	}
	return &sdk.GetOutput{Body: resp.Body, Object: objectFromHeaders(ref, resp.Header)}, nil
}

func objectFromHeaders(ref sdk.ObjectRef, h http.Header) sdk.Object {
	size, _ := strconv.ParseInt(h.Get("Content-Length"), 10, 64)
	lm, _ := http.ParseTime(h.Get("Last-Modified"))
	meta := map[string]string{}
	for k, v := range h {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") && len(v) > 0 {
			meta[strings.ToLower(k)[len("x-amz-meta-"):]] = v[0]
		}
	}
	return sdk.Object{
		Bucket:       ref.Bucket,
		Key:          ref.Key,
		Size:         size,
		ETag:         strings.Trim(h.Get("ETag"), `"`),
		ContentType:  h.Get("Content-Type"),
		LastModified: lm,
		Metadata:     meta,
	}
}

func (s *Store) Stat(ctx context.Context, ref sdk.ObjectRef) (sdk.Object, error) {
	resp, err := s.do(ctx, http.MethodHead, ref.Bucket, ref.Key, nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return sdk.Object{}, err
	}
	drain(resp)
	if resp.StatusCode == http.StatusNotFound {
		return sdk.Object{}, fmt.Errorf("%w: %s/%s", artifacts.ErrObjectNotFound, ref.Bucket, ref.Key)
	}
	if resp.StatusCode/100 != 2 {
		return sdk.Object{}, fmt.Errorf("%w: head status %d", artifacts.ErrBackendUnavailable, resp.StatusCode)
	}
	return objectFromHeaders(ref, resp.Header), nil
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

func (s *Store) Delete(ctx context.Context, ref sdk.ObjectRef) error {
	resp, err := s.do(ctx, http.MethodDelete, ref.Bucket, ref.Key, nil, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return err
	}
	// S3 DELETE is idempotent: 204 whether or not the key existed.
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return errorFromResponse(resp, ref.Bucket, ref.Key)
	}
	drain(resp)
	return nil
}

func (s *Store) List(ctx context.Context, bucket string, opts sdk.ListOptions) (sdk.ListResult, error) {
	q := url.Values{}
	q.Set("list-type", "2")
	if opts.Prefix != "" {
		q.Set("prefix", opts.Prefix)
	}
	if opts.StartAfter != "" {
		q.Set("start-after", opts.StartAfter)
	}
	if opts.MaxKeys > 0 {
		q.Set("max-keys", strconv.Itoa(opts.MaxKeys))
	}
	if !opts.Recursive {
		q.Set("delimiter", "/")
	}
	resp, err := s.do(ctx, http.MethodGet, bucket, "", q, nil, 0, nil, emptyPayloadHash)
	if err != nil {
		return sdk.ListResult{}, err
	}
	if resp.StatusCode/100 != 2 {
		return sdk.ListResult{}, errorFromResponse(resp, bucket, "")
	}
	defer resp.Body.Close()
	var res listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&res); err != nil {
		return sdk.ListResult{}, fmt.Errorf("%w: decode list: %v", artifacts.ErrBackendUnavailable, err)
	}
	out := sdk.ListResult{IsTruncated: res.IsTruncated, NextMarker: res.NextContinuationToken}
	for _, c := range res.Contents {
		lm, _ := time.Parse(time.RFC3339, c.LastModified)
		out.Objects = append(out.Objects, sdk.Object{
			Bucket:       bucket,
			Key:          c.Key,
			Size:         c.Size,
			ETag:         strings.Trim(c.ETag, `"`),
			LastModified: lm,
		})
	}
	return out, nil
}

func (s *Store) Copy(ctx context.Context, src, dst sdk.ObjectRef) error {
	header := http.Header{}
	header.Set("X-Amz-Copy-Source", "/"+src.Bucket+"/"+src.Key)
	resp, err := s.do(ctx, http.MethodPut, dst.Bucket, dst.Key, nil, nil, 0, header, emptyPayloadHash)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return errorFromResponse(resp, dst.Bucket, dst.Key)
	}
	drain(resp)
	return nil
}

func (s *Store) Move(ctx context.Context, src, dst sdk.ObjectRef) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

func (s *Store) GenerateSignedURL(_ context.Context, ref sdk.ObjectRef, opts sdk.SignedURLOptions) (string, error) {
	method := string(opts.Method)
	if method == "" {
		method = http.MethodGet
	}
	expiry := opts.Expiry
	if expiry <= 0 {
		expiry = 15 * time.Minute
	}
	u, host := s.buildURL(ref.Bucket, ref.Key, nil)
	query := s.creds.presign(method, host, u.Path, expiry, s.now())
	return u.String() + "?" + query, nil
}

func (s *Store) Validate(ctx context.Context) error {
	// A ListBuckets is the cheapest authenticated call that proves both
	// connectivity and credential validity.
	_, err := s.ListBuckets(ctx)
	return err
}

func (s *Store) Close() error {
	s.http.CloseIdleConnections()
	return nil
}

var _ sdk.ObjectStore = (*Store)(nil)
