// Package s3 implements the Storage SDK against any S3-compatible object store
// (AWS S3, MinIO, GCS interop, Ceph, SeaweedFS, …) using ONLY the Go standard
// library. It speaks the S3 REST protocol directly over net/http and signs
// requests with AWS Signature Version 4 implemented from scratch (crypto/hmac +
// crypto/sha256). This is the strongest form of decoupling: no vendor SDK is
// imported, so the platform depends on the open S3 wire protocol rather than any
// one vendor's client library.
package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	algorithm       = "AWS4-HMAC-SHA256"
	defaultService  = "s3"
	terminator      = "aws4_request"
	amzDateFormat   = "20060102T150405Z"
	dateStampFormat = "20060102"
	// emptyPayloadHash is sha256("") — the payload hash for body-less requests.
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// unsignedPayload lets large/streaming bodies be sent without buffering to
	// hash them (standard S3 practice; the transport still protects integrity).
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// credentials holds the SigV4 signing identity.
type credentials struct {
	accessKey string
	secretKey string
	region    string
	service   string // "s3" in production; overridable for AWS test-vector verification
}

func (c credentials) svc() string {
	if c.service == "" {
		return defaultService
	}
	return c.service
}

// hmacSHA256 returns HMAC-SHA256(key, data).
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// signingKey derives the per-day, per-region signing key.
func (c credentials) signingKey(dateStamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+c.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, c.region)
	kService := hmacSHA256(kRegion, c.svc())
	return hmacSHA256(kService, terminator)
}

func (c credentials) scope(dateStamp string) string {
	return strings.Join([]string{dateStamp, c.region, c.svc(), terminator}, "/")
}

// uriEncode implements AWS's RFC3986 encoding. When encodeSlash is false, '/'
// is preserved (used for object key paths); otherwise everything outside the
// unreserved set is percent-encoded (used for query components).
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte('/')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalHeaders selects the headers that participate in the signature (host,
// content-type, content-md5, and all x-amz-*) and returns the canonical header
// block plus the semicolon-joined signed-header list.
func canonicalHeaders(req *http.Request) (canonical, signed string) {
	type kv struct{ k, v string }
	var hs []kv
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	hs = append(hs, kv{"host", host})
	for name, vals := range req.Header {
		lower := strings.ToLower(name)
		if lower == "content-type" || lower == "content-md5" || strings.HasPrefix(lower, "x-amz-") {
			hs = append(hs, kv{lower, strings.TrimSpace(strings.Join(vals, ","))})
		}
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].k < hs[j].k })
	var cb, sb strings.Builder
	for i, h := range hs {
		cb.WriteString(h.k)
		cb.WriteByte(':')
		cb.WriteString(h.v)
		cb.WriteByte('\n')
		if i > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(h.k)
	}
	return cb.String(), sb.String()
}

// canonicalQuery returns the sorted, encoded canonical query string.
func canonicalQuery(query map[string][]string) string {
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), query[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	return strings.Join(parts, "&")
}

// sign adds SigV4 authentication headers to req. payloadHash is the hex SHA-256
// of the body, or unsignedPayload for streamed bodies.
func (c credentials) sign(req *http.Request, payloadHash string, now time.Time) {
	amzDate := now.UTC().Format(amzDateFormat)
	dateStamp := now.UTC().Format(dateStampFormat)

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	canonHeaders, signedHeaders := canonicalHeaders(req)
	canonicalURI := uriEncode(req.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery(req.URL.Query()),
		canonHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		c.scope(dateStamp),
		sha256Hex(canonicalRequest),
	}, "\n")

	signature := hex.EncodeToString(hmacSHA256(c.signingKey(dateStamp), stringToSign))
	auth := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, c.accessKey, c.scope(dateStamp), signedHeaders, signature)
	req.Header.Set("Authorization", auth)
}

// presign builds a presigned URL query string (SigV4 in the query, not headers)
// for the given method, host, path, and expiry. Only the host header is signed,
// and the payload is unsigned — the standard presign profile.
func (c credentials) presign(method, host, path string, expiry time.Duration, now time.Time) string {
	amzDate := now.UTC().Format(amzDateFormat)
	dateStamp := now.UTC().Format(dateStampFormat)
	credential := c.accessKey + "/" + c.scope(dateStamp)

	q := map[string][]string{
		"X-Amz-Algorithm":     {algorithm},
		"X-Amz-Credential":    {credential},
		"X-Amz-Date":          {amzDate},
		"X-Amz-Expires":       {fmt.Sprintf("%d", int(expiry.Seconds()))},
		"X-Amz-SignedHeaders": {"host"},
	}
	canonicalQueryStr := canonicalQuery(q)
	canonicalURI := uriEncode(path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalHeaders := "host:" + host + "\n"
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryStr,
		canonicalHeaders,
		"host",
		unsignedPayload,
	}, "\n")
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		c.scope(dateStamp),
		sha256Hex(canonicalRequest),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(c.signingKey(dateStamp), stringToSign))
	return canonicalQueryStr + "&X-Amz-Signature=" + signature
}
