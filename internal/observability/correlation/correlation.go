// Package correlation is the Correlation Manager: it defines the set of
// identifiers that make an operation traceable end-to-end across every CPIP
// subsystem, and the context plumbing that carries them. It is a dependency-free
// leaf (stdlib only) so every framework — logging, tracing, metrics, health —
// can enrich its signals with the same IDs without creating cycles.
//
// The identifier set unifies the platform: a single request flowing through the
// collaboration engine → execution platform → sandbox runtime → persistence
// layer carries one CorrelationID, and each hop stamps its own RequestID /
// ExecutionID / SandboxID / WorkerID, so logs, spans, and metrics can be joined.
//
// Cross-process propagation uses the W3C Trace Context wire format (traceparent)
// plus a compact correlation header, so a future gRPC/HTTP boundary interops with
// standard tooling without this package importing any of it.
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// IDs is the full set of correlation identifiers for one operation. Zero-valued
// fields are simply absent; nothing here is required.
type IDs struct {
	CorrelationID string `json:"correlation_id,omitempty"` // stable across the whole operation
	RequestID     string `json:"request_id,omitempty"`     // one inbound request
	ExecutionID   string `json:"execution_id,omitempty"`   // one code execution/job
	SandboxID     string `json:"sandbox_id,omitempty"`     // one sandbox instance
	WorkerID      string `json:"worker_id,omitempty"`      // one worker processing the job
	SessionID     string `json:"session_id,omitempty"`     // one user/collab session
	NodeID        string `json:"node_id,omitempty"`        // the cluster node handling it
	TraceID       string `json:"trace_id,omitempty"`       // W3C trace id (16 bytes hex)
	SpanID        string `json:"span_id,omitempty"`        // W3C span id (8 bytes hex)
}

// Merge overlays non-empty fields of other onto a copy of ids and returns it.
func (ids IDs) Merge(other IDs) IDs {
	set := func(dst *string, src string) {
		if src != "" {
			*dst = src
		}
	}
	set(&ids.CorrelationID, other.CorrelationID)
	set(&ids.RequestID, other.RequestID)
	set(&ids.ExecutionID, other.ExecutionID)
	set(&ids.SandboxID, other.SandboxID)
	set(&ids.WorkerID, other.WorkerID)
	set(&ids.SessionID, other.SessionID)
	set(&ids.NodeID, other.NodeID)
	set(&ids.TraceID, other.TraceID)
	set(&ids.SpanID, other.SpanID)
	return ids
}

// Fields returns the non-empty IDs as an ordered key/value slice suitable for
// structured logging and span attributes.
func (ids IDs) Fields() [][2]string {
	var out [][2]string
	add := func(k, v string) {
		if v != "" {
			out = append(out, [2]string{k, v})
		}
	}
	add("correlation_id", ids.CorrelationID)
	add("request_id", ids.RequestID)
	add("execution_id", ids.ExecutionID)
	add("sandbox_id", ids.SandboxID)
	add("worker_id", ids.WorkerID)
	add("session_id", ids.SessionID)
	add("node_id", ids.NodeID)
	add("trace_id", ids.TraceID)
	add("span_id", ids.SpanID)
	return out
}

type ctxKey struct{}

// From returns the IDs carried in ctx (zero value if none).
func From(ctx context.Context) IDs {
	if ids, ok := ctx.Value(ctxKey{}).(IDs); ok {
		return ids
	}
	return IDs{}
}

// With returns a context carrying ids (replacing any existing set).
func With(ctx context.Context, ids IDs) context.Context {
	return context.WithValue(ctx, ctxKey{}, ids)
}

// Update merges delta onto the IDs already in ctx and returns the new context —
// the primitive each hop uses to stamp its own identifier while preserving the
// upstream CorrelationID.
func Update(ctx context.Context, delta IDs) context.Context {
	return With(ctx, From(ctx).Merge(delta))
}

// EnsureCorrelationID returns a context guaranteed to carry a CorrelationID,
// generating one if absent. It is the entry-point hop's first call.
func EnsureCorrelationID(ctx context.Context) (context.Context, string) {
	ids := From(ctx)
	if ids.CorrelationID == "" {
		ids.CorrelationID = NewCorrelationID()
		ctx = With(ctx, ids)
	}
	return ctx, ids.CorrelationID
}

// --- ID generation (W3C-compatible) ---

// NewCorrelationID returns a 128-bit random correlation id (32 hex chars).
func NewCorrelationID() string { return randHex(16) }

// NewRequestID returns a 64-bit random request id (16 hex chars).
func NewRequestID() string { return randHex(8) }

// NewTraceID returns a W3C-compatible 16-byte trace id (32 hex chars).
func NewTraceID() string { return randHex(16) }

// NewSpanID returns a W3C-compatible 8-byte span id (16 hex chars).
func NewSpanID() string { return randHex(8) }

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("correlation: system entropy source unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// --- W3C Trace Context propagation ---

const (
	// HeaderTraceParent is the W3C traceparent header name.
	HeaderTraceParent = "traceparent"
	// HeaderCorrelation is a compact correlation header for the non-trace IDs.
	HeaderCorrelation = "x-cpip-correlation"
)

// Inject writes ids into a carrier map (HTTP headers, gRPC metadata) using the
// W3C traceparent format plus a compact correlation header. The carrier is any
// string map; this package never imports a transport.
func Inject(ids IDs, carrier map[string]string) {
	if carrier == nil {
		return
	}
	if ids.TraceID != "" && ids.SpanID != "" {
		// version-traceid-spanid-flags; flags 01 = sampled.
		carrier[HeaderTraceParent] = "00-" + ids.TraceID + "-" + ids.SpanID + "-01"
	}
	parts := make([]string, 0, 8)
	kv := func(k, v string) {
		if v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	kv("cid", ids.CorrelationID)
	kv("rid", ids.RequestID)
	kv("eid", ids.ExecutionID)
	kv("sbx", ids.SandboxID)
	kv("wrk", ids.WorkerID)
	kv("sid", ids.SessionID)
	kv("nid", ids.NodeID)
	if len(parts) > 0 {
		carrier[HeaderCorrelation] = strings.Join(parts, ",")
	}
}

// Extract parses correlation IDs from a carrier map produced by Inject (or a
// standard traceparent from another system).
func Extract(carrier map[string]string) IDs {
	var ids IDs
	if tp := get(carrier, HeaderTraceParent); tp != "" {
		if segs := strings.Split(tp, "-"); len(segs) >= 3 {
			ids.TraceID = segs[1]
			ids.SpanID = segs[2]
		}
	}
	if c := get(carrier, HeaderCorrelation); c != "" {
		for _, p := range strings.Split(c, ",") {
			k, v, ok := strings.Cut(p, "=")
			if !ok {
				continue
			}
			switch k {
			case "cid":
				ids.CorrelationID = v
			case "rid":
				ids.RequestID = v
			case "eid":
				ids.ExecutionID = v
			case "sbx":
				ids.SandboxID = v
			case "wrk":
				ids.WorkerID = v
			case "sid":
				ids.SessionID = v
			case "nid":
				ids.NodeID = v
			}
		}
	}
	return ids
}

// get performs a case-insensitive lookup (HTTP header maps are canonicalized).
func get(carrier map[string]string, key string) string {
	if v, ok := carrier[key]; ok {
		return v
	}
	for k, v := range carrier {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}
