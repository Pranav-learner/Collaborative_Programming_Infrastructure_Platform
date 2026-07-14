package correlation

import (
	"context"
	"testing"
)

func TestEnsureAndFrom(t *testing.T) {
	ctx := context.Background()
	ctx, cid := EnsureCorrelationID(ctx)
	if cid == "" {
		t.Fatal("expected a correlation id")
	}
	if From(ctx).CorrelationID != cid {
		t.Fatal("correlation id not stored in context")
	}
	// Idempotent: a second Ensure keeps the same id.
	_, cid2 := EnsureCorrelationID(ctx)
	if cid2 != cid {
		t.Fatalf("ensure changed the id: %s != %s", cid2, cid)
	}
}

func TestUpdateMergesWithoutClobbering(t *testing.T) {
	ctx := With(context.Background(), IDs{CorrelationID: "c1", RequestID: "r1"})
	ctx = Update(ctx, IDs{ExecutionID: "e1"})
	ids := From(ctx)
	if ids.CorrelationID != "c1" || ids.RequestID != "r1" || ids.ExecutionID != "e1" {
		t.Fatalf("merge lost fields: %+v", ids)
	}
	// Update overrides only provided fields.
	ctx = Update(ctx, IDs{RequestID: "r2"})
	if From(ctx).RequestID != "r2" || From(ctx).CorrelationID != "c1" {
		t.Fatalf("override wrong: %+v", From(ctx))
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	ids := IDs{
		CorrelationID: "cid", RequestID: "rid", ExecutionID: "eid", SandboxID: "sbx",
		WorkerID: "wrk", SessionID: "sid", NodeID: "nid",
		TraceID: "0af7651916cd43dd8448eb211c80319c", SpanID: "b7ad6b7169203331",
	}
	carrier := map[string]string{}
	Inject(ids, carrier)
	if carrier[HeaderTraceParent] != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Fatalf("bad traceparent: %q", carrier[HeaderTraceParent])
	}
	got := Extract(carrier)
	if got != ids {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, ids)
	}
}

func TestExtractStandardTraceParent(t *testing.T) {
	// A traceparent from an external system (no correlation header).
	carrier := map[string]string{"Traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	got := Extract(carrier)
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || got.SpanID != "00f067aa0ba902b7" {
		t.Fatalf("failed to parse external traceparent: %+v", got)
	}
}

func TestIDFormats(t *testing.T) {
	if len(NewTraceID()) != 32 {
		t.Fatal("trace id must be 32 hex chars")
	}
	if len(NewSpanID()) != 16 {
		t.Fatal("span id must be 16 hex chars")
	}
}
