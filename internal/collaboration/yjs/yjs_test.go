package yjs

import (
	"sync/atomic"
	"testing"
)

func TestInsertDeleteText(t *testing.T) {
	d := New(Options{GC: true})
	defer d.Destroy()

	d.InsertText(0, "Hello World")
	d.DeleteText(5, 6) // remove " World"
	if got := d.GetText(); got != "Hello" {
		t.Fatalf("text = %q, want Hello", got)
	}
}

func TestEncodeApplyRoundTrip(t *testing.T) {
	src := New(Options{GC: true})
	defer src.Destroy()
	src.InsertText(0, "shared state")

	dst := New(Options{GC: true})
	defer dst.Destroy()

	// Full state transfer.
	if err := dst.ApplyUpdate(src.EncodeStateAsUpdate(nil)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if dst.GetText() != "shared state" {
		t.Fatalf("dst = %q", dst.GetText())
	}

	// Incremental transfer after a further edit.
	dstSV, err := DecodeStateVector(dst.EncodeStateVector())
	if err != nil {
		t.Fatalf("decode sv: %v", err)
	}
	src.InsertText(12, "!")
	delta := src.EncodeStateAsUpdate(dstSV)
	if err := dst.ApplyUpdate(delta); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	if dst.GetText() != "shared state!" {
		t.Fatalf("dst after delta = %q", dst.GetText())
	}
}

func TestMultiFile(t *testing.T) {
	d := New(Options{GC: true})
	defer d.Destroy()

	d.InsertTextIn("main.go", 0, "package main")
	d.InsertTextIn("util.go", 0, "package util")

	if d.GetTextIn("main.go") != "package main" || d.GetTextIn("util.go") != "package util" {
		t.Fatalf("multi-file content wrong: %q / %q", d.GetTextIn("main.go"), d.GetTextIn("util.go"))
	}
	if len(d.Files()) != 3 { // content (default) + main.go + util.go
		t.Fatalf("files = %v, want 3", d.Files())
	}
}

func TestUpdateHandlerFires(t *testing.T) {
	d := New(Options{GC: true})
	defer d.Destroy()

	var count int64
	d.SetUpdateHandler(func(_ []byte, _ any) { atomic.AddInt64(&count, 1) })
	d.InsertText(0, "a")
	d.InsertText(1, "b")
	if atomic.LoadInt64(&count) != 2 {
		t.Fatalf("handler fired %d times, want 2", count)
	}
}

func TestConvergenceIsCommutative(t *testing.T) {
	// Two peers edit concurrently, exchange full state, and must converge.
	a := New(Options{GC: true})
	b := New(Options{GC: true})
	defer a.Destroy()
	defer b.Destroy()

	a.InsertText(0, "AAA")
	b.InsertText(0, "BBB")

	ua := a.EncodeStateAsUpdate(nil)
	ub := b.EncodeStateAsUpdate(nil)
	if err := a.ApplyUpdate(ub); err != nil {
		t.Fatalf("a apply b: %v", err)
	}
	if err := b.ApplyUpdate(ua); err != nil {
		t.Fatalf("b apply a: %v", err)
	}
	if a.GetText() != b.GetText() {
		t.Fatalf("did not converge: %q vs %q", a.GetText(), b.GetText())
	}
	if len(a.GetText()) != 6 {
		t.Fatalf("converged text length = %d, want 6", len(a.GetText()))
	}
}
