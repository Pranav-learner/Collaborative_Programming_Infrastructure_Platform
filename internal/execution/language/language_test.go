package language

import (
	"testing"
)

func TestRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	l := Language{ID: "go", DisplayName: "Go", Status: StatusStable}
	if err := r.Register(l); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register(l); err != ErrAlreadyRegistered {
		t.Fatalf("dup err = %v, want ErrAlreadyRegistered", err)
	}
	got, err := r.Get("go")
	if err != nil || got.DisplayName != "Go" {
		t.Fatalf("get = %+v err %v", got, err)
	}
	if _, err := r.Get("nope"); err != ErrNotRegistered {
		t.Fatalf("missing err = %v", err)
	}
}

func TestRunnable(t *testing.T) {
	r := NewRegistry()
	r.Upsert(Language{ID: "beta", Status: StatusBeta})
	r.Upsert(Language{ID: "off", Status: StatusDisabled})

	if _, err := r.Runnable("beta"); err != nil {
		t.Fatalf("beta should be runnable: %v", err)
	}
	if _, err := r.Runnable("off"); err == nil {
		t.Fatal("disabled language must not be runnable")
	}
}

func TestDefaultRegistrySeeded(t *testing.T) {
	r := Default()
	if r.Count() < 5 {
		t.Fatalf("default registry too small: %d", r.Count())
	}
	py, err := r.Get("python3")
	if err != nil {
		t.Fatalf("python3 missing: %v", err)
	}
	if py.Profile.MemoryBytes == 0 || py.DefaultTimeout == 0 {
		t.Fatalf("python3 profile incomplete: %+v", py)
	}
	// List is sorted by ID.
	list := r.List()
	for i := 1; i < len(list); i++ {
		if list[i-1].ID > list[i].ID {
			t.Fatal("List not sorted by ID")
		}
	}
}

func TestStatusString(t *testing.T) {
	if StatusStable.String() != "stable" || StatusDisabled.String() != "disabled" {
		t.Error("status strings changed")
	}
}
