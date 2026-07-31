package gor

import (
	"context"
	"errors"
	"testing"

	"github.com/suraciii/gor/store"
)

func TestState_PersistsAllRegisteredValuesAsOneRecord(t *testing.T) {
	backend := store.NewMemory()
	binder := newBinder(Identity{Type: "account", Key: "alice"}, backend)
	balance := NewState[int64](binder, "balance")
	name := NewState[string](binder, "name")

	if err := balance.Set(context.Background(), 42); err != nil {
		t.Fatalf("balance Set: %v", err)
	}
	if err := name.Set(context.Background(), "Alice"); err != nil {
		t.Fatalf("name Set: %v", err)
	}

	record, err := backend.Read(context.Background(), store.Identity{Type: "account", Key: "alice"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != `{"balance":42,"name":"Alice"}` {
		t.Fatalf("stored data = %s, want one JSON object", record.Data)
	}
	if record.ETag != 2 {
		t.Fatalf("stored ETag = %d, want 2", record.ETag)
	}
}

func TestState_LoadsValuesAndETagFromStore(t *testing.T) {
	backend := store.NewMemory()
	first := newBinder(Identity{Type: "account", Key: "alice"}, backend)
	firstBalance := NewState[int64](first, "balance")
	if err := firstBalance.Set(context.Background(), 42); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	second := newBinder(Identity{Type: "account", Key: "alice"}, backend)
	secondBalance := NewState[int64](second, "balance")
	if err := second.load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	if secondBalance.Get() != 42 {
		t.Fatalf("loaded balance = %d, want 42", secondBalance.Get())
	}

	if err := secondBalance.Set(context.Background(), 43); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	record, err := backend.Read(context.Background(), store.Identity{Type: "account", Key: "alice"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if record.ETag != 2 {
		t.Fatalf("stored ETag = %d, want 2", record.ETag)
	}
}

func TestState_ConflictLeavesValueAndMarksBinder(t *testing.T) {
	backend := store.NewMemory()
	first := newBinder(Identity{Type: "account", Key: "alice"}, backend)
	firstBalance := NewState[int64](first, "balance")
	if err := firstBalance.Set(context.Background(), 1); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	second := newBinder(Identity{Type: "account", Key: "alice"}, backend)
	secondBalance := NewState[int64](second, "balance")
	if err := second.load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := firstBalance.Set(context.Background(), 2); err != nil {
		t.Fatalf("conflicting seed Set: %v", err)
	}

	err := secondBalance.Set(context.Background(), 3)
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("Set error = %v, want store.ErrConflict", err)
	}
	if secondBalance.Get() != 1 {
		t.Fatalf("value after conflict = %d, want 1", secondBalance.Get())
	}
	if !errors.Is(second.discardError(), store.ErrConflict) {
		t.Fatalf("discard marker = %v, want store.ErrConflict", second.discardError())
	}
}

func TestNewState_PanicsOnDuplicateName(t *testing.T) {
	binder := newBinder(Identity{Type: "account", Key: "alice"}, store.NewMemory())
	NewState[int64](binder, "balance")

	defer func() {
		if recover() == nil {
			t.Fatal("duplicate state name did not panic")
		}
	}()
	NewState[string](binder, "balance")
}
