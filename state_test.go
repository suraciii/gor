package gor

import (
	"context"
	"errors"
	"testing"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

func newTestBinder(id GrainId, backend store.Store, schedules store.ScheduleStore, sourceClock clock.Clock) *Binder {
	return newBinder(&Runtime{store: backend, scheduleStore: schedules, clock: sourceClock}, id)
}

func TestState_PersistsAllRegisteredValuesAsOneRecord(t *testing.T) {
	backend := store.NewMemory()
	binder := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
	balance := NewState[int64](binder, "balance")
	name := NewState[string](binder, "name")

	if err := balance.Set(context.Background(), 42); err != nil {
		t.Fatalf("balance Set: %v", err)
	}
	if err := name.Set(context.Background(), "Alice"); err != nil {
		t.Fatalf("name Set: %v", err)
	}

	record, err := backend.Read(context.Background(), store.GrainId{GrainType: "account", GrainKey: "alice"})
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

func TestSelf_ReturnsBinderIdentity(t *testing.T) {
	want := GrainId{GrainType: "account", GrainKey: "alice"}
	binder := newTestBinder(want, store.NewMemory(), nil, clock.Real{})

	if got := Self(binder); got != want {
		t.Fatalf("Self = %#v, want %#v", got, want)
	}
}

func TestState_LoadsValuesAndETagFromStore(t *testing.T) {
	backend := store.NewMemory()
	first := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
	firstBalance := NewState[int64](first, "balance")
	if err := firstBalance.Set(context.Background(), 42); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	second := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
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
	record, err := backend.Read(context.Background(), store.GrainId{GrainType: "account", GrainKey: "alice"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if record.ETag != 2 {
		t.Fatalf("stored ETag = %d, want 2", record.ETag)
	}
}

func TestState_ConflictLeavesValueAndMarksBinder(t *testing.T) {
	backend := store.NewMemory()
	first := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
	firstBalance := NewState[int64](first, "balance")
	if err := firstBalance.Set(context.Background(), 1); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	second := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
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

func TestState_WriteErrorLeavesValueAndMarksBinder(t *testing.T) {
	writeErr := errors.New("store unavailable")
	binder := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, failingWriteStore{err: writeErr}, nil, clock.Real{})
	balance := NewState[int64](binder, "balance")
	balance.cell.value = 1

	err := balance.Set(context.Background(), 2)
	if !errors.Is(err, writeErr) {
		t.Fatalf("Set error = %v, want %v", err, writeErr)
	}
	if balance.Get() != 1 {
		t.Fatalf("value after write error = %d, want 1", balance.Get())
	}
	if !errors.Is(binder.discardError(), writeErr) {
		t.Fatalf("discard marker = %v, want %v", binder.discardError(), writeErr)
	}
}

type failingWriteStore struct {
	err error
}

func (failingWriteStore) Read(context.Context, store.GrainId) (store.Record, error) {
	return store.Record{}, nil
}

func (s failingWriteStore) Write(context.Context, store.GrainId, []byte, store.ETag) (store.ETag, error) {
	return 0, s.err
}

func TestNewState_PanicsOnDuplicateName(t *testing.T) {
	binder := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, store.NewMemory(), nil, clock.Real{})
	NewState[int64](binder, "balance")

	defer func() {
		if recover() == nil {
			t.Fatal("duplicate state name did not panic")
		}
	}()
	NewState[string](binder, "balance")
}
