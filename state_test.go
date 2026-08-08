package gor

import (
	"context"
	"errors"
	"path/filepath"
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
	err    error
	record store.Record
}

func (f failingWriteStore) Read(context.Context, store.GrainId) (store.Record, error) {
	return f.record, nil
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

func TestState_NewValueIsAbsentAndNotPersisted(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, backend store.Store) {
		binder := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
		absent := NewState[int](binder, "absent")
		other := NewState[int](binder, "other")

		if absent.Exists() {
			t.Fatal("new State Exists = true, want false")
		}
		if absent.Get() != 0 {
			t.Fatalf("new State Get = %d, want zero", absent.Get())
		}
		if err := other.Set(context.Background(), 7); err != nil {
			t.Fatalf("other Set: %v", err)
		}

		record, err := backend.Read(context.Background(), binder.identity)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(record.Data) != `{"other":7}` {
			t.Fatalf("stored data = %s, want only the present named value", record.Data)
		}
	})
}

func TestState_PresentZeroValueIsDistinctFromAbsent(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, backend store.Store) {
		id := GrainId{GrainType: "account", GrainKey: "alice"}
		binder := newTestBinder(id, backend, nil, clock.Real{})
		value := NewState[int](binder, "value")

		if err := value.Set(context.Background(), 0); err != nil {
			t.Fatalf("Set zero: %v", err)
		}
		if !value.Exists() {
			t.Fatal("present zero State Exists = false, want true")
		}
		if value.Get() != 0 {
			t.Fatalf("present zero State Get = %d, want zero", value.Get())
		}

		restarted := newTestBinder(id, backend, nil, clock.Real{})
		restartedValue := NewState[int](restarted, "value")
		if err := restarted.load(context.Background()); err != nil {
			t.Fatalf("load: %v", err)
		}
		if !restartedValue.Exists() || restartedValue.Get() != 0 {
			t.Fatalf("restarted zero State = (exists=%v, value=%d), want (true, 0)", restartedValue.Exists(), restartedValue.Get())
		}
	})
}

func TestState_ClearKeepsEmptyRecordAndAbsenceAfterRestart(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, backend store.Store) {
		id := GrainId{GrainType: "account", GrainKey: "alice"}
		binder := newTestBinder(id, backend, nil, clock.Real{})
		value := NewState[int](binder, "value")
		if err := value.Set(context.Background(), 9); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := value.Clear(context.Background()); err != nil {
			t.Fatalf("Clear: %v", err)
		}
		if value.Exists() {
			t.Fatal("cleared State Exists = true, want false")
		}
		if value.Get() != 0 {
			t.Fatalf("cleared State Get = %d, want zero", value.Get())
		}

		record, err := backend.Read(context.Background(), store.GrainId(id))
		if err != nil {
			t.Fatalf("Read after Clear: %v", err)
		}
		if string(record.Data) != `{}` || record.ETag != 2 {
			t.Fatalf("record after Clear = %#v, want empty record with ETag 2", record)
		}

		restarted := newTestBinder(id, backend, nil, clock.Real{})
		restartedValue := NewState[int](restarted, "value")
		if err := restarted.load(context.Background()); err != nil {
			t.Fatalf("restart load: %v", err)
		}
		if restartedValue.Exists() || restartedValue.Get() != 0 {
			t.Fatalf("restarted cleared State = (exists=%v, value=%d), want (false, 0)", restartedValue.Exists(), restartedValue.Get())
		}
	})
}

func TestState_ClearPreservesOtherPresentValues(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, backend store.Store) {
		binder := newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, nil, clock.Real{})
		first := NewState[int](binder, "first")
		second := NewState[string](binder, "second")
		if err := first.Set(context.Background(), 1); err != nil {
			t.Fatalf("first Set: %v", err)
		}
		if err := second.Set(context.Background(), "two"); err != nil {
			t.Fatalf("second Set: %v", err)
		}
		if err := first.Clear(context.Background()); err != nil {
			t.Fatalf("first Clear: %v", err)
		}
		if err := second.Set(context.Background(), "updated"); err != nil {
			t.Fatalf("second update: %v", err)
		}
		if first.Exists() {
			t.Fatal("cleared first State Exists = true, want false")
		}
		if !second.Exists() || second.Get() != "updated" {
			t.Fatalf("second State = (exists=%v, value=%q), want (true, updated)", second.Exists(), second.Get())
		}

		record, err := backend.Read(context.Background(), binder.identity)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(record.Data) != `{"second":"updated"}` {
			t.Fatalf("stored data after preserving other value = %s, want only second", record.Data)
		}
	})
}

func TestState_ClearConflictLeavesPresenceAndValueUnchanged(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, backend store.Store) {
		id := GrainId{GrainType: "account", GrainKey: "alice"}
		first := newTestBinder(id, backend, nil, clock.Real{})
		firstValue := NewState[int](first, "value")
		if err := firstValue.Set(context.Background(), 1); err != nil {
			t.Fatalf("first Set: %v", err)
		}

		second := newTestBinder(id, backend, nil, clock.Real{})
		secondValue := NewState[int](second, "value")
		if err := second.load(context.Background()); err != nil {
			t.Fatalf("second load: %v", err)
		}
		record, err := backend.Read(context.Background(), store.GrainId(id))
		if err != nil {
			t.Fatalf("Read before external update: %v", err)
		}
		if _, err := backend.Write(context.Background(), store.GrainId(id), []byte(`{"value":2}`), record.ETag); err != nil {
			t.Fatalf("external Write: %v", err)
		}

		err = secondValue.Clear(context.Background())
		if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("Clear error = %v, want store.ErrConflict", err)
		}
		if !secondValue.Exists() || secondValue.Get() != 1 {
			t.Fatalf("State after conflict = (exists=%v, value=%d), want (true, 1)", secondValue.Exists(), secondValue.Get())
		}
		if !errors.Is(second.discardError(), store.ErrConflict) {
			t.Fatalf("discard marker = %v, want store.ErrConflict", second.discardError())
		}
	})
}

func TestState_ClearStoreFailureLeavesPresenceAndValueUnchanged(t *testing.T) {
	writeErr := errors.New("store unavailable")
	binder := newTestBinder(
		GrainId{GrainType: "account", GrainKey: "alice"},
		failingWriteStore{err: writeErr, record: store.Record{Data: []byte(`{"value":1}`), ETag: 1}},
		nil,
		clock.Real{},
	)
	value := NewState[int](binder, "value")
	if err := binder.load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}

	err := value.Clear(context.Background())
	if !errors.Is(err, writeErr) {
		t.Fatalf("Clear error = %v, want %v", err, writeErr)
	}
	if !value.Exists() || value.Get() != 1 {
		t.Fatalf("State after store failure = (exists=%v, value=%d), want (true, 1)", value.Exists(), value.Get())
	}
	if !errors.Is(binder.discardError(), writeErr) {
		t.Fatalf("discard marker = %v, want %v", binder.discardError(), writeErr)
	}
}

func forEachStateBackend(t *testing.T, test func(*testing.T, store.Store)) {
	t.Helper()
	for _, backend := range []struct {
		name string
		open func(*testing.T) store.Store
	}{
		{name: "memory", open: func(*testing.T) store.Store { return store.NewMemory() }},
		{name: "sqlite", open: func(t *testing.T) store.Store {
			sqliteStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatalf("OpenSQLite: %v", err)
			}
			t.Cleanup(func() {
				if err := sqliteStore.Close(); err != nil {
					t.Errorf("Close SQLite: %v", err)
				}
			})
			return sqliteStore
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			test(t, backend.open(t))
		})
	}
}
