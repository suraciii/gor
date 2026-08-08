package domain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestApplicationStore_SafeRepeatAndActionIDConflict(t *testing.T) {
	stores := []ApplicationStore{NewMemoryApplicationStore()}
	business, err := OpenSQLiteApplicationStore(filepath.Join(t.TempDir(), "business.db"))
	if err != nil {
		t.Fatal(err)
	}
	stores = append(stores, business)
	for _, application := range stores {
		t.Run(storeName(application), func(t *testing.T) {
			t.Cleanup(func() { _ = application.Close() })
			ctx := context.Background()
			first := PendingAction{ActionID: "b", DeviceKey: "device-1", State: "two", TraceID: "trace-b"}
			second := PendingAction{ActionID: "a", DeviceKey: "device-1", State: "one", TraceID: "trace-a"}
			if err := application.SavePending(ctx, first); err != nil {
				t.Fatal(err)
			}
			if err := application.SavePending(ctx, first); err != nil {
				t.Fatalf("identical SavePending: %v", err)
			}
			if err := application.SavePending(ctx, second); err != nil {
				t.Fatal(err)
			}
			if err := application.SavePending(ctx, PendingAction{ActionID: "a", DeviceKey: "device-1", State: "changed"}); !errors.Is(err, ErrPendingActionConflict) {
				t.Fatalf("conflicting SavePending = %v, want conflict", err)
			}
			pending, err := application.ListPending(ctx)
			if err != nil || len(pending) != 2 || pending[0].ActionID != "a" || pending[1].ActionID != "b" {
				t.Fatalf("ListPending = (%#v, %v), want ActionID order", pending, err)
			}
			if err := application.ApplyPending(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			if err := application.ApplyPending(ctx, "a"); err != nil {
				t.Fatalf("Safe Repeat ApplyPending: %v", err)
			}
			record, ok, err := application.ReadApplied(ctx, "a")
			if err != nil || !ok || record.State != "one" || record.TraceID != "trace-a" {
				t.Fatalf("ReadApplied = (%#v, %v, %v), want one receipt", record, ok, err)
			}
		})
	}
}

func TestSQLiteApplicationStore_PersistsPendingAndAppliedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "business.db")
	first, err := OpenSQLiteApplicationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SavePending(context.Background(), PendingAction{ActionID: "persisted", DeviceKey: "device-1", State: "value", TraceID: "trace"}); err != nil {
		first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenSQLiteApplicationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ApplyPending(context.Background(), "persisted"); err != nil {
		second.Close()
		t.Fatal(err)
	}
	defer second.Close()
	record, ok, err := second.ReadApplied(context.Background(), "persisted")
	if err != nil || !ok || record.ActionID != "persisted" {
		t.Fatalf("ReadApplied after reopen = (%#v, %v, %v), want persisted receipt", record, ok, err)
	}
}

func TestSQLiteApplicationStore_PathWithURICharactersUsesRequestedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "business?#.db")
	first, err := OpenSQLiteApplicationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SavePending(context.Background(), PendingAction{ActionID: "uri", DeviceKey: "device-1", State: "value", TraceID: "trace"}); err != nil {
		first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("requested Application database %q: %v", path, err)
	}

	second, err := OpenSQLiteApplicationStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	pending, err := second.ListPending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ActionID != "uri" {
		t.Fatalf("pending after reopen = %#v, want URI-path record", pending)
	}
}

func storeName(application ApplicationStore) string {
	if _, ok := application.(*MemoryApplicationStore); ok {
		return "memory"
	}
	return "sqlite"
}
