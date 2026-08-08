package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSQLiteStore_WriteWithMatchingETagReturnsNewETag(t *testing.T) {
	store := newSQLiteTestStore(t)
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	etag, err := store.Write(context.Background(), id, []byte("first"), 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if etag != 1 {
		t.Fatalf("ETag = %d, want 1", etag)
	}

	record, err := store.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != "first" || record.ETag != etag {
		t.Fatalf("Record = %#v, want data first and ETag %d", record, etag)
	}

	nextETag, err := store.Write(context.Background(), id, []byte("second"), etag)
	if err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if nextETag != 2 {
		t.Fatalf("second ETag = %d, want 2", nextETag)
	}
}

func TestSQLiteStore_ConflictLeavesRecordUnchanged(t *testing.T) {
	store := newSQLiteTestStore(t)
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	etag, err := store.Write(context.Background(), id, []byte("original"), 0)
	if err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	newETag, err := store.Write(context.Background(), id, []byte("replacement"), etag+1)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
	if newETag != 0 {
		t.Fatalf("conflicting ETag = %d, want 0", newETag)
	}

	record, err := store.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != "original" || record.ETag != etag {
		t.Fatalf("Record after conflict = %#v, want original data and ETag %d", record, etag)
	}
}

func TestSQLiteStore_ZeroETagConflictsWithExistingRecord(t *testing.T) {
	store := newSQLiteTestStore(t)
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	if _, err := store.Write(context.Background(), id, []byte("existing"), 0); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	if _, err := store.Write(context.Background(), id, []byte("overwrite"), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
}

func TestSQLiteStore_NonzeroETagConflictsWithMissingRecord(t *testing.T) {
	store := newSQLiteTestStore(t)
	id := GrainId{GrainType: "account", GrainKey: "missing"}

	if _, err := store.Write(context.Background(), id, []byte("unexpected"), 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
}

func TestSQLiteStore_ReadMissingReturnsZeroRecord(t *testing.T) {
	store := newSQLiteTestStore(t)

	record, err := store.Read(context.Background(), GrainId{GrainType: "account", GrainKey: "missing"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if record.Data != nil || record.ETag != 0 {
		t.Fatalf("Record = %#v, want zero Record", record)
	}
}

func TestSQLiteStore_DifferentIdentitiesAreIndependent(t *testing.T) {
	store := newSQLiteTestStore(t)
	alice := GrainId{GrainType: "account", GrainKey: "alice"}
	bob := GrainId{GrainType: "account", GrainKey: "bob"}

	if _, err := store.Write(context.Background(), alice, []byte("alice"), 0); err != nil {
		t.Fatalf("alice Write: %v", err)
	}
	if _, err := store.Write(context.Background(), bob, []byte("bob"), 0); err != nil {
		t.Fatalf("bob Write: %v", err)
	}

	aliceRecord, err := store.Read(context.Background(), alice)
	if err != nil {
		t.Fatalf("alice Read: %v", err)
	}
	bobRecord, err := store.Read(context.Background(), bob)
	if err != nil {
		t.Fatalf("bob Read: %v", err)
	}
	if string(aliceRecord.Data) != "alice" || aliceRecord.ETag != 1 {
		t.Fatalf("alice Record = %#v", aliceRecord)
	}
	if string(bobRecord.Data) != "bob" || bobRecord.ETag != 1 {
		t.Fatalf("bob Record = %#v", bobRecord)
	}
}

func TestSQLiteStore_ReadAndWriteCopyData(t *testing.T) {
	store := newSQLiteTestStore(t)
	id := GrainId{GrainType: "account", GrainKey: "alice"}
	data := []byte("original")

	if _, err := store.Write(context.Background(), id, data, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	data[0] = 'X'

	record, err := store.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	record.Data[0] = 'Y'

	unchanged, err := store.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if string(unchanged.Data) != "original" {
		t.Fatalf("stored data = %q, want original", unchanged.Data)
	}
}

func TestSQLiteStore_PersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	first, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite first: %v", err)
	}
	if _, err := first.Write(context.Background(), id, []byte("persisted"), 0); err != nil {
		first.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite second: %v", err)
	}
	defer second.Close()
	record, err := second.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if string(record.Data) != "persisted" || record.ETag != 1 {
		t.Fatalf("Record after reopen = %#v, want persisted data and ETag 1", record)
	}
}

func TestOpenSQLite_PathWithURICharactersUsesRequestedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store?#.db")
	id := GrainId{GrainType: "account", GrainKey: "uri"}

	first, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite first: %v", err)
	}
	if _, err := first.Write(context.Background(), id, []byte("uri"), 0); err != nil {
		first.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("requested coordination database %q: %v", path, err)
	}
	if _, err := os.Stat(stateFilePath(path)); err != nil {
		t.Fatalf("requested State database %q: %v", stateFilePath(path), err)
	}

	second, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite second: %v", err)
	}
	defer second.Close()
	record, err := second.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if string(record.Data) != "uri" {
		t.Fatalf("Record after reopen = %#v, want uri", record)
	}
}

func TestOpenSQLite_MissingParentDirErrorNamesPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no", "such", "dir", "gor.db")

	_, err := OpenSQLite(path)
	if err == nil {
		t.Fatalf("OpenSQLite: expected error for missing parent dir")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("OpenSQLite error %q does not mention path %q", err, path)
	}
}

func newSQLiteTestStore(t *testing.T) *SQLite {
	t.Helper()
	store, err := OpenSQLite(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}
