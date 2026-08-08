package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/suraciii/gor/clock"
)

// openStateProbe opens a raw connection to path and keeps it open for the
// test's lifetime. While a probe holds the file open, SQLite does not run its
// implicit last-connection-close checkpoint, so the -wal file keeps whatever
// the store's own Close left in it.
func openStateProbe(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		t.Fatalf("open probe %q: %v", path, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping probe %q: %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
}

func walSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("stat %s-wal: %v", path, err)
	}
	return fi.Size()
}

func TestOpenSQLite_DefaultDurabilityIsFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	if _, err := s.Write(context.Background(), GrainId{GrainType: "account", GrainKey: "alice"}, []byte("x"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	openStateProbe(t, stateFilePath(path))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Full does not flush on close: the state WAL must still hold the
	// committed frame.
	if size := walSize(t, stateFilePath(path)); size == 0 {
		t.Fatalf("state -wal size = 0 after Close at default durability, want committed frames")
	}
}

func TestOpenSQLiteRelaxed_WritesPersistAcrossCloseAndFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	s, err := OpenSQLite(path, WithDurability(DurabilityRelaxed))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	first, err := s.Write(context.Background(), id, []byte("one"), 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if first != 1 {
		t.Fatalf("ETag = %d, want 1", first)
	}
	second, err := s.Write(context.Background(), id, []byte("two"), first)
	if err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if second != 2 {
		t.Fatalf("ETag = %d, want 2", second)
	}
	if _, err := s.Write(context.Background(), id, []byte("stale"), first); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Write error = %v, want ErrConflict", err)
	}
	if err := s.Put(context.Background(), Reminder{
		GrainId: GrainId{GrainType: "account", GrainKey: "alice"},
		Name:    "tick",
		Method:  "Tick",
		DueAt:   time.Unix(0, 1).UTC(),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	openStateProbe(t, stateFilePath(path))
	openStateProbe(t, path)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Relaxed flushes the state database on close, but leaves the
	// coordination database's WAL untouched.
	if size := walSize(t, stateFilePath(path)); size != 0 {
		t.Fatalf("state -wal size = %d after Relaxed Close, want 0 (flushed)", size)
	}
	if size := walSize(t, path); size == 0 {
		t.Fatalf("main -wal size = 0 after Relaxed Close, want untouched coordination WAL")
	}

	reopened, err := OpenSQLite(path, WithDurability(DurabilityRelaxed))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	record, err := reopened.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if string(record.Data) != "two" || record.ETag != 2 {
		t.Fatalf("Record after reopen = %#v, want data two and ETag 2", record)
	}
	third, err := reopened.Write(context.Background(), id, []byte("three"), record.ETag)
	if err != nil {
		t.Fatalf("Write after reopen: %v", err)
	}
	if third != 3 {
		t.Fatalf("ETag = %d, want 3 (counter continues)", third)
	}
}

func TestOpenSQLiteWithClock_AcceptsDurabilityOption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")

	s, err := OpenSQLiteWithClock(path, clock.NewFake(time.Unix(0, 0).UTC()), WithDurability(DurabilityRelaxed))
	if err != nil {
		t.Fatalf("OpenSQLiteWithClock: %v", err)
	}
	if _, err := s.Write(context.Background(), GrainId{GrainType: "account", GrainKey: "alice"}, []byte("x"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	openStateProbe(t, stateFilePath(path))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if size := walSize(t, stateFilePath(path)); size != 0 {
		t.Fatalf("state -wal size = %d after Relaxed Close, want 0 (flushed)", size)
	}
}

func TestSQLiteDSN_SyncLevelPerTier(t *testing.T) {
	if dsn := sqliteDSN("x.db", DurabilityFull); !strings.Contains(dsn, "synchronous(FULL)") {
		t.Fatalf("Full DSN %q missing synchronous(FULL)", dsn)
	}
	if dsn := sqliteDSN("x.db", DurabilityRelaxed); !strings.Contains(dsn, "synchronous(NORMAL)") {
		t.Fatalf("Relaxed DSN %q missing synchronous(NORMAL)", dsn)
	}

	// A connection opened with the store's own DSN construction must report
	// the tier's sync level.
	for _, c := range []struct {
		durability Durability
		want       int
	}{
		{DurabilityFull, 2},
		{DurabilityRelaxed, 1},
	} {
		db, err := sql.Open("sqlite", sqliteDSN(filepath.Join(t.TempDir(), "s.db"), c.durability))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		var sync int
		if err := db.QueryRow(`PRAGMA synchronous`).Scan(&sync); err != nil {
			t.Fatalf("PRAGMA synchronous: %v", err)
		}
		db.Close()
		if sync != c.want {
			t.Fatalf("synchronous = %d for durability %d, want %d", sync, c.durability, c.want)
		}
	}
}

func TestStateFilePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"data/gor.db", "data/gor-state.db"},
		{"gor.db", "gor-state.db"},
		{"gor", "gor-state"},
		{"dir/foo.bar.db", "dir/foo.bar-state.db"},
	}
	for _, c := range cases {
		if got := stateFilePath(c.in); got != c.want {
			t.Errorf("stateFilePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestOpenSQLite_UnknownDurabilityValueRejected(t *testing.T) {
	_, err := OpenSQLite(filepath.Join(t.TempDir(), "gor.db"), WithDurability(Durability(7)))
	if err == nil {
		t.Fatalf("OpenSQLite: expected error for unknown durability value")
	}
}
