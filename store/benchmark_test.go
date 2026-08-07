package store

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func BenchmarkStateWriteFull(b *testing.B) {
	benchmarkStateWrite(b, DurabilityFull)
}

func BenchmarkStateWriteRelaxed(b *testing.B) {
	benchmarkStateWrite(b, DurabilityRelaxed)
}

func benchmarkStateWrite(b *testing.B, durability Durability) {
	dir := benchmarkRealDiskDir(b)
	s, err := OpenSQLite(filepath.Join(dir, "bench.db"), WithDurability(durability))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})

	id := Identity{Type: "benchmark", Key: "state"}
	data := []byte(`{"value":1}`)
	if _, err := s.Write(context.Background(), id, data, 0); err != nil {
		b.Fatal(err)
	}
	etag := ETag(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next, err := s.Write(context.Background(), id, data, etag)
		if err != nil {
			b.Fatal(err)
		}
		etag = next
	}
}

func benchmarkRealDiskDir(b *testing.B) string {
	b.Helper()
	base := os.Getenv("GOR_BENCH_DIR")
	if base == "" {
		var err error
		base, err = os.Getwd()
		if err != nil {
			b.Fatal(err)
		}
	}
	dir, err := os.MkdirTemp(base, ".gor-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error(err)
		}
	})

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		b.Fatal(err)
	}
	const (
		tmpfsSuperMagic = 0x01021994
		ramfsSuperMagic = 0x858458f6
	)
	magic := uint64(stat.Type)
	b.Logf("benchmark data path: statfs magic %#x", magic)
	if magic == tmpfsSuperMagic || magic == ramfsSuperMagic {
		b.Fatalf("benchmark data path %q is on an in-memory filesystem (statfs magic %#x); use real disk storage", dir, magic)
	}
	return dir
}
