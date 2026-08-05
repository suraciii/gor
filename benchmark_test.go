package gor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type benchmarkEntity interface {
	Noop(context.Context) error
	Seed(context.Context) error
}

type benchmarkEntityImpl struct {
	state State[uint64]
}

type benchmarkEntityProxy struct {
	invoker Invoker
	id      Identity
}

func (e *benchmarkEntityImpl) Noop(context.Context) error {
	return nil
}

func (e *benchmarkEntityImpl) Seed(ctx context.Context) error {
	return e.state.Set(ctx, 1)
}

func (p *benchmarkEntityProxy) Noop(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Noop", nil, nil)
}

func (p *benchmarkEntityProxy) Seed(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Seed", nil, nil)
}

func dispatchBenchmarkEntity(ctx context.Context, instance benchmarkEntity, method string, _ []any, _ any) error {
	switch method {
	case "Noop":
		return instance.Noop(ctx)
	case "Seed":
		return instance.Seed(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newBenchmarkRuntime(b *testing.B, backend store.Store, sourceClock clock.Clock, idleTimeout, evictionInterval time.Duration) *Runtime {
	b.Helper()
	rt, err := New(
		WithStore(backend),
		WithClock(sourceClock),
		WithIdleTimeout(idleTimeout),
		WithEvictionInterval(evictionInterval),
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := InstallType[benchmarkEntity](rt, dispatchBenchmarkEntity, func(invoker Invoker, id Identity) benchmarkEntity {
		return &benchmarkEntityProxy{invoker: invoker, id: id}
	}); err != nil {
		rt.Close()
		b.Fatal(err)
	}
	if err := Register[benchmarkEntity](rt, func(binder *Binder) benchmarkEntity {
		return &benchmarkEntityImpl{state: NewState[uint64](binder, "value")}
	}); err != nil {
		rt.Close()
		b.Fatal(err)
	}
	b.Cleanup(rt.Close)
	return rt
}

func BenchmarkInvocationRoundTrip(b *testing.B) {
	rt := newBenchmarkRuntime(b, store.NewMemory(), clock.Real{}, 0, 0)
	entity := Ref[benchmarkEntity](rt, "benchmark")
	if err := entity.Noop(context.Background()); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := entity.Noop(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateWrite(b *testing.B) {
	database := openBenchmarkSQLite(b, "state.db")
	rt, err := New(WithStore(database), WithClock(clock.Real{}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { rt.Close() })

	binder := newBinder(rt, Identity{Type: "benchmark", Key: "state"})
	state := NewState[uint64](binder, "value")
	if err := state.Set(context.Background(), 0); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := state.Set(context.Background(), uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColdActivation(b *testing.B) {
	database := openBenchmarkSQLite(b, "cold-activation.db")
	fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
	rt := newBenchmarkRuntime(b, database, fakeClock, time.Second, time.Second)
	entity := Ref[benchmarkEntity](rt, "benchmark")
	if err := entity.Seed(context.Background()); err != nil {
		b.Fatal(err)
	}

	fakeClock.Advance(2 * time.Second)
	waitForBenchmarkEviction(b, rt)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := entity.Noop(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if i+1 < b.N {
			fakeClock.Advance(2 * time.Second)
			waitForBenchmarkEviction(b, rt)
			b.StartTimer()
		}
	}
}

func openBenchmarkSQLite(b *testing.B, filename string) *store.SQLite {
	b.Helper()
	dir := benchmarkRealDiskDir(b)
	database, err := store.OpenSQLite(filepath.Join(dir, filename))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Error(err)
		}
	})
	return database
}

func waitForBenchmarkEviction(b *testing.B, rt *Runtime) {
	b.Helper()
	for len(rt.Identities()) != 0 {
		runtime.Gosched()
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
