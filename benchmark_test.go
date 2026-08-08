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
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
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
	id      GrainId
}

type benchmarkNoopRequest struct{}
type benchmarkNoopReply struct{}
type benchmarkSeedRequest struct{}
type benchmarkSeedReply struct{}

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

func dispatchBenchmarkEntity(ctx context.Context, instance benchmarkEntity, method string, _ any, _ any) error {
	switch method {
	case "Noop":
		return instance.Noop(ctx)
	case "Seed":
		return instance.Seed(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newBenchmarkEntityCall(method string) (args any, reply any) {
	switch method {
	case "Noop":
		return &benchmarkNoopRequest{}, &benchmarkNoopReply{}
	case "Seed":
		return &benchmarkSeedRequest{}, &benchmarkSeedReply{}
	default:
		return nil, nil
	}
}

func newBenchmarkRuntime(b *testing.B, backend store.Store, sourceClock clock.Clock, idleTimeout, evictionInterval time.Duration, options ...Option) *Runtime {
	b.Helper()
	options = append([]Option{
		WithStore(backend),
		WithClock(sourceClock),
		WithIdleTimeout(idleTimeout),
		WithEvictionInterval(evictionInterval),
	}, options...)
	rt, err := New(options...)
	if err != nil {
		b.Fatal(err)
	}
	installBenchmarkEntity(b, rt)
	b.Cleanup(rt.Close)
	return rt
}

func installBenchmarkEntity(b *testing.B, rt *Runtime) {
	b.Helper()
	if err := InstallType[benchmarkEntity](rt, dispatchBenchmarkEntity, func(invoker Invoker, id GrainId) benchmarkEntity {
		return &benchmarkEntityProxy{invoker: invoker, id: id}
	}, newBenchmarkEntityCall); err != nil {
		rt.Close()
		b.Fatal(err)
	}
	if err := Register[benchmarkEntity](rt, func(binder *Binder) benchmarkEntity {
		return &benchmarkEntityImpl{state: NewState[uint64](binder, "value")}
	}); err != nil {
		b.Fatal(err)
	}
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

func BenchmarkInvocationRoundTripWithOnCall(b *testing.B) {
	rt := newBenchmarkRuntime(b, store.NewMemory(), clock.Real{}, 0, 0, OnCall(func(CallObservation) {}))
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

func BenchmarkForwardingRoundTrip(b *testing.B) {
	first, _, localID, remoteID := newBenchmarkForwardingRuntimes(b)
	local := Ref[benchmarkEntity](first, localID.GrainKey)
	remote := Ref[benchmarkEntity](first, remoteID.GrainKey)
	if err := local.Noop(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := remote.Noop(context.Background()); err != nil {
		b.Fatal(err)
	}

	benchmarkInvocation := func(b *testing.B, entity benchmarkEntity) {
		b.Helper()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := entity.Noop(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("Local", func(b *testing.B) {
		benchmarkInvocation(b, local)
	})
	b.Run("Forwarded", func(b *testing.B) {
		benchmarkInvocation(b, remote)
	})
}

func newBenchmarkForwardingRuntimes(b *testing.B) (*Runtime, *Runtime, GrainId, GrainId) {
	b.Helper()
	backend := store.NewMemory()
	members := store.NewMemory()
	sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
	firstTransport, err := transport.New("127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	secondTransport, err := transport.New("127.0.0.1:0")
	if err != nil {
		_ = firstTransport.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = firstTransport.Close() })
	b.Cleanup(func() { _ = secondTransport.Close() })

	newNode := func(nodeTransport *transport.TCP, generation string) *Runtime {
		rt, err := New(
			WithStore(backend),
			WithMemberStore(members),
			WithNodeAddr(nodeTransport.Addr()),
			WithGeneration(generation),
			WithClock(sourceClock),
			WithHeartbeatInterval(time.Hour),
			WithViewInterval(time.Hour),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(0),
			WithTransport(nodeTransport),
		)
		if err != nil {
			b.Fatal(err)
		}
		installBenchmarkEntity(b, rt)
		b.Cleanup(rt.Close)
		return rt
	}
	first := newNode(firstTransport, "benchmark-first")
	second := newNode(secondTransport, "benchmark-second")
	view := cluster.NewView([]store.Member{
		{NodeAddr: firstTransport.Addr(), Generation: "benchmark-first", Status: store.MemberActive},
		{NodeAddr: secondTransport.Addr(), Generation: "benchmark-second", Status: store.MemberActive},
	})
	first.clusterView.Store(&view)
	second.clusterView.Store(&view)

	var localID, remoteID GrainId
	for index := 0; index < 4096; index++ {
		id := GrainId{GrainType: TypeName[benchmarkEntity](), GrainKey: fmt.Sprintf("forward-%04d", index)}
		owner, ok := cluster.Owner(view, store.GrainId(id))
		if !ok {
			continue
		}
		switch owner {
		case firstTransport.Addr():
			localID = id
		case secondTransport.Addr():
			remoteID = id
		}
		if localID != (GrainId{}) && remoteID != (GrainId{}) {
			return first, second, localID, remoteID
		}
	}
	b.Fatal("could not find local and forwarded benchmark identities")
	return nil, nil, GrainId{}, GrainId{}
}

func BenchmarkStateWrite(b *testing.B) {
	database := openBenchmarkSQLite(b, "state.db")
	rt, err := New(WithStore(database), WithClock(clock.Real{}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { rt.Close() })

	binder := newBinder(rt, GrainId{GrainType: "benchmark", GrainKey: "state"})
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
	for len(rt.Activations()) != 0 {
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
