package gor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
)

// chainEntity is a test entity whose Chain method walks a ring of entity
// keys: it calls the first key's Chain with the rest of the ring. A test
// constructs a cycle by closing the ring on a key already walked.
type chainEntity interface {
	Chain(context.Context, []string) error
	Block(context.Context) error
}

type chainEntityImpl struct {
	b            *Binder
	blockStarted chan struct{}
	blockRelease chan struct{}
}

func (e *chainEntityImpl) Chain(ctx context.Context, ring []string) error {
	if len(ring) == 0 {
		return nil
	}
	return Ref[chainEntity](e.b, ring[0]).Chain(ctx, ring[1:])
}

func (e *chainEntityImpl) Block(ctx context.Context) error {
	if e.blockStarted != nil {
		close(e.blockStarted)
	}
	if e.blockRelease != nil {
		<-e.blockRelease
	}
	return nil
}

type chainRequest struct {
	A0 []string
}

type chainReply struct{}

type chainBlockRequest struct{}

type chainBlockReply struct{}

func dispatchChain(ctx context.Context, instance chainEntity, method string, args any, reply any) error {
	switch method {
	case "Chain":
		return instance.Chain(ctx, args.(*chainRequest).A0)
	case "Block":
		return instance.Block(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newChainCall(method string) (args any, reply any) {
	switch method {
	case "Chain":
		return &chainRequest{}, &chainReply{}
	case "Block":
		return &chainBlockRequest{}, &chainBlockReply{}
	default:
		return nil, nil
	}
}

type chainEntityProxy struct {
	invoker Invoker
	id      GrainId
}

func (p *chainEntityProxy) Chain(ctx context.Context, ring []string) error {
	var reply chainReply
	return p.invoker.Invoke(ctx, p.id, "Chain", &chainRequest{A0: ring}, &reply)
}

func (p *chainEntityProxy) Block(ctx context.Context) error {
	var reply chainBlockReply
	return p.invoker.Invoke(ctx, p.id, "Block", &chainBlockRequest{}, &reply)
}

func installChainWithFactory(t *testing.T, rt *Runtime, factory func(*Binder) chainEntity) {
	t.Helper()
	if err := InstallType[chainEntity](rt, dispatchChain, func(invoker Invoker, id GrainId) chainEntity {
		return &chainEntityProxy{invoker: invoker, id: id}
	}, newChainCall, nil); err != nil {
		t.Fatal(err)
	}
	if err := Register[chainEntity](rt, factory); err != nil {
		t.Fatal(err)
	}
}

func installChain(t *testing.T, rt *Runtime) {
	t.Helper()
	installChainWithFactory(t, rt, func(b *Binder) chainEntity {
		return &chainEntityImpl{b: b}
	})
}

func installBlockingChain(t *testing.T, rt *Runtime, started, release chan struct{}) {
	t.Helper()
	installChainWithFactory(t, rt, func(b *Binder) chainEntity {
		return &chainEntityImpl{b: b, blockStarted: started, blockRelease: release}
	})
}

func chainID(key string) GrainId {
	return GrainId{GrainType: TypeName[chainEntity](), GrainKey: key}
}

func assertCallCycle(t *testing.T, err error, keys ...string) {
	t.Helper()
	if !errors.Is(err, ErrCallCycle) {
		t.Fatalf("error = %v, want %v", err, ErrCallCycle)
	}
	if code, ok := CodeOf(err); !ok || code != ErrCallCycle {
		t.Fatalf("CodeOf(error) = (%q, %v), want (%q, true)", code, ok, ErrCallCycle)
	}
	message := err.Error()
	if !strings.Contains(message, "call cycle detected") {
		t.Fatalf("error message = %q, want it to say a call cycle was detected", message)
	}
	for _, key := range keys {
		if !strings.Contains(message, key) {
			t.Fatalf("error message = %q, want it to name %q", message, key)
		}
	}
}

func TestCallCycle_TwoEntityCycleNamesBothEntities(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installChain(t, rt)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rt.Invoke(ctx, chainID("cycle-a"), "Chain", &chainRequest{A0: []string{"cycle-b", "cycle-a"}}, &chainReply{})
		assertCallCycle(t, err, "cycle-a", "cycle-b")
	})
}

func TestCallCycle_ThreeEntityCycleNamesWholeCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installChain(t, rt)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rt.Invoke(ctx, chainID("cycle-a"), "Chain", &chainRequest{A0: []string{"cycle-b", "cycle-c", "cycle-a"}}, &chainReply{})
		assertCallCycle(t, err, "cycle-a", "cycle-b", "cycle-c")
	})
}

func TestCallCycle_SelfCallIsACycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installChain(t, rt)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rt.Invoke(ctx, chainID("cycle-a"), "Chain", &chainRequest{A0: []string{"cycle-a"}}, &chainReply{})
		assertCallCycle(t, err, "cycle-a")
	})
}

func TestCallCycle_SelfCallDuringActivationIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installChainWithFactory(t, rt, func(b *Binder) chainEntity {
			return &selfCallingEntity{b: b}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rt.Invoke(ctx, chainID("activating"), "Chain", &chainRequest{A0: []string{"activating"}}, &chainReply{})
		assertCallCycle(t, err, "activating")
	})
}

// selfCallingEntity calls itself from OnActivate. The triggering call occupies
// the entity from admission on, so the activation-time call back into it is a
// cycle rather than a wait for an activation that cannot complete.
type selfCallingEntity struct {
	b *Binder
}

func (e *selfCallingEntity) Chain(ctx context.Context, ring []string) error {
	return Ref[chainEntity](e.b, ring[0]).Chain(ctx, ring[1:])
}

func (*selfCallingEntity) Block(context.Context) error {
	return nil
}

func (e *selfCallingEntity) OnActivate(ctx context.Context) error {
	return Ref[chainEntity](e.b, Self(e.b).GrainKey).Chain(ctx, []string{Self(e.b).GrainKey})
}

func TestCallCycle_SlowCallWithoutCycleTimesOutPlainly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		started := make(chan struct{})
		release := make(chan struct{})
		installBlockingChain(t, rt, started, release)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		callDone := make(chan error, 1)
		go func() {
			callDone <- rt.Invoke(ctx, chainID("slow"), "Block", &chainBlockRequest{}, &chainBlockReply{})
		}()
		<-started
		// The root goroutine blocks on a channel, so the bubble advances time
		// until the caller's deadline fires and the slow call returns.
		err := <-callDone
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("slow call error = %v, want context.DeadlineExceeded", err)
		}
		if errors.Is(err, ErrCallCycle) {
			t.Fatalf("slow call reported a call cycle: %v", err)
		}
		if code, ok := CodeOf(err); ok {
			t.Fatalf("slow call error code = %q, want none", code)
		}
		close(release)
		synctest.Wait()
	})
}

func TestCallCycle_ForwardedCycleNamesBothEntities(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1500, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstTransport := network.add("node-a")
		secondTransport := network.add("node-b")
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions, WithTransport(firstTransport))
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")
		secondOptions = append(secondOptions, WithTransport(secondTransport))
		first := mustNew(t, firstOptions...)
		second := mustNew(t, secondOptions...)
		defer first.Close()
		defer second.Close()
		installChain(t, first)
		installChain(t, second)
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		fakeClock.Advance(time.Second)
		synctest.Wait()

		a := findChainOwner(t, first, "node-a")
		b := findChainOwner(t, first, "node-b")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := first.Invoke(ctx, a, "Chain", &chainRequest{A0: []string{b.GrainKey, a.GrainKey}}, &chainReply{})
		assertCallCycle(t, err, a.GrainKey, b.GrainKey)
	})
}

func findChainOwner(t *testing.T, rt *Runtime, owner string) GrainId {
	t.Helper()
	view := rt.clusterView.Load()
	for index := 0; index < 4096; index++ {
		candidate := chainID(fmt.Sprintf("chain-%d", index))
		if candidateOwner, ok := cluster.Owner(*view, store.GrainId(candidate)); ok && candidateOwner == owner {
			return candidate
		}
	}
	t.Fatalf("no identity owned by %q", owner)
	return GrainId{}
}
