package gor

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
)

// failingStore fails every state write. It exists to trigger the discard path:
// a failed write makes the runtime discard the current instance.
type failingStore struct {
	*store.Memory
}

func (s *failingStore) Write(_ context.Context, _ store.GrainId, _ []byte, _ store.ETag) (store.ETag, error) {
	return 0, errors.New("simulated write failure")
}

// TestDeactivationReason_Idle covers the idle mapping: the hook sees Idle
// when the fake clock advances past the idle timeout.
func TestDeactivationReason_Idle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		reasons := make(chan DeactivationReason, 1)
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(time.Second),
			WithEvictionInterval(time.Second),
		)
		defer rt.Close()
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
		})

		id := GrainId{GrainType: TypeName[lifecycleAccount](), GrainKey: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		select {
		case got := <-reasons:
			if got != Idle {
				t.Fatalf("deactivation reason = %v, want Idle", got)
			}
		default:
			t.Fatal("idle eviction did not run the deactivation hook")
		}
	})
}

// TestDeactivationReason_ContextIsBackground pins the lifecycle context
// contract: the hook receives a fresh context with no deadline that is never
// canceled, independent of any caller's context.
func TestDeactivationReason_ContextIsBackground(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		contexts := make(chan context.Context, 1)
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(time.Second),
			WithEvictionInterval(time.Second),
		)
		defer rt.Close()
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateContexts = contexts
		})

		id := GrainId{GrainType: TypeName[lifecycleAccount](), GrainKey: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		select {
		case ctx := <-contexts:
			if ctx.Done() != nil {
				t.Fatal("deactivation context is cancelable")
			}
			if deadline, ok := ctx.Deadline(); ok {
				t.Fatalf("deactivation context has a deadline: %v", deadline)
			}
			if ctx.Err() != nil {
				t.Fatalf("deactivation context is already canceled: %v", ctx.Err())
			}
		default:
			t.Fatal("idle eviction did not run the deactivation hook")
		}
	})
}

// TestDeactivationReason_RuntimeClosed covers the graceful-close mapping and
// the ordering contract: Close starts the hook with RuntimeClosed, waits for
// it to return, and only then completes.
func TestDeactivationReason_RuntimeClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reasons := make(chan DeactivationReason, 1)
		releaseHook := make(chan struct{})
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
			entity.releaseDeactivate = releaseHook
		})

		id := GrainId{GrainType: TypeName[lifecycleAccount](), GrainKey: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()

		select {
		case got := <-reasons:
			if got != RuntimeClosed {
				t.Fatalf("deactivation reason = %v, want RuntimeClosed", got)
			}
		default:
			t.Fatal("Close did not run the deactivation hook")
		}
		select {
		case <-closeDone:
			t.Fatal("Close completed before the deactivation hook returned")
		default:
		}

		close(releaseHook)
		synctest.Wait()
		<-closeDone
	})
}

// TestDeactivationReason_FaultedPanic covers the panic mapping: a panicking
// method discards the instance and the hook sees Faulted.
func TestDeactivationReason_FaultedPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reasons := make(chan DeactivationReason, 1)
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
		})

		if err := Ref[lifecycleAccount](rt, "alice").Panic(context.Background()); err == nil {
			t.Fatal("panicking method returned a nil error")
		}
		synctest.Wait()

		select {
		case got := <-reasons:
			if got != Faulted {
				t.Fatalf("deactivation reason = %v, want Faulted", got)
			}
		default:
			t.Fatal("panic did not run the deactivation hook")
		}
		if got := rt.Activations(); len(got) != 0 {
			t.Fatalf("activations after panic = %#v, want none", got)
		}
	})
}

// TestDeactivationReason_FaultedDiscard covers the discard mapping: a failed
// state write makes the entity discard the current instance and the hook sees
// Faulted.
func TestDeactivationReason_FaultedDiscard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reasons := make(chan DeactivationReason, 1)
		rt := mustNew(t,
			WithStore(&failingStore{Memory: store.NewMemory()}),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
		)
		defer rt.Close()
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
		})

		err := Ref[lifecycleAccount](rt, "alice").SetValue(context.Background(), 5)
		if err == nil {
			t.Fatal("SetValue on a failing store succeeded")
		}
		synctest.Wait()

		select {
		case got := <-reasons:
			if got != Faulted {
				t.Fatalf("deactivation reason = %v, want Faulted", got)
			}
		default:
			t.Fatal("write failure did not run the deactivation hook")
		}
		if got := rt.Activations(); len(got) != 0 {
			t.Fatalf("activations after discard = %#v, want none", got)
		}
	})
}

// TestDeactivationReason_IdleSurvivesClose covers the fixed-reason contract
// from the hook's side: when Close arrives while an idle deactivation is
// already in flight, the hook still reports Idle, and Close waits for the
// in-flight hook instead of racing it.
func TestDeactivationReason_IdleSurvivesClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		reasons := make(chan DeactivationReason, 1)
		releaseHook := make(chan struct{})
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(time.Second),
			WithEvictionInterval(time.Second),
		)
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
			entity.releaseDeactivate = releaseHook
		})

		id := GrainId{GrainType: TypeName[lifecycleAccount](), GrainKey: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()
		select {
		case <-closeDone:
			t.Fatal("Close completed while the idle deactivation hook was still in flight")
		default:
		}

		close(releaseHook)
		synctest.Wait()
		select {
		case got := <-reasons:
			if got != Idle {
				t.Fatalf("deactivation reason = %v, want Idle (fixed at the idle transition)", got)
			}
		default:
			t.Fatal("idle deactivation hook did not report a reason")
		}
		<-closeDone
	})
}

// TestDeactivationReason_OwnershipLost covers the ownership mapping: when the
// member view changes so the identity no longer belongs to this node, the
// deactivation hook sees OwnershipLost.
func TestDeactivationReason_OwnershipLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(700, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		first := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		reasons := make(chan DeactivationReason, 1)
		installLifecycleAccount(t, first, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateReasons = reasons
		})

		before := cluster.NewView([]store.Member{{
			NodeAddr:   "node-a",
			Generation: "generation-a",
			Status:     store.MemberActive,
		}})
		after := cluster.NewView([]store.Member{
			{
				NodeAddr:   "node-a",
				Generation: "generation-a",
				Status:     store.MemberActive,
			},
			{
				NodeAddr:   "node-b",
				Generation: "generation-b",
				Status:     store.MemberActive,
			},
		})
		var target GrainId
		for index := 0; index < 4096; index++ {
			candidate := GrainId{GrainType: TypeName[lifecycleAccount](), GrainKey: strconv.Itoa(index)}
			beforeOwner, beforeOK := cluster.Owner(before, store.GrainId(candidate))
			afterOwner, afterOK := cluster.Owner(after, store.GrainId(candidate))
			if beforeOK && afterOK && beforeOwner == "node-a" && afterOwner == "node-b" {
				target = candidate
				break
			}
		}
		if target == (GrainId{}) {
			t.Fatal("no identity moved from node-a to node-b")
		}

		if err := first.Invoke(context.Background(), target, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial invocation error = %v", err)
		}

		second := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		registerAccount(t, second)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()

		select {
		case got := <-reasons:
			if got != OwnershipLost {
				t.Fatalf("deactivation reason = %v, want OwnershipLost", got)
			}
		default:
			t.Fatal("deactivation hook did not run after ownership change")
		}
		first.Close()
		second.Close()
	})
}
