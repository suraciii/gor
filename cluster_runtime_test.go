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

func TestNew_ClusterJoinErrorIsReturned(t *testing.T) {
	_, err := New(WithMemberStore(failingMemberStore{}))
	if err == nil {
		t.Fatal("New returned nil error for a failed cluster join")
	}
}

func TestRuntime_ClusterRejectsInvocationForAnotherOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(600, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		first := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")...)
		second := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")...)
		registerAccount(t, first)
		registerAccount(t, second)
		synctest.Wait()

		fakeClock.Advance(time.Second)
		synctest.Wait()

		var target Identity
		var owner string
		for index := 0; index < 4096; index++ {
			candidate := Identity{Type: TypeName[Account](), Key: strconv.Itoa(index)}
			err := first.Invoke(context.Background(), candidate, "Balance", nil, new(int64))
			var wrongOwner WrongOwnerError
			if errors.As(err, &wrongOwner) {
				target = candidate
				owner = wrongOwner.Owner
				break
			}
			if err != nil {
				t.Fatalf("local candidate %v error = %v", candidate, err)
			}
		}
		if target == (Identity{}) {
			t.Fatal("no identity was routed to the other owner")
		}
		if owner != "node-b" {
			t.Fatalf("wrong owner = %q, want node-b", owner)
		}

		var balance int64
		if err := second.Invoke(context.Background(), target, "Balance", nil, &balance); err != nil {
			t.Fatalf("owner invocation error = %v", err)
		}
		first.Close()
		second.Close()
	})
}

func TestRuntime_ClusterDeactivatesMovedActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(700, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		first := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")...)
		registerFactoryCalls := atomic.Int32{}
		installAccount(t, first)
		if err := Register[Account](first, func(b *Binder) Account {
			registerFactoryCalls.Add(1)
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

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
		var target Identity
		for index := 0; index < 4096; index++ {
			candidate := Identity{Type: TypeName[Account](), Key: strconv.Itoa(index)}
			beforeOwner, beforeOK := cluster.Owner(before, store.Identity(candidate))
			afterOwner, afterOK := cluster.Owner(after, store.Identity(candidate))
			if beforeOK && afterOK && beforeOwner == "node-a" && afterOwner == "node-b" {
				target = candidate
				break
			}
		}
		if target == (Identity{}) {
			t.Fatal("no identity moved from node-a to node-b")
		}

		if err := first.Invoke(context.Background(), target, "Balance", nil, new(int64)); err != nil {
			t.Fatalf("initial local invocation error = %v", err)
		}
		if got := registerFactoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls after initial invocation = %d, want 1", got)
		}

		second := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")...)
		registerAccount(t, second)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()

		var balance int64
		if err := first.Runtime.Invoke(context.Background(), target, "Balance", nil, &balance); err != nil {
			t.Fatalf("direct runtime invocation after ownership change = %v", err)
		}
		if got := registerFactoryCalls.Load(); got != 2 {
			t.Fatalf("factory calls after ownership change = %d, want 2", got)
		}
		first.Close()
		second.Close()
	})
}

func TestRuntime_ClusterKillLeavesMemberForFailureDetection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(800, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a")...)
		rt.Kill()

		member := findClusterMember(t, members, "node-a", "generation-a")
		if member.Status != store.MemberActive {
			t.Fatalf("member after Kill = %#v, want active row left for failure detection", member)
		}
	})
}

func clusterRuntimeOptions(backend store.Store, members store.MemberStore, sourceClock clock.Clock, nodeAddr, generation string) []Option {
	return []Option{
		WithStore(backend),
		WithMemberStore(members),
		WithNodeAddr(nodeAddr),
		WithGeneration(generation),
		WithClock(sourceClock),
		WithHeartbeatInterval(time.Hour),
		WithViewInterval(time.Second),
		WithDeadAfter(time.Hour),
		WithIdleTimeout(0),
		WithEvictionInterval(0),
	}
}

func findClusterMember(t *testing.T, backend store.MemberStore, nodeAddr, generation string) store.Member {
	t.Helper()
	members, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeAddr == nodeAddr && member.Generation == generation {
			return member
		}
	}
	t.Fatalf("member %s/%s not found in %#v", nodeAddr, generation, members)
	return store.Member{}
}

type failingMemberStore struct{}

func (failingMemberStore) WriteMember(context.Context, store.Member) (store.ETag, error) {
	return 0, errors.New("member store unavailable")
}

func (failingMemberStore) ListMembers(context.Context) ([]store.Member, error) {
	return nil, errors.New("member store unavailable")
}
