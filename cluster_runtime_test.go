package gor

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

func TestNew_ClusterJoinErrorIsReturned(t *testing.T) {
	_, err := New(WithMemberStore(failingMemberStore{}))
	if err == nil {
		t.Fatal("New returned nil error for a failed cluster join")
	}
}

func TestNew_ClusterRequiresTransport(t *testing.T) {
	_, err := New(WithMemberStore(store.NewMemory()))
	if err == nil {
		t.Fatal("New returned nil error for a cluster without transport")
	}
}

func TestNew_TransportRequiresMemberStore(t *testing.T) {
	network := newTestTransportNetwork()
	_, err := New(WithTransport(network.add("node-a")))
	if err == nil {
		t.Fatal("New returned nil error for a transport without member store")
	}
}

func TestRuntime_ClusterForwardsInvocationToAnotherOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(600, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		first := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		second := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		registerAccount(t, first)
		registerAccount(t, second)
		synctest.Wait()

		fakeClock.Advance(time.Second)
		synctest.Wait()

		var target Identity
		for index := 0; index < 4096; index++ {
			candidate := Identity{Type: TypeName[Account](), Key: strconv.Itoa(index)}
			owner, ok := cluster.Owner(*first.clusterView.Load(), store.Identity(candidate))
			if ok && owner == "node-b" {
				target = candidate
				break
			}
		}
		if target == (Identity{}) {
			t.Fatal("no identity was routed to the other owner")
		}

		var balance accountBalanceReply
		if err := first.Invoke(context.Background(), target, "Balance", &accountBalanceRequest{}, &balance); err != nil {
			t.Fatalf("forwarded invocation error = %v", err)
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
		network := newTestTransportNetwork()
		first := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
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

		if err := first.Invoke(context.Background(), target, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); err != nil {
			t.Fatalf("initial local invocation error = %v", err)
		}
		if got := registerFactoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls after initial invocation = %d, want 1", got)
		}

		second := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		registerAccount(t, second)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()

		var balance accountBalanceReply
		if err := first.Runtime.Invoke(context.Background(), target, "Balance", &accountBalanceRequest{}, &balance); err != nil {
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
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		rt.Kill()

		member := findClusterMember(t, members, "node-a", "generation-a")
		if member.Status != store.MemberActive {
			t.Fatalf("member after Kill = %#v, want active row left for failure detection", member)
		}
	})
}

func TestRuntime_DoneClosesForCloseAndKill(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*Runtime)
	}{
		{name: "close", stop: (*Runtime).Close},
		{name: "kill", stop: (*Runtime).Kill},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fakeClock := clock.NewFake(time.Unix(900, 0).UTC())
				members := store.NewMemory()
				network := newTestTransportNetwork()
				rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
				test.stop(rt)

				select {
				case <-rt.Done():
				default:
					t.Fatal("runtime Done channel is still open after stop")
				}
			})
		})
	}
}

func TestRuntime_ClusterDeathStopsAndDeactivates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1000, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions,
			WithHeartbeatInterval(time.Second),
			WithViewInterval(time.Hour),
			WithTransport(network.add("node-a")),
		)
		first := mustNew(t, firstOptions...)
		registerAccount(t, first)
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")
		secondOptions = append(secondOptions,
			WithHeartbeatInterval(time.Hour),
			WithViewInterval(time.Hour),
			WithTransport(network.add("node-b")),
		)
		second := mustNew(t, secondOptions...)

		id := Identity{Type: TypeName[Account](), Key: "self-death"}
		if err := first.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); err != nil {
			t.Fatalf("initial invocation error = %v", err)
		}
		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := members.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark self dead: %v", err)
		}

		fakeClock.Advance(time.Second)
		synctest.Wait()
		select {
		case <-first.Done():
		default:
			t.Fatal("runtime Done channel is still open after cluster death")
		}
		if identities := first.Runtime.Identities(); len(identities) != 0 {
			t.Fatalf("identities after cluster death = %#v, want empty", identities)
		}
		if err := first.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); err == nil {
			t.Fatal("invocation after cluster death unexpectedly succeeded")
		}
		if err := first.Runtime.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, runtimepkg.ErrRuntimeClosed) {
			t.Fatalf("direct runtime invocation after cluster death error = %v, want %v", err, runtimepkg.ErrRuntimeClosed)
		}

		first.Close()
		second.Close()
	})
}

func TestRuntime_HandleRejectsAfterClusterDeath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1100, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		network := newTestTransportNetwork()
		first := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		second := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		registerAccount(t, first)

		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := members.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		select {
		case <-first.done:
		default:
			t.Fatal("runtime done is still open after cluster death")
		}
		if first.shuttingDown.Load() {
			t.Fatal("cluster death incorrectly marked runtime as shutting down")
		}

		payload, err := first.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
		if err != nil {
			t.Fatalf("handle error = %v, want nil", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error != runtimepkg.ErrRuntimeClosed.Error() {
			t.Fatalf("response error = %q, want %q", response.Error, runtimepkg.ErrRuntimeClosed.Error())
		}

		first.Close()
		second.Close()
	})
}

func clusterRuntimeOptions(backend store.Store, members store.MemberStore, sourceClock clock.Clock, nodeAddr, generation string, endpoints ...transport.Transport) []Option {
	options := []Option{
		WithStore(backend),
		WithMemberStore(members),
		WithNodeAddr(nodeAddr),
		WithGeneration(generation),
		WithClock(sourceClock),
		WithHeartbeatInterval(time.Hour),
		WithViewInterval(time.Second),
		WithProbeInterval(time.Second),
		WithProbeTimeout(500 * time.Millisecond),
		WithProbeFailures(3),
		WithVoteTTL(6 * time.Second),
		WithMaxTickGap(2 * time.Second),
		WithMaxTableLatency(500 * time.Millisecond),
		WithIdleTimeout(0),
		WithEvictionInterval(0),
	}
	if len(endpoints) > 0 {
		options = append(options, WithTransport(endpoints[0]))
	}
	return options
}

func findClusterMember(t *testing.T, backend store.MemberStore, nodeAddr, generation string) store.Member {
	t.Helper()
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	for _, member := range snapshot.Members {
		if member.NodeAddr == nodeAddr && member.Generation == generation {
			return member
		}
	}
	t.Fatalf("member %s/%s not found in %#v", nodeAddr, generation, snapshot.Members)
	return store.Member{}
}

type failingMemberStore struct{}

func (failingMemberStore) WriteMember(context.Context, store.Member) (store.ETag, error) {
	return 0, errors.New("member store unavailable")
}

func (failingMemberStore) ListMembers(context.Context) (store.MemberSnapshot, error) {
	return store.MemberSnapshot{}, errors.New("member store unavailable")
}
