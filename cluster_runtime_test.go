package gor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
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
		if err := first.engine.Invoke(context.Background(), target, "Balance", &accountBalanceRequest{}, &balance); err != nil {
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
		if activations := first.Activations(); len(activations) != 0 {
			t.Fatalf("activations after cluster death = %#v, want empty", activations)
		}
		if err := first.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); err == nil {
			t.Fatal("invocation after cluster death unexpectedly succeeded")
		}
		if err := first.engine.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, runtimepkg.ErrRuntimeClosed) {
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

		payload, err := first.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
		if err != nil {
			t.Fatalf("handle error = %v, want nil", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrNodeDead) {
			t.Fatalf("response error = %#v, want node-dead code after cluster death", response.Error)
		}

		first.Close()
		second.Close()
	})
}

// TestRuntime_ClusterDeathClosesTransportAndStops pins the death path's
// teardown: once the cluster declares this node dead, the runtime waits for
// admitted calls, closes its transport, and only then reaches the terminal
// stopped state. The transport is root infrastructure, so stopped must not be
// announced while it is still serving.
func TestRuntime_ClusterDeathClosesTransportAndStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1150, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		network := newTestTransportNetwork()
		firstOptions := clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions,
			WithHeartbeatInterval(time.Second),
			WithViewInterval(time.Hour),
			WithTransport(network.add("node-a")),
		)
		first := mustNew(t, firstOptions...)
		second := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)

		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := members.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		select {
		case <-first.Done():
		default:
			t.Fatal("runtime Done is still open after cluster death")
		}
		select {
		case <-first.transportDone:
		default:
			t.Fatal("transport is still serving after cluster death")
		}
		first.lifecycleMu.Lock()
		state := first.state
		first.lifecycleMu.Unlock()
		if state != rootStopped {
			t.Fatalf("root state after cluster death = %v, want stopped", state)
		}

		first.Close()
		second.Close()
	})
}

func TestRuntime_ClusterDeathSkipsOnDeactivate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1800, 0).UTC()
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
		deactivateCalls := new(atomic.Int32)
		installLifecycleAccount(t, first, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateCalls = deactivateCalls
		})

		id := Identity{Type: TypeName[lifecycleAccount](), Key: "self-death"}
		if err := first.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
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
			t.Fatal("runtime Done is still open after cluster death")
		}
		// External death collapses to sudden stop, so OnDeactivate must not run.
		if got := deactivateCalls.Load(); got != 0 {
			t.Fatalf("OnDeactivate calls after cluster death = %d, want 0 (sudden stop)", got)
		}
		first.Close()
	})
}

// sideEffectEntity records every Touch as a side effect and carries no state.
// It exists so a test can tell whether a method body ran on a particular node.
type sideEffectEntity interface {
	Touch(context.Context) error
}

type sideEffectTouchRequest struct{}
type sideEffectTouchReply struct{}

type sideEffectEntityProxy struct {
	invoker Invoker
	id      Identity
}

func (p *sideEffectEntityProxy) Touch(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Touch", &sideEffectTouchRequest{}, &sideEffectTouchReply{})
}

type sideEffectEntityImpl struct {
	calls *atomic.Int32
}

func (e *sideEffectEntityImpl) Touch(context.Context) error {
	e.calls.Add(1)
	return nil
}

func dispatchSideEffectEntity(ctx context.Context, instance sideEffectEntity, method string, _ any, _ any) error {
	if method != "Touch" {
		return fmt.Errorf("unknown method %q", method)
	}
	return instance.Touch(ctx)
}

func newSideEffectEntityCall(method string) (args any, reply any) {
	if method != "Touch" {
		return nil, nil
	}
	return &sideEffectTouchRequest{}, &sideEffectTouchReply{}
}

func installSideEffectEntity(t *testing.T, rt *Runtime, calls *atomic.Int32) {
	t.Helper()
	if err := InstallType[sideEffectEntity](rt, dispatchSideEffectEntity, func(invoker Invoker, id Identity) sideEffectEntity {
		return &sideEffectEntityProxy{invoker: invoker, id: id}
	}, newSideEffectEntityCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[sideEffectEntity](rt, func(b *Binder) sideEffectEntity {
		return &sideEffectEntityImpl{calls: calls}
	}); err != nil {
		t.Fatal(err)
	}
}

// barrierMemberStore wraps a member store so a dead-status write is persisted
// but held open until the test releases it. It lets a death scenario pin the
// moment the dead row is visible without the WriteMember call returning.
type barrierMemberStore struct {
	store.MemberStore
	deadWritten chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (s *barrierMemberStore) WriteMember(ctx context.Context, m store.Member) (store.ETag, error) {
	etag, err := s.MemberStore.WriteMember(ctx, m)
	if err == nil && m.Status == store.MemberDead {
		s.once.Do(func() { close(s.deadWritten) })
		<-s.release
	}
	return etag, err
}

// TestScenario_ClusterDeathStopsNodeAndHandsoff is the cluster-death scenario:
// once the cluster declares this node dead, its Done signal closes, a direct
// call on it is rejected without running the method, and another node that has
// converged can execute the same entity. A write barrier on the dead row pins
// the moment the row is visible.
func TestScenario_ClusterDeathStopsNodeAndHandsoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(2200, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := &barrierMemberStore{
			MemberStore: store.NewMemory(),
			deadWritten: make(chan struct{}),
			release:     make(chan struct{}),
		}
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		nodeA := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		nodeB := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		calls := new(atomic.Int32)
		installSideEffectEntity(t, nodeA, calls)
		installSideEffectEntity(t, nodeB, calls)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()

		var id Identity
		for index := 0; index < 4096; index++ {
			candidate := Identity{Type: TypeName[sideEffectEntity](), Key: strconv.Itoa(index)}
			if owner, ok := cluster.Owner(*nodeA.clusterView.Load(), store.Identity(candidate)); ok && owner == "node-a" {
				id = candidate
				break
			}
		}
		if id == (Identity{}) {
			t.Fatal("no identity owned by node-a")
		}
		if err := nodeA.Invoke(context.Background(), id, "Touch", &sideEffectTouchRequest{}, &sideEffectTouchReply{}); err != nil {
			t.Fatalf("initial touch on owner = %v, want nil", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("calls after initial touch = %d, want 1", got)
		}

		// Declare node-a dead. The write persists and the barrier holds it open.
		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		go func() {
			if _, err := members.WriteMember(context.Background(), self); err != nil {
				t.Errorf("mark node dead: %v", err)
			}
		}()
		<-members.deadWritten
		fakeClock.Advance(time.Second)
		synctest.Wait()

		// A direct call on the dead node is rejected and its method body never
		// runs. The side-effect check comes first so a mutation that keeps the
		// node serving is caught at the method body, not at a downstream signal.
		callsBefore := calls.Load()
		touchErr := nodeA.Invoke(context.Background(), id, "Touch", &sideEffectTouchRequest{}, &sideEffectTouchReply{})
		if got := calls.Load(); got != callsBefore {
			t.Fatalf("method body ran on dead node-a: calls = %d, want %d", got, callsBefore)
		}
		if !errors.Is(touchErr, ErrNodeDead) {
			t.Fatalf("touch on dead node-a = %v, want ErrNodeDead", touchErr)
		}
		select {
		case <-nodeA.Done():
		default:
			t.Fatal("node-a Done is still open after cluster death")
		}

		// The node that converged can execute the same entity.
		if err := nodeB.Invoke(context.Background(), id, "Touch", &sideEffectTouchRequest{}, &sideEffectTouchReply{}); err != nil {
			t.Fatalf("touch on node-b after handoff = %v, want nil", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("calls after touch on node-b = %d, want 2", got)
		}

		close(members.release)
		nodeA.Close()
		nodeB.Close()
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
