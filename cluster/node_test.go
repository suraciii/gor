package cluster

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

const (
	testHeartbeat = time.Second
	testView      = time.Second
)

func TestNodeJoinWritesJoiningReadsThenActivates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0).UTC()
		backend := &recordingMemberStore{backend: store.NewMemory()}
		node, err := New(testNodeConfig(backend, clock.NewFake(start), "node-a", "generation-a"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		initial := <-node.ViewChanges()

		if node.State() != StateActive {
			t.Fatalf("state after New = %v, want active", node.State())
		}
		owner, ok := Owner(initial, store.Identity{Type: "account", Key: "join"})
		if !ok || owner != "node-a" {
			t.Fatalf("initial view owner = %q, %v; want node-a", owner, ok)
		}
		if got, want := backend.operations[:3], []string{"write:joining", "list", "write:active"}; !slices.Equal(got, want) {
			t.Fatalf("join operations = %v, want %v", got, want)
		}

		node.Close()
		if node.State() != StateDead {
			t.Fatalf("state after Close = %v, want dead", node.State())
		}
		if got := backend.operations[len(backend.operations)-1]; got != "write:dead" {
			t.Fatalf("last operation = %q, want write:dead", got)
		}
		member := findTestMember(t, backend, "node-a", "generation-a")
		if member.Status != store.MemberDead {
			t.Fatalf("member after Close = %#v, want dead", member)
		}
		node.Close()
	})
}

func TestNodeHeartbeatUpdatesAliveTimeWithCAS(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(200, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := &recordingMemberStore{backend: store.NewMemory()}
		node, err := New(testNodeConfig(backend, fakeClock, "node-a", "generation-a"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()

		fakeClock.Advance(testHeartbeat)
		synctest.Wait()

		member := findTestMember(t, backend, "node-a", "generation-a")
		if member.Status != store.MemberActive || member.ETag != 3 {
			t.Fatalf("member after heartbeat = %#v, want active with ETag 3; operations = %v", member, backend.operations)
		}
		if !member.IamAliveAt.Equal(start.Add(testHeartbeat)) {
			t.Fatalf("iam_alive_at = %s, want %s", member.IamAliveAt, start.Add(testHeartbeat))
		}
		node.Close()
	})
}

func TestNodeHeartbeatConflictAfterAppliedWriteKeepsNodeAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(250, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := &appliedConflictMemberStore{backend: store.NewMemory()}
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		config.ViewInterval = time.Hour
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()

		backend.nextAppliedConflict.Store(true)
		fakeClock.Advance(testHeartbeat)
		synctest.Wait()
		if node.State() != StateActive {
			t.Fatalf("state after applied heartbeat conflict = %v, want active", node.State())
		}
		member := findTestMember(t, backend, "node-a", "generation-a")
		if member.ETag != 3 || !member.IamAliveAt.Equal(start.Add(testHeartbeat)) {
			t.Fatalf("member after applied heartbeat conflict = %#v, want ETag 3 at first heartbeat", member)
		}

		fakeClock.Advance(testHeartbeat)
		synctest.Wait()
		member = findTestMember(t, backend, "node-a", "generation-a")
		if node.State() != StateActive || member.ETag != 4 || !member.IamAliveAt.Equal(start.Add(2*testHeartbeat)) {
			t.Fatalf("member after recovered heartbeat = %#v, state = %v; want ETag 4 at second heartbeat and active", member, node.State())
		}
		node.Close()
	})
}

func TestNodeViewPollingNotifiesWhenMemberJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(300, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		first, err := New(testNodeConfig(backend, fakeClock, "node-a", "generation-a"))
		if err != nil {
			t.Fatalf("New first: %v", err)
		}
		initial := <-first.ViewChanges()

		second, err := New(testNodeConfig(backend, fakeClock, "node-b", "generation-b"))
		if err != nil {
			t.Fatalf("New second: %v", err)
		}
		<-second.ViewChanges()
		synctest.Wait()

		fakeClock.Advance(testView)
		synctest.Wait()
		updated := <-first.ViewChanges()
		changed := 0
		for index := 0; index < 4096; index++ {
			identity := store.Identity{Type: "account", Key: strconv.Itoa(index)}
			beforeOwner, beforeOK := Owner(initial, identity)
			afterOwner, afterOK := Owner(updated, identity)
			if !beforeOK || beforeOwner != "node-a" || !afterOK || (afterOwner != "node-a" && afterOwner != "node-b") {
				t.Fatalf("owners for %v = %q/%v -> %q/%v, want node-a before and node-a/node-b after", identity, beforeOwner, beforeOK, afterOwner, afterOK)
			}
			if beforeOwner != afterOwner {
				changed++
			}
		}
		if changed == 0 {
			t.Fatal("member join did not change ownership of any identity")
		}

		second.Close()
		first.Close()
	})
}

func TestNodePollPreservesViewWhenListMembersFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(350, 0).UTC()
		backend := store.NewMemory()
		other := store.Member{
			NodeAddr:   "node-other",
			Generation: "generation-other",
			Status:     store.MemberActive,
			IamAliveAt: start,
		}
		if _, err := backend.WriteMember(context.Background(), other); err != nil {
			t.Fatalf("seed other member: %v", err)
		}

		fakeClock := clock.NewFake(start)
		table := &failingListMemberStore{backend: backend}
		node, err := New(testNodeConfig(table, fakeClock, "node-a", "generation-a"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		previous := <-node.ViewChanges()
		self := findTestMember(t, backend, "node-a", "generation-a")
		table.failNext.Store(true)

		got, alive := node.pollView(self, previous)
		if !alive {
			t.Fatal("node stopped after ListMembers failure")
		}
		if !sameView(got, previous) {
			t.Fatalf("view after ListMembers failure = %#v, want unchanged from %#v", got, previous)
		}
		node.Close()
	})
}

func TestNodePollDoesNotKillStaleMember(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(400, 0).UTC()
		backend := store.NewMemory()
		stale := store.Member{
			NodeAddr:   "node-stale",
			Generation: "generation-stale",
			Status:     store.MemberActive,
			IamAliveAt: start.Add(-10 * time.Second),
		}
		staleETag, err := backend.WriteMember(context.Background(), stale)
		if err != nil {
			t.Fatalf("seed stale member: %v", err)
		}
		stale.ETag = staleETag

		fakeClock := clock.NewFake(start)
		node, err := New(testNodeConfig(backend, fakeClock, "node-observer", "generation-observer"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		initial := <-node.ViewChanges()
		self := findTestMember(t, backend, "node-observer", "generation-observer")

		got, alive := node.pollView(self, initial)
		if !alive {
			t.Fatal("node stopped while polling a stale member")
		}
		if !sameView(got, initial) {
			t.Fatalf("view after stale-member poll changed = %#v, want unchanged", got)
		}
		stillActive := findTestMember(t, backend, stale.NodeAddr, stale.Generation)
		if stillActive.Status != store.MemberActive || stillActive.ETag != stale.ETag {
			t.Fatalf("stale member after poll = %#v, want unchanged active row", stillActive)
		}
		node.Close()
	})
}

func TestNodeStopsWhenHeartbeatCASFindsDeadSelf(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(500, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		config.ViewInterval = time.Hour
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()
		self := findTestMember(t, backend, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := backend.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}

		fakeClock.Advance(testHeartbeat)
		synctest.Wait()
		select {
		case <-node.Done():
		default:
			t.Fatal("node is still running after heartbeat CAS found a dead self")
		}
		if node.State() != StateDead {
			t.Fatalf("state after heartbeat found a dead self = %v, want dead", node.State())
		}
		node.Close()
	})
}

func TestNodeStopsWhenViewPollFindsDeadSelf(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(550, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		config.HeartbeatInterval = time.Hour
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()
		self := findTestMember(t, backend, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := backend.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}

		fakeClock.Advance(testView)
		synctest.Wait()
		select {
		case <-node.Done():
		default:
			t.Fatal("node is still running after view poll found a dead self")
		}
		if node.State() != StateDead {
			t.Fatalf("state after view poll found a dead self = %v, want dead", node.State())
		}
		node.Close()
	})
}

type recordingMemberStore struct {
	backend    store.MemberStore
	operations []string
}

type appliedConflictMemberStore struct {
	backend             store.MemberStore
	nextAppliedConflict atomic.Bool
}

type failingListMemberStore struct {
	backend  store.MemberStore
	failNext atomic.Bool
}

func (s *appliedConflictMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	if s.nextAppliedConflict.CompareAndSwap(true, false) {
		if _, err := s.backend.WriteMember(ctx, member); err != nil {
			return 0, err
		}
		return 0, store.ErrConflict
	}
	return s.backend.WriteMember(ctx, member)
}

func (s *appliedConflictMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	return s.backend.ListMembers(ctx)
}

func (s *failingListMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	return s.backend.WriteMember(ctx, member)
}

func (s *failingListMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	if s.failNext.CompareAndSwap(true, false) {
		return store.MemberSnapshot{}, errors.New("member list unavailable")
	}
	return s.backend.ListMembers(ctx)
}

func (s *recordingMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	s.operations = append(s.operations, "write:"+string(member.Status))
	return s.backend.WriteMember(ctx, member)
}

func (s *recordingMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	s.operations = append(s.operations, "list")
	return s.backend.ListMembers(ctx)
}

func testNodeConfig(table store.MemberStore, sourceClock clock.Clock, nodeAddr, generation string) Config {
	return Config{
		Table:             table,
		Clock:             sourceClock,
		Prober:            testProber{},
		NodeAddr:          nodeAddr,
		Generation:        generation,
		HeartbeatInterval: testHeartbeat,
		ViewInterval:      testView,
		ProbeInterval:     time.Hour,
		ProbeTimeout:      time.Second,
		ProbeFailures:     3,
		VoteTTL:           6 * time.Second,
		MaxTickGap:        2 * time.Hour,
		MaxTableLatency:   500 * time.Millisecond,
	}
}

type testProber struct{}

func (testProber) Probe(_ context.Context, target MemberID) <-chan ProbeResult {
	replies := make(chan ProbeResult, 1)
	replies <- ProbeResult{ID: target}
	return replies
}

func findTestMember(t *testing.T, backend store.MemberStore, nodeAddr, generation string) store.Member {
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

var _ store.MemberStore = (*recordingMemberStore)(nil)
var _ store.MemberStore = (*appliedConflictMemberStore)(nil)
var _ store.MemberStore = (*failingListMemberStore)(nil)
