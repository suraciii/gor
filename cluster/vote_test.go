package cluster

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

func TestShouldMarkDeadCountsOnlyCurrentNeighborsAndLiveVotes(t *testing.T) {
	start := time.Unix(3000, 0).UTC()
	backend := store.NewMemory(clock.NewFake(start))
	members := make([]store.Member, 0, 5)
	for index := range 5 {
		members = append(members, store.Member{
			NodeAddr:   "node-" + string(rune('a'+index)),
			Generation: "generation",
			Status:     store.MemberActive,
			IamAliveAt: start,
		})
	}
	for _, member := range members {
		if _, err := backend.WriteMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	target := MemberID{NodeAddr: members[0].NodeAddr, Generation: members[0].Generation}
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	neighbors := targetNeighbors(snapshot.Members, target)
	if len(neighbors) != 2 {
		t.Fatalf("target neighbors = %v, want 2", neighbors)
	}
	nonNeighbors := make([]MemberID, 0, 2)
	for _, member := range activeMemberIDs(snapshot.Members) {
		if member == target || containsMemberID(neighbors, member) {
			continue
		}
		nonNeighbors = append(nonNeighbors, member)
	}
	row := snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	row.SuspectVotes = map[MemberID]store.SuspectVote{
		nonNeighbors[0]: {ExpiresAt: start.Add(time.Second)},
		nonNeighbors[1]: {ExpiresAt: start.Add(time.Second)},
	}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if shouldMarkDead(snapshot, target) {
		t.Fatal("non-neighbor votes marked target dead")
	}

	row = snapshot.Members[memberIndex(snapshot.Members, row)]
	row.SuspectVotes = map[MemberID]store.SuspectVote{
		neighbors[0]: {ExpiresAt: start},
		neighbors[1]: {ExpiresAt: start},
	}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if shouldMarkDead(snapshot, target) {
		t.Fatal("expired neighbor votes marked target dead")
	}

	row = snapshot.Members[memberIndex(snapshot.Members, row)]
	row.SuspectVotes[neighbors[0]] = store.SuspectVote{ExpiresAt: start.Add(time.Second)}
	row.SuspectVotes[neighbors[1]] = store.SuspectVote{ExpiresAt: start.Add(time.Second)}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !shouldMarkDead(snapshot, target) {
		t.Fatal("two current unexpired neighbor votes did not mark target dead")
	}
}

func TestShouldMarkDeadUsesMinNeighborThreshold(t *testing.T) {
	start := time.Unix(3100, 0).UTC()
	clockSource := clock.NewFake(start)
	backend := store.NewMemory(clockSource)
	for _, member := range []store.Member{
		{NodeAddr: "node-a", Generation: "generation-a", Status: store.MemberActive, IamAliveAt: start},
		{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive, IamAliveAt: start},
	} {
		if _, err := backend.WriteMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	target := MemberID{NodeAddr: "node-a", Generation: "generation-a"}
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	row := snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	row.SuspectVotes = map[MemberID]store.SuspectVote{
		{NodeAddr: "node-b", Generation: "generation-b"}: {ExpiresAt: start.Add(time.Second)},
	}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !shouldMarkDead(snapshot, target) {
		t.Fatal("one vote did not mark a two-member target dead")
	}

	backend = store.NewMemory(clockSource)
	for _, member := range []store.Member{
		{NodeAddr: "node-a", Generation: "generation-a", Status: store.MemberActive, IamAliveAt: start},
		{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive, IamAliveAt: start},
		{NodeAddr: "node-c", Generation: "generation-c", Status: store.MemberActive, IamAliveAt: start},
	} {
		if _, err := backend.WriteMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target = MemberID{NodeAddr: "node-a", Generation: "generation-a"}
	row = snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	neighbors := targetNeighbors(snapshot.Members, target)
	row.SuspectVotes = map[MemberID]store.SuspectVote{
		neighbors[0]: {ExpiresAt: start.Add(time.Second)},
	}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if shouldMarkDead(snapshot, target) {
		t.Fatal("one vote marked a three-member target dead")
	}
}

func TestUpdateSuspectVoteRecomputesAfterCASConflict(t *testing.T) {
	start := time.Unix(3200, 0).UTC()
	clockSource := clock.NewFake(start)
	backend := store.NewMemory(clockSource)
	members := make([]store.Member, 0, 4)
	for index := range 4 {
		member := store.Member{
			NodeAddr:   "node-" + string(rune('a'+index)),
			Generation: "generation",
			Status:     store.MemberActive,
			IamAliveAt: start,
		}
		members = append(members, member)
		if _, err := backend.WriteMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := MemberID{NodeAddr: members[0].NodeAddr, Generation: members[0].Generation}
	neighbors := targetNeighbors(snapshot.Members, target)
	self := neighbors[0]
	other := neighbors[1]
	row := snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	table := &conflictingVoteStore{backend: backend, target: target, other: other}
	node := &Node{
		table:   table,
		ctx:     context.Background(),
		voteTTL: time.Second,
	}
	node.updateSuspectVote(target, self, true)

	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	row = snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	if len(row.SuspectVotes) != 2 {
		t.Fatalf("votes after conflict merge = %#v, want concurrent vote preserved", row.SuspectVotes)
	}
	if _, ok := row.SuspectVotes[other]; !ok {
		t.Fatalf("votes after conflict merge = %#v, want vote from %v", row.SuspectVotes, other)
	}
	if _, ok := row.SuspectVotes[self]; !ok {
		t.Fatalf("votes after conflict merge = %#v, want own vote", row.SuspectVotes)
	}
}

func TestUnhealthyNodeDoesNotVoteOrJudge(t *testing.T) {
	start := time.Unix(3300, 0).UTC()
	clockSource := clock.NewFake(start)
	backend := store.NewMemory(clockSource)
	members := []store.Member{
		{NodeAddr: "node-a", Generation: "generation-a", Status: store.MemberActive, IamAliveAt: start},
		{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive, IamAliveAt: start},
		{NodeAddr: "node-c", Generation: "generation-c", Status: store.MemberActive, IamAliveAt: start},
	}
	for _, member := range members {
		if _, err := backend.WriteMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := MemberID{NodeAddr: "node-a", Generation: "generation-a"}
	row := snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	neighbors := targetNeighbors(snapshot.Members, target)
	self := MemberID{NodeAddr: "node-b", Generation: "generation-b"}
	var other MemberID
	for _, neighbor := range neighbors {
		if neighbor != self {
			other = neighbor
			break
		}
	}
	row.SuspectVotes = map[MemberID]store.SuspectVote{
		other: {ExpiresAt: start.Add(time.Second)},
	}
	if _, err := backend.WriteMember(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	node := &Node{
		table:             backend,
		ctx:               context.Background(),
		health:            unhealthy,
		probeFailures:     map[MemberID]int{target: 2},
		probeFailureLimit: 3,
		voteTTL:           time.Second,
	}
	node.handleProbeEvent(probeEvent{
		target: target,
		token:  1,
		result: ProbeResult{Err: errors.New("probe failed")},
	}, map[MemberID]probeTask{target: {token: 1}}, self)

	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	row = snapshot.Members[memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})]
	if row.Status != store.MemberActive {
		t.Fatalf("unhealthy node changed target status to %s", row.Status)
	}
	if len(row.SuspectVotes) != 1 {
		t.Fatalf("unhealthy node changed votes = %#v", row.SuspectVotes)
	}
	if len(node.probeFailures) != 0 {
		t.Fatalf("unhealthy node retained failures = %#v", node.probeFailures)
	}
}

type conflictingVoteStore struct {
	backend  store.MemberStore
	target   MemberID
	other    MemberID
	injected bool
}

func (s *conflictingVoteStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	if !s.injected && member.NodeAddr == s.target.NodeAddr && member.Generation == s.target.Generation {
		s.injected = true
		snapshot, err := s.backend.ListMembers(ctx)
		if err != nil {
			return 0, err
		}
		index := memberIndex(snapshot.Members, store.Member{NodeAddr: s.target.NodeAddr, Generation: s.target.Generation})
		row := snapshot.Members[index]
		row.SuspectVotes = map[MemberID]store.SuspectVote{
			s.other: {ExpiresAt: snapshot.TableNow.Add(time.Second)},
		}
		if _, err := s.backend.WriteMember(ctx, row); err != nil {
			return 0, err
		}
		return 0, store.ErrConflict
	}
	return s.backend.WriteMember(ctx, member)
}

func (s *conflictingVoteStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	return s.backend.ListMembers(ctx)
}

var _ store.MemberStore = (*conflictingVoteStore)(nil)

func TestNodeStopsAfterSuccessfulProbeWhenSelfIsDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(3400, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory(fakeClock)
		other := store.Member{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive, IamAliveAt: start}
		if _, err := backend.WriteMember(context.Background(), other); err != nil {
			t.Fatal(err)
		}
		prober := &nodeProber{calls: make(chan nodeProbeCall, 2)}
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		config.Prober = prober
		config.HeartbeatInterval = time.Hour
		config.ViewInterval = time.Hour
		config.ProbeInterval = time.Second
		config.ProbeTimeout = 500 * time.Millisecond
		node, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		<-node.ViewChanges()
		synctest.Wait()

		fakeClock.Advance(time.Second)
		synctest.Wait()
		calls := readProbeCalls(t, prober, 1)
		self := findTestMember(t, backend, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := backend.WriteMember(context.Background(), self); err != nil {
			t.Fatal(err)
		}
		calls[0].replies <- ProbeResult{ID: calls[0].target}
		synctest.Wait()

		fakeClock.Advance(time.Second)
		synctest.Wait()
		select {
		case <-node.Done():
		default:
			t.Fatal("node stayed alive after a successful probe with a dead self row")
		}
	})
}
