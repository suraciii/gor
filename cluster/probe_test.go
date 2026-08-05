package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

func TestNewRequiresProber(t *testing.T) {
	_, err := New(Config{
		Table: store.NewMemory(),
		Clock: clock.NewFake(time.Unix(1900, 0).UTC()),
	})
	if !errors.Is(err, ErrProberRequired) {
		t.Fatalf("New error = %v, want %v", err, ErrProberRequired)
	}
}

func TestProbeTargetsKeepTwoNeighborsAsClusterGrows(t *testing.T) {
	self := MemberID{NodeAddr: "node-00", Generation: "generation-00"}
	for _, count := range []int{1, 2, 3, 30} {
		members := make([]store.Member, 0, count)
		for index := 0; index < count; index++ {
			members = append(members, store.Member{
				NodeAddr:   fmt.Sprintf("node-%02d", index),
				Generation: fmt.Sprintf("generation-%02d", index),
				Status:     store.MemberActive,
			})
		}
		targets := probeTargets(activeMemberIDs(members), self)
		want := 0
		if count == 2 {
			want = 1
		}
		if count >= 3 {
			want = 2
		}
		if len(targets) != want {
			t.Fatalf("active members=%d targets=%v, want %d", count, targets, want)
		}
		if count == 30 {
			wantTargets := expectedProbeTargets(members, self)
			if !slices.Equal(targets, wantTargets) {
				t.Fatalf("active members=%d targets=%v, want ring neighbors %v", count, targets, wantTargets)
			}
		}
	}
}

func expectedProbeTargets(snapshot []store.Member, self MemberID) []MemberID {
	ids := make([]MemberID, 0, len(snapshot))
	for _, member := range snapshot {
		if member.Status == store.MemberActive {
			ids = append(ids, MemberID{NodeAddr: member.NodeAddr, Generation: member.Generation})
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		leftHash := hashParts(ids[i].NodeAddr, ids[i].Generation)
		rightHash := hashParts(ids[j].NodeAddr, ids[j].Generation)
		if leftHash != rightHash {
			return leftHash < rightHash
		}
		if ids[i].NodeAddr != ids[j].NodeAddr {
			return ids[i].NodeAddr < ids[j].NodeAddr
		}
		return ids[i].Generation < ids[j].Generation
	})
	selfIndex := slices.Index(ids, self)
	next := ids[(selfIndex+1)%len(ids)]
	previous := ids[(selfIndex+len(ids)-1)%len(ids)]
	if next == previous {
		return []MemberID{next}
	}
	return []MemberID{next, previous}
}

func TestProbeTargetsSortHashTiesByMemberID(t *testing.T) {
	members := []MemberID{
		{NodeAddr: "node-b", Generation: "generation-2"},
		{NodeAddr: "node-a", Generation: "generation-2"},
		{NodeAddr: "node-a", Generation: "generation-1"},
	}
	points := []probePoint{
		{hash: 7, id: members[0]},
		{hash: 7, id: members[1]},
		{hash: 7, id: members[2]},
	}
	sortProbePoints(points)
	want := []MemberID{members[2], members[1], members[0]}
	got := make([]MemberID, 0, len(points))
	for _, point := range points {
		got = append(got, point.id)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tie order = %v, want %v", got, want)
	}
}

func TestReconcileProbeStateCancelsRemovedNeighborAndDropsFailures(t *testing.T) {
	oldTarget := MemberID{NodeAddr: "node-old", Generation: "generation-old"}
	newTarget := MemberID{NodeAddr: "node-new", Generation: "generation-new"}
	canceled := false
	tasks := map[MemberID]probeTask{
		oldTarget: {cancel: func() { canceled = true }, token: 1},
	}
	failures := map[MemberID]int{oldTarget: 3}

	reconcileProbeState([]MemberID{newTarget}, tasks, failures)

	if !canceled {
		t.Fatal("removed neighbor probe was not canceled")
	}
	if _, ok := tasks[oldTarget]; ok {
		t.Fatal("removed neighbor probe remained in flight")
	}
	if _, ok := failures[oldTarget]; ok {
		t.Fatal("removed neighbor failure count was retained")
	}
	if _, ok := failures[newTarget]; ok {
		t.Fatal("new neighbor inherited a failure count")
	}
}

func TestRecordProbeEventDropsStaleTokenAndCanceledTarget(t *testing.T) {
	oldTarget := MemberID{NodeAddr: "node-old", Generation: "generation-old"}
	newTarget := MemberID{NodeAddr: "node-new", Generation: "generation-new"}
	tasks := map[MemberID]probeTask{
		oldTarget: {token: 2, cancel: func() {}},
	}
	failures := map[MemberID]int{oldTarget: 4}

	recordProbeEvent(probeEvent{
		target: oldTarget,
		token:  1,
		result: ProbeResult{ID: oldTarget},
	}, tasks, failures)
	if got := failures[oldTarget]; got != 4 {
		t.Fatalf("stale probe changed failure count to %d, want 4", got)
	}

	reconcileProbeState([]MemberID{newTarget}, tasks, failures)
	recordProbeEvent(probeEvent{
		target: oldTarget,
		token:  2,
		result: ProbeResult{ID: oldTarget},
	}, tasks, failures)
	if _, ok := failures[oldTarget]; ok {
		t.Fatal("late canceled probe recreated a failure count")
	}
}

func TestNodeProbeStateMachineTracksNeighborsAndExactReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(2200, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		members := []store.Member{
			{NodeAddr: "node-a", Generation: "generation-a", Status: store.MemberActive, IamAliveAt: start},
			{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive, IamAliveAt: start},
			{NodeAddr: "node-c", Generation: "generation-c", Status: store.MemberActive, IamAliveAt: start},
		}
		for _, member := range members[1:] {
			if _, err := backend.WriteMember(context.Background(), member); err != nil {
				t.Fatalf("seed member: %v", err)
			}
		}
		prober := &nodeProber{calls: make(chan nodeProbeCall, 4)}
		node, err := New(Config{
			Table:             backend,
			Clock:             fakeClock,
			Prober:            prober,
			NodeAddr:          "node-a",
			Generation:        "generation-a",
			HeartbeatInterval: time.Hour,
			ViewInterval:      time.Hour,
			ProbeInterval:     time.Second,
			ProbeTimeout:      500 * time.Millisecond,
			ProbeFailures:     3,
			VoteTTL:           6 * time.Second,
			MaxTickGap:        2 * time.Second,
			MaxTableLatency:   500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer node.Close()
		<-node.ViewChanges()
		synctest.Wait()

		fakeClock.Advance(time.Second)
		synctest.Wait()
		calls := readProbeCalls(t, prober, 2)
		wantTargets := probeTargets(activeMemberIDs(members), MemberID{NodeAddr: "node-a", Generation: "generation-a"})
		assertProbeTargets(t, calls, wantTargets)

		fakeClock.Advance(500 * time.Millisecond)
		synctest.Wait()
		for _, target := range wantTargets {
			if got := node.probeFailures[target]; got != 1 {
				t.Fatalf("timeout failure count for %#v = %d, want 1", target, got)
			}
		}

		fakeClock.Advance(500 * time.Millisecond)
		synctest.Wait()
		calls = readProbeCalls(t, prober, 2)
		assertProbeTargets(t, calls, wantTargets)
		wrong := calls[0]
		wrong.replies <- ProbeResult{ID: MemberID{NodeAddr: wrong.target.NodeAddr, Generation: "generation-new"}}
		calls[1].replies <- ProbeResult{ID: calls[1].target}
		synctest.Wait()
		if got := node.probeFailures[wrong.target]; got != 2 {
			t.Fatalf("mismatched generation failure count = %d, want 2", got)
		}
		if got := node.probeFailures[calls[1].target]; got != 0 {
			t.Fatalf("matching reply failure count = %d, want 0", got)
		}

		fakeClock.Advance(time.Second)
		synctest.Wait()
		for _, call := range readProbeCalls(t, prober, 2) {
			call.replies <- ProbeResult{ID: call.target}
		}
		synctest.Wait()
		for _, target := range wantTargets {
			if got := node.probeFailures[target]; got != 0 {
				t.Fatalf("successful reply failure count for %#v = %d, want 0", target, got)
			}
		}
	})
}

func assertProbeTargets(t *testing.T, calls []nodeProbeCall, want []MemberID) {
	t.Helper()
	got := make([]MemberID, 0, len(calls))
	for _, call := range calls {
		got = append(got, call.target)
	}
	sort.Slice(got, func(i, j int) bool { return memberIDLess(got[i], got[j]) })
	want = append([]MemberID(nil), want...)
	sort.Slice(want, func(i, j int) bool { return memberIDLess(want[i], want[j]) })
	if !slices.Equal(got, want) {
		t.Fatalf("probe targets = %v, want %v", got, want)
	}
}

func TestWaitForProbeCancelsProberWhenClockTimeoutFires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(2000, 0).UTC()
		fakeClock := clock.NewFake(start)
		prober := &blockingProber{contexts: make(chan context.Context, 1), replies: make(chan ProbeResult)}
		target := MemberID{NodeAddr: "node-b", Generation: "generation-b"}
		resultDone := make(chan ProbeResult, 1)
		go func() {
			resultDone <- waitForProbe(context.Background(), fakeClock, prober, target, time.Second)
		}()
		synctest.Wait()

		probeContext := <-prober.contexts
		fakeClock.Advance(time.Second)
		synctest.Wait()
		result := <-resultDone
		if !errors.Is(result.Err, ErrProbeTimeout) {
			t.Fatalf("probe result error = %v, want timeout", result.Err)
		}
		select {
		case <-probeContext.Done():
		default:
			t.Fatal("probe context remained active after timeout")
		}
	})
}

func TestWaitForProbeStopsOnCloseSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClock := clock.NewFake(time.Unix(2100, 0).UTC())
		prober := &blockingProber{contexts: make(chan context.Context, 1), replies: make(chan ProbeResult)}
		ctx, cancel := context.WithCancel(context.Background())
		resultDone := make(chan ProbeResult, 1)
		go func() {
			resultDone <- waitForProbe(ctx, fakeClock, prober, MemberID{NodeAddr: "node-b", Generation: "generation-b"}, time.Second)
		}()
		synctest.Wait()
		probeContext := <-prober.contexts
		cancel()
		synctest.Wait()
		result := <-resultDone
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("probe result error = %v, want context canceled", result.Err)
		}
		select {
		case <-probeContext.Done():
		default:
			t.Fatal("probe context remained active after close signal")
		}
	})
}

type blockingProber struct {
	contexts chan context.Context
	replies  chan ProbeResult
}

func (p *blockingProber) Probe(ctx context.Context, _ MemberID) <-chan ProbeResult {
	p.contexts <- ctx
	return p.replies
}

type nodeProbeCall struct {
	target  MemberID
	replies chan ProbeResult
}

type nodeProber struct {
	calls chan nodeProbeCall
}

func (p *nodeProber) Probe(_ context.Context, target MemberID) <-chan ProbeResult {
	replies := make(chan ProbeResult, 1)
	p.calls <- nodeProbeCall{target: target, replies: replies}
	return replies
}

func readProbeCalls(t *testing.T, prober *nodeProber, count int) []nodeProbeCall {
	t.Helper()
	calls := make([]nodeProbeCall, 0, count)
	for range count {
		calls = append(calls, <-prober.calls)
	}
	return calls
}
