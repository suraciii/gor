//go:build sim

package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

type probeScenario struct {
	backend *fakeStore
	tracker *timerTracker
	clock   *clock.Fake
	network *simulationNetwork
	voteTTL time.Duration
	nodes   []*probeScenarioNode
	rules   []*probeBlockRule
}

type probeScenarioNode struct {
	rt        *gor.Runtime
	transport *probeScenarioTransport
}

type probeBlockRule struct {
	predicate func(string, string) bool
	enabled   atomic.Bool
}

type probeScenarioTransport struct {
	base     *simulationTransport
	scenario *probeScenario
	probes   atomic.Int64
}

func newProbeScenario(t *testing.T, count int, frozen map[int]time.Time) *probeScenario {
	return newProbeScenarioWithOptions(t, count, frozen, 6*simulationStepDuration, nil)
}

func newProbeScenarioWithOptions(t *testing.T, count int, frozen map[int]time.Time, voteTTL time.Duration, wrapMemberStore func(store.MemberStore) store.MemberStore) *probeScenario {
	t.Helper()
	tracker := newTimerTracker()
	backend := newFakeStore(tracker)
	sourceClock := clock.NewFake(time.Unix(10000, 0).UTC())
	backend.setMemberClock(sourceClock)
	scenario := &probeScenario{
		backend: backend,
		tracker: tracker,
		clock:   sourceClock,
		network: newSimulationNetwork(backend),
		voteTTL: voteTTL,
		nodes:   make([]*probeScenarioNode, count),
	}
	for id := range scenario.nodes {
		memberStore := store.MemberStore(backend)
		if wrapMemberStore != nil {
			memberStore = wrapMemberStore(memberStore)
		}
		if frozenAt, ok := frozen[id]; ok {
			memberStore = &frozenMemberStore{
				backend:    memberStore,
				nodeAddr:   fmt.Sprintf("node-%d", id),
				generation: memberGeneration(id, 0),
				frozenAt:   frozenAt,
			}
		}
		node, err := scenario.addRuntime(id, 0, memberStore)
		if err != nil {
			scenario.close()
			t.Fatalf("start node %d: %v", id, err)
		}
		scenario.nodes[id] = node
	}
	return scenario
}

func (c *probeScenario) addRuntime(id, generation int, memberStore store.MemberStore) (*probeScenarioNode, error) {
	addr := fmt.Sprintf("node-%d", id)
	_, endpoint := c.network.addNode(addr)
	probeTransport := &probeScenarioTransport{base: endpoint, scenario: c}
	rt, err := newCounterRuntimeWithOptions(
		c.backend,
		c.tracker,
		gor.WithClock(c.clock),
		gor.WithMemberStore(memberStore),
		gor.WithNodeAddr(addr),
		gor.WithGeneration(memberGeneration(id, generation)),
		gor.WithHeartbeatInterval(time.Hour),
		gor.WithViewInterval(simulationStepDuration),
		gor.WithProbeInterval(simulationStepDuration),
		gor.WithProbeTimeout(simulationStepDuration/2),
		gor.WithProbeFailures(3),
		gor.WithVoteTTL(c.voteTTL),
		gor.WithMaxTickGap(2*simulationStepDuration),
		gor.WithMaxTableLatency(simulationStepDuration/2),
		gor.WithTransport(probeTransport),
	)
	if err != nil {
		return nil, err
	}
	return &probeScenarioNode{rt: rt, transport: probeTransport}, nil
}

func (c *probeScenario) addRule(predicate func(string, string) bool) *probeBlockRule {
	rule := &probeBlockRule{predicate: predicate}
	c.rules = append(c.rules, rule)
	return rule
}

func (c *probeScenario) blocked(source, destination string) bool {
	for _, rule := range c.rules {
		if rule.enabled.Load() && rule.predicate(source, destination) {
			return true
		}
	}
	return false
}

func (c *probeScenario) advance(duration time.Duration) {
	for duration > 0 {
		step := simulationStepDuration
		if duration < step {
			step = duration
		}
		c.clock.Advance(step)
		runtime.Gosched()
		synctest.Wait()
		c.backend.waitForIdle()
		c.network.waitForIdle()
		duration -= step
	}
	synctest.Wait()
}

// advanceClock advances the injected clock while keeping the bubble clock
// frozen. A helper goroutine steps the clock and yields generously while the
// driver blocks on its completion: the cluster's run loops keep processing
// probe ticks (so they stay healthy and record failures), while the spinning
// helper keeps the bubble from advancing, so held deliveries stay held and
// probe timeouts fire while their messages are still in the network. The
// driver must settle before observing.
func (c *probeScenario) advanceClock(duration time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for duration > 0 {
			step := simulationStepDuration
			if duration < step {
				step = duration
			}
			c.clock.Advance(step)
			for range 2000 {
				runtime.Gosched()
			}
			duration -= step
		}
	}()
	<-done
}

func (c *probeScenario) close() {
	for _, node := range c.nodes {
		if node != nil && node.rt != nil {
			node.rt.Close()
			node.rt = nil
		}
	}
}

func (t *probeScenarioTransport) Addr() string {
	return t.base.Addr()
}

func (t *probeScenarioTransport) Serve(ctx context.Context, handler transport.Handler) error {
	return t.base.Serve(ctx, handler)
}

func (t *probeScenarioTransport) Send(ctx context.Context, addr string, payload []byte) ([]byte, error) {
	var request struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(payload, &request); err == nil && request.Kind == "probe" {
		t.probes.Add(1)
	}
	if t.scenario.blocked(t.base.Addr(), addr) {
		return nil, errSimNetworkPartition
	}
	return t.base.Send(ctx, addr, payload)
}

func (t *probeScenarioTransport) Close() error {
	return t.base.Close()
}

func (t *probeScenarioTransport) Kill() error {
	return t.base.Kill()
}

type frozenMemberStore struct {
	backend    store.MemberStore
	nodeAddr   string
	generation string
	frozenAt   time.Time
}

type phaseDeadWriteGateStore struct {
	backend  store.MemberStore
	blocking atomic.Bool
	attempts atomic.Int64
	release  chan struct{}
}

func (s *phaseDeadWriteGateStore) unblock() {
	s.blocking.Store(false)
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

func (s *phaseDeadWriteGateStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	if member.Status == store.MemberDead && s.blocking.Load() {
		s.attempts.Add(1)
		<-s.release
		current, err := s.backend.ListMembers(ctx)
		if err != nil {
			return 0, err
		}
		for _, row := range current.Members {
			if row.NodeAddr == member.NodeAddr && row.Generation == member.Generation {
				return row.ETag, nil
			}
		}
		return 0, store.ErrConflict
	}
	return s.backend.WriteMember(ctx, member)
}

func (s *phaseDeadWriteGateStore) releaseAll(ctx context.Context) error {
	defer s.unblock()
	snapshot, err := s.backend.ListMembers(ctx)
	if err != nil {
		return err
	}
	for _, member := range snapshot.Members {
		if member.Status != store.MemberActive {
			continue
		}
		member.Status = store.MemberDead
		if _, err := s.backend.WriteMember(ctx, member); err != nil {
			return err
		}
	}
	return nil
}

func (s *phaseDeadWriteGateStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	return s.backend.ListMembers(ctx)
}

func (s *frozenMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	if member.NodeAddr == s.nodeAddr && member.Generation == s.generation {
		member.IamAliveAt = s.frozenAt
	}
	return s.backend.WriteMember(ctx, member)
}

func (s *frozenMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	return s.backend.ListMembers(ctx)
}

func scenarioMember(t *testing.T, scenario *probeScenario, nodeAddr, generation string) store.Member {
	t.Helper()
	snapshot, err := scenario.backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range snapshot.Members {
		if member.NodeAddr == nodeAddr && member.Generation == generation {
			return member
		}
	}
	t.Fatalf("member %s/%s not found in %#v", nodeAddr, generation, snapshot.Members)
	return store.Member{}
}

func hasSuspectVote(member store.Member, voter string) bool {
	for id := range member.SuspectVotes {
		if id.NodeAddr == voter {
			return true
		}
	}
	return false
}

func TestSim_ProbeIsolationVotesHealthyNodeDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scenario := newProbeScenario(t, 3, nil)
		defer scenario.close()
		scenario.advance(2 * simulationStepDuration)

		target := "node-2"
		isolate := scenario.addRule(func(source, destination string) bool {
			return source == target || destination == target
		})
		isolate.enabled.Store(true)
		scenario.advance(10 * simulationStepDuration)

		member := scenarioMember(t, scenario, target, memberGeneration(2, 0))
		if member.Status != store.MemberDead {
			t.Fatalf("isolated target status = %s, want dead after neighbor votes", member.Status)
		}
		if len(member.SuspectVotes) != 2 {
			t.Fatalf("isolated target votes = %#v, want both neighbors", member.SuspectVotes)
		}
		select {
		case <-scenario.nodes[2].rt.Done():
		default:
			t.Fatal("isolated target Done channel remained open")
		}
	})
}

func TestSim_ExpiredProbeVoteDoesNotKillHealthyNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scenario := newProbeScenario(t, 4, nil)
		defer scenario.close()
		scenario.advance(2 * simulationStepDuration)

		target := "node-0"
		var oldVoter string
		var oldRule *probeBlockRule
		for id := 1; id < 4; id++ {
			voter := fmt.Sprintf("node-%d", id)
			rule := scenario.addRule(func(source, destination string) bool {
				return source == voter && destination == target
			})
			rule.enabled.Store(true)
			scenario.advance(4 * simulationStepDuration)
			member := scenarioMember(t, scenario, target, memberGeneration(0, 0))
			if hasSuspectVote(member, voter) {
				oldVoter = voter
				oldRule = rule
				break
			}
			rule.enabled.Store(false)
		}
		if oldVoter == "" {
			t.Fatal("no target neighbor left a suspect vote")
		}
		oldRule.enabled.Store(true)
		oldID := 0
		fmt.Sscanf(oldVoter, "node-%d", &oldID)
		scenario.nodes[oldID].rt.Kill()
		scenario.nodes[oldID].rt = nil
		scenario.advance(7 * simulationStepDuration)

		member := scenarioMember(t, scenario, target, memberGeneration(0, 0))
		if hasSuspectVote(member, oldVoter) {
			t.Fatalf("expired vote from %s remained in target row: %#v", oldVoter, member.SuspectVotes)
		}

		var newVoter string
		for id := 1; id < 4; id++ {
			voter := fmt.Sprintf("node-%d", id)
			if voter == oldVoter {
				continue
			}
			rule := scenario.addRule(func(source, destination string) bool {
				return source == voter && destination == target
			})
			rule.enabled.Store(true)
			scenario.advance(4 * simulationStepDuration)
			member = scenarioMember(t, scenario, target, memberGeneration(0, 0))
			if hasSuspectVote(member, voter) {
				newVoter = voter
				break
			}
			rule.enabled.Store(false)
		}
		if newVoter == "" {
			t.Fatal("no current target neighbor produced the second vote")
		}
		if member.Status != store.MemberActive {
			t.Fatalf("target status with one fresh vote = %s, want active", member.Status)
		}
		if len(member.SuspectVotes) != 1 {
			t.Fatalf("target votes after expiry = %#v, want only %s", member.SuspectVotes, newVoter)
		}
		select {
		case <-scenario.nodes[0].rt.Done():
			t.Fatal("healthy target stopped after expired vote and one fresh vote")
		default:
		}
	})
}

func TestSim_StaleHeartbeatDoesNotOverrideSuccessfulProbes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(10000, 0).UTC()
		scenario := newProbeScenario(t, 3, map[int]time.Time{0: start})
		defer scenario.close()
		scenario.advance(12 * simulationStepDuration)

		target := scenarioMember(t, scenario, "node-0", memberGeneration(0, 0))
		probeSends := scenario.nodes[1].transport.probes.Load() + scenario.nodes[2].transport.probes.Load()
		if target.Status != store.MemberActive || !target.IamAliveAt.Equal(start) || probeSends == 0 {
			t.Fatalf("target did not stay active while direct probes succeeded: member=%#v probe-sends=%d", target, probeSends)
		}
		select {
		case <-scenario.nodes[0].rt.Done():
			t.Fatal("target Done channel closed despite successful direct probes")
		default:
		}
	})
}

func TestSim_InterleavedPartitionCanKillAllAndNewGenerationRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadWrites *phaseDeadWriteGateStore
		scenario := newProbeScenarioWithOptions(t, 4, nil, 100*simulationStepDuration, func(backend store.MemberStore) store.MemberStore {
			if deadWrites == nil {
				deadWrites = &phaseDeadWriteGateStore{backend: backend, release: make(chan struct{})}
			}
			return deadWrites
		})
		defer func() {
			deadWrites.unblock()
			scenario.close()
		}()
		scenario.advance(2 * simulationStepDuration)

		members := make([]store.MemberID, 0, 4)
		groups := make(map[string]int, 4)
		for id := range scenario.nodes {
			members = append(members, store.MemberID{
				NodeAddr:   fmt.Sprintf("node-%d", id),
				Generation: memberGeneration(id, 0),
			})
		}
		sort.Slice(members, func(left, right int) bool {
			leftHash := probeScenarioHash(members[left].NodeAddr, members[left].Generation)
			rightHash := probeScenarioHash(members[right].NodeAddr, members[right].Generation)
			if leftHash != rightHash {
				return leftHash < rightHash
			}
			if members[left].NodeAddr != members[right].NodeAddr {
				return members[left].NodeAddr < members[right].NodeAddr
			}
			return members[left].Generation < members[right].Generation
		})
		for index, member := range members {
			groups[member.NodeAddr] = index % 2
			if index > 0 && groups[members[index-1].NodeAddr] == groups[member.NodeAddr] {
				t.Fatalf("probe ring is not interleaved at %v and %v", members[index-1], member)
			}
		}
		if groups[members[0].NodeAddr] == groups[members[len(members)-1].NodeAddr] {
			t.Fatalf("probe ring wrap is not interleaved: %v", members)
		}
		groupCounts := [2]int{}
		for _, group := range groups {
			groupCounts[group]++
		}
		if groupCounts != [2]int{2, 2} {
			t.Fatalf("partition groups = %v, want 2+2", groupCounts)
		}

		partition := scenario.addRule(func(source, destination string) bool {
			return groups[source] != groups[destination]
		})
		partition.enabled.Store(true)
		deadWrites.blocking.Store(true)
		scenario.advance(8 * simulationStepDuration)
		if deadWrites.attempts.Load() != 4 {
			t.Fatalf("partition dead-write attempts = %d, want four", deadWrites.attempts.Load())
		}
		if err := deadWrites.releaseAll(context.Background()); err != nil {
			t.Fatalf("release partition death barrier: %v", err)
		}
		synctest.Wait()
		scenario.advance(2 * simulationStepDuration)

		active, err := activeScenarioMembers(scenario)
		if err != nil {
			t.Fatal(err)
		}
		if len(active) != 0 {
			t.Fatalf("active members after interleaved split = %#v, want none", active)
		}

		for id, node := range scenario.nodes {
			member := scenarioMember(t, scenario, fmt.Sprintf("node-%d", id), memberGeneration(id, 0))
			if member.Status != store.MemberDead {
				t.Fatalf("node %d status = %s, want dead after interleaved split", id, member.Status)
			}
			select {
			case <-node.rt.Done():
			default:
				t.Fatalf("node %d Done channel remained open after all-member death", id)
			}
		}
		for _, node := range scenario.nodes {
			node.rt.Close()
			node.rt = nil
		}
		replacement, err := scenario.addRuntime(0, 1, scenario.backend)
		if err != nil {
			t.Fatalf("start replacement generation: %v", err)
		}
		scenario.nodes[0] = replacement
		scenario.advance(2 * simulationStepDuration)
		snapshot, err := scenario.backend.ListMembers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		activeCount := 0
		for _, member := range snapshot.Members {
			if member.Status == store.MemberActive {
				activeCount++
				if member.NodeAddr != "node-0" || member.Generation != memberGeneration(0, 1) {
					t.Fatalf("unexpected active replacement row: %#v", member)
				}
			}
		}
		if activeCount != 1 {
			t.Fatalf("active rows after replacement = %d, want one; rows=%#v", activeCount, snapshot.Members)
		}
	})
}

func activeScenarioMembers(scenario *probeScenario) ([]store.Member, error) {
	snapshot, err := scenario.backend.ListMembers(context.Background())
	if err != nil {
		return nil, err
	}
	active := make([]store.Member, 0, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.Status == store.MemberActive {
			active = append(active, member)
		}
	}
	return active, nil
}

func probeScenarioHash(parts ...string) uint64 {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hash.Sum64()
}

var _ store.MemberStore = (*frozenMemberStore)(nil)
var _ transport.Transport = (*probeScenarioTransport)(nil)
