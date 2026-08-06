//go:build sim

package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sort"
	"sync/atomic"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

const clusterNodeCount = 2

type clusterNode struct {
	id int
	rt *gor.Runtime
}

type simulationCluster struct {
	backend     *fakeStore
	tracker     *timerTracker
	clock       *clock.Fake
	generations []int
	// driverLive is the liveness the decision encoding reads: nodes the
	// driver crashed, left, or failed to restart. Crash and leave are
	// decisions; restart-success is an observation pinned to the seed by the
	// member-fault target binding, so this set is a pure function of the
	// seed. Cluster-declared death (probe/vote) does not move it — that is
	// an observation, and letting it feed the decision half would split the
	// decision lines again. clusterLiveNodeIDs is the cluster's own view for
	// the invariant side; the two routinely disagree after a vote.
	driverLive        []bool
	nodes             []*clusterNode
	network           *simulationNetwork
	operationSequence atomic.Int64
}

func newSimulationCluster(backend *fakeStore, count int, tracker *timerTracker) (*simulationCluster, error) {
	cluster := &simulationCluster{
		backend:     backend,
		tracker:     tracker,
		clock:       clock.NewFake(time.Unix(0, 0)),
		generations: make([]int, count),
		driverLive:  make([]bool, count),
		nodes:       make([]*clusterNode, count),
		network:     newSimulationNetwork(backend),
	}
	for id := range cluster.driverLive {
		cluster.driverLive[id] = true
	}
	cluster.backend.setMemberClock(cluster.clock)
	for id := range cluster.nodes {
		rt, err := cluster.newRuntime(id, 0)
		if err != nil {
			cluster.close()
			return nil, err
		}
		cluster.nodes[id] = &clusterNode{id: id, rt: rt}
	}
	return cluster, nil
}

func (c *simulationCluster) newRuntime(id, generation int) (*gor.Runtime, error) {
	addr := fmt.Sprintf("node-%d", id)
	members, network := c.network.addNode(addr)
	return newCounterRuntimeWithOptions(
		c.backend,
		c.tracker,
		gor.WithClock(c.clock),
		gor.WithMemberStore(members),
		gor.WithScheduleStore(&nodeScheduleStore{backend: c.backend, addr: addr}),
		gor.WithNodeAddr(addr),
		gor.WithGeneration(memberGeneration(id, generation)),
		gor.WithHeartbeatInterval(simulationStepDuration),
		gor.WithViewInterval(simulationStepDuration),
		gor.WithProbeInterval(simulationStepDuration),
		gor.WithProbeTimeout(simulationStepDuration/2),
		gor.WithProbeFailures(3),
		gor.WithVoteTTL(6*simulationStepDuration),
		gor.WithMaxTickGap(2*simulationStepDuration),
		gor.WithMaxTableLatency(simulationStepDuration/2),
		gor.WithIdleTimeout(2*simulationStepDuration),
		gor.WithEvictionInterval(simulationStepDuration),
		gor.WithTransport(network),
	)
}

func (c *simulationCluster) close() {
	for _, node := range c.nodes {
		if node.rt != nil {
			node.rt.Close()
			node.rt = nil
		}
	}
}

func (c *simulationCluster) crash(id int) error {
	node, err := c.node(id)
	if err != nil {
		return err
	}
	if node.rt == nil {
		return fmt.Errorf("node %d is already stopped", id)
	}
	node.rt.Kill()
	node.rt = nil
	c.driverLive[id] = false
	return nil
}

func (c *simulationCluster) leave(id int) error {
	node, err := c.node(id)
	if err != nil {
		return err
	}
	if node.rt == nil {
		return fmt.Errorf("node %d is already stopped", id)
	}
	done := make(chan struct{})
	go func() {
		node.rt.Close()
		close(done)
	}()
	c.advance(10 * simulationStepDuration)
	<-done
	node.rt = nil
	c.driverLive[id] = false
	return nil
}

func (c *simulationCluster) restart(id int) error {
	node, err := c.node(id)
	if err != nil {
		return err
	}
	if node.rt != nil {
		if !runtimeStopped(node.rt) {
			return fmt.Errorf("node %d is already running", id)
		}
		node.rt.Close()
		node.rt = nil
	}
	generation := c.generations[id] + 1
	c.generations[id] = generation
	result := make(chan struct {
		rt  *gor.Runtime
		err error
	}, 1)
	go func() {
		rt, err := c.newRuntime(id, generation)
		result <- struct {
			rt  *gor.Runtime
			err error
		}{rt: rt, err: err}
	}()
	c.advance(10 * simulationStepDuration)
	created := <-result
	if created.err != nil {
		c.driverLive[id] = false
		return created.err
	}
	node.rt = created.rt
	c.driverLive[id] = true
	return nil
}

func memberGeneration(id, generation int) string {
	return fmt.Sprintf("generation-%d-%d", id, generation)
}

func (c *simulationCluster) advance(duration time.Duration) {
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

func (c *simulationCluster) settle() {
	c.backend.setFaultPlans(nil)
	c.backend.setMemberFault(memberFaultSpec{})
	c.network.setDelays(nil)
	c.network.setDrops(networkDropSpec{})
	for range 20 {
		c.advance(simulationStepDuration)
	}
}

func (c *simulationCluster) partition(groups map[int]int) error {
	addressGroups := make(map[string]int, len(groups))
	for id, group := range groups {
		node, err := c.node(id)
		if err != nil {
			return err
		}
		if node.rt == nil || runtimeStopped(node.rt) {
			return fmt.Errorf("cannot partition stopped node %d", id)
		}
		addressGroups[fmt.Sprintf("node-%d", id)] = group
	}
	if len(addressGroups) != len(c.clusterLiveNodeIDs()) {
		return fmt.Errorf("partition groups cover %d nodes, want %d", len(addressGroups), len(c.clusterLiveNodeIDs()))
	}
	return c.network.partition(addressGroups)
}

func (c *simulationCluster) heal() {
	c.network.heal()
}

func (c *simulationCluster) checkInvariants(ids []store.Identity) error {
	if err := c.backend.checkMemberStatuses(); err != nil {
		return err
	}
	// The invariant side reads cluster liveness, not the driver's. Owner
	// uniqueness is an observation: a cluster-declared-dead node must not be
	// counted, and a cluster-dead-but-driver-live node read through the
	// driver set would contribute its stale last view and raise a false red.
	liveIDs := c.clusterLiveNodeIDs()
	if len(liveIDs) == 0 {
		return nil
	}
	for _, id := range ids {
		owners := 0
		for _, nodeID := range liveIDs {
			if c.nodes[nodeID].rt.Owns(id) {
				owners++
			}
		}
		if owners != 1 {
			return fmt.Errorf("identity %s/%s has %d live owners after settle", id.Type, id.Key, owners)
		}
	}
	return nil
}

func (c *simulationCluster) node(id int) (*clusterNode, error) {
	if id < 0 || id >= len(c.nodes) {
		return nil, fmt.Errorf("unknown node %d", id)
	}
	return c.nodes[id], nil
}

// liveNodeIDs is the decision half's liveness reader: nodes the driver has
// not crashed, left, or failed to restart. It never consults the runtime;
// cluster-declared death is an observation and may not feed the decision
// half. The invariant half reads clusterLiveNodeIDs instead.
func (c *simulationCluster) liveNodeIDs() []int {
	ids := make([]int, 0, len(c.nodes))
	for _, node := range c.nodes {
		if c.driverLive[node.id] {
			ids = append(ids, node.id)
		}
	}
	return ids
}

// clusterLiveNodeIDs is the invariant half's liveness reader: nodes whose
// runtime still runs, in the cluster's own view. It is the observation the
// probe/vote mechanism produces, so it may move with scheduling.
func (c *simulationCluster) clusterLiveNodeIDs() []int {
	ids := make([]int, 0, len(c.nodes))
	for _, node := range c.nodes {
		if node.rt != nil && !runtimeStopped(node.rt) {
			ids = append(ids, node.id)
		}
	}
	return ids
}

// targetPool returns the nodes a fault target may draw this step — the
// driver's live nodes plus the node being restarted this step. The pool is
// the driver's own model (liveNodeIDs), so the draw is a pure function of the
// seed. A cluster-dead-but-driver-live node stays in the pool and rejects at
// admission, so a fault drawn against it may not fire this step — the
// accepted cost of keeping the decision half pure.
func (c *simulationCluster) targetPool(restartNode int) []int {
	pool := c.liveNodeIDs()
	if restartNode >= 0 {
		pool = append(pool, restartNode)
	}
	return pool
}

func (c *simulationCluster) stoppedNodeIDs() []int {
	ids := make([]int, 0, len(c.nodes))
	for _, node := range c.nodes {
		if !c.driverLive[node.id] {
			ids = append(ids, node.id)
		}
	}
	return ids
}

func runtimeStopped(rt *gor.Runtime) bool {
	select {
	case <-rt.Done():
		return true
	default:
		return false
	}
}

type clusterAction uint8

const (
	clusterCall clusterAction = iota
	clusterCrash
	clusterRestart
	clusterLeave
	clusterSchedule
	clusterDisarm
)

func chooseClusterAction(rng *rand.Rand, cluster *simulationCluster) clusterAction {
	liveNodeIDs := cluster.liveNodeIDs()
	actions := make([]clusterAction, 0, 5)
	if len(liveNodeIDs) > 0 {
		actions = append(actions, clusterCall, clusterCrash, clusterLeave, clusterSchedule, clusterDisarm)
	}
	if len(cluster.stoppedNodeIDs()) > 0 {
		actions = append(actions, clusterRestart)
	}
	return actions[rng.IntN(len(actions))]
}

type decision struct {
	rt    *gor.Runtime
	id    gor.Identity
	delta int64
}

type invocationResult struct {
	id        gor.Identity
	operation porcupine.Operation
	err       error
}

func executeDecisions(cluster *simulationCluster, decisions []decision, crashNode *int, history *counterHistory) ([]string, error) {
	results := make(chan invocationResult, len(decisions))
	for _, selected := range decisions {
		selected := selected
		go func() {
			call := cluster.operationSequence.Add(1)
			var reply counterAddReply
			err := selected.rt.Invoke(context.Background(), selected.id, "Add", &counterAddRequest{A0: selected.delta}, &reply)
			results <- invocationResult{
				id: selected.id,
				operation: porcupine.Operation{
					Input:  selected.delta,
					Call:   call,
					Output: counterOperationOutputFor(reply.R0, err),
					Return: cluster.operationSequence.Add(1),
				},
				err: err,
			}
		}()
	}
	synctest.Wait()
	if crashNode != nil {
		if err := cluster.crash(*crashNode); err != nil {
			return nil, err
		}
	}
	synctest.Wait()
	// Kill lets the caller return while the entity method is still sleeping in
	// the store; if root exits, the fake clock stops and synctest reports a leak.
	cluster.backend.waitForIdle()
	cluster.network.waitForIdle()

	outcomes := make([]string, 0, len(decisions))
	for range decisions {
		result := <-results
		history.add(storeIdentity(result.id), result.operation)
		outcome, err := classifyOutcome(result.err)
		if err != nil {
			return nil, err
		}
		outcomes = append(outcomes, outcome)
	}
	sort.Strings(outcomes)
	return outcomes, nil
}
