//go:build sim

package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/suraciii/gor"
)

const clusterNodeCount = 2

type clusterNode struct {
	id int
	rt *gor.Runtime
}

type simulationCluster struct {
	backend *fakeStore
	tracker *timerTracker
	nodes   []*clusterNode
}

func newSimulationCluster(backend *fakeStore, count int, tracker *timerTracker) (*simulationCluster, error) {
	cluster := &simulationCluster{
		backend: backend,
		tracker: tracker,
		nodes:   make([]*clusterNode, count),
	}
	for id := range cluster.nodes {
		rt, err := newCounterRuntime(backend, tracker)
		if err != nil {
			cluster.close()
			return nil, err
		}
		cluster.nodes[id] = &clusterNode{id: id, rt: rt}
	}
	return cluster, nil
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
	return nil
}

func (c *simulationCluster) restart(id int) error {
	node, err := c.node(id)
	if err != nil {
		return err
	}
	if node.rt != nil {
		return fmt.Errorf("node %d is already running", id)
	}
	rt, err := newCounterRuntime(c.backend, c.tracker)
	if err != nil {
		return err
	}
	node.rt = rt
	return nil
}

func (c *simulationCluster) node(id int) (*clusterNode, error) {
	if id < 0 || id >= len(c.nodes) {
		return nil, fmt.Errorf("unknown node %d", id)
	}
	return c.nodes[id], nil
}

func (c *simulationCluster) liveNodeIDs() []int {
	ids := make([]int, 0, len(c.nodes))
	for _, node := range c.nodes {
		if node.rt != nil {
			ids = append(ids, node.id)
		}
	}
	return ids
}

func (c *simulationCluster) stoppedNodeIDs() []int {
	ids := make([]int, 0, len(c.nodes))
	for _, node := range c.nodes {
		if node.rt == nil {
			ids = append(ids, node.id)
		}
	}
	return ids
}

type clusterAction uint8

const (
	clusterCall clusterAction = iota
	clusterCrash
	clusterRestart
	clusterSchedule
	clusterDisarm
)

func chooseClusterAction(rng *rand.Rand, cluster *simulationCluster) clusterAction {
	liveNodeIDs := cluster.liveNodeIDs()
	actions := make([]clusterAction, 0, 5)
	if len(liveNodeIDs) > 0 {
		actions = append(actions, clusterCall, clusterCrash, clusterSchedule, clusterDisarm)
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
			call := time.Now().UnixNano()
			var value int64
			err := selected.rt.Invoke(context.Background(), selected.id, "Add", []any{selected.delta}, &value)
			results <- invocationResult{
				id: selected.id,
				operation: porcupine.Operation{
					Input:  selected.delta,
					Call:   call,
					Output: counterOperationOutputFor(value, err),
					Return: time.Now().UnixNano(),
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
