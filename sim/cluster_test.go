//go:build sim

package sim

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

type dualActivationEntity struct {
	value   gor.State[int64]
	gate    <-chan struct{}
	entered chan<- struct{}
}

func (c *dualActivationEntity) Add(ctx context.Context, delta int64) (int64, error) {
	c.entered <- struct{}{}
	<-c.gate
	next := c.value.Get() + delta
	if err := c.value.Set(ctx, next); err != nil {
		return 0, err
	}
	return next, nil
}

func (*dualActivationEntity) Arm(context.Context, string, time.Duration, time.Duration) error {
	return nil
}

func (*dualActivationEntity) Disarm(context.Context, string) error {
	return nil
}

func (*dualActivationEntity) Tick(context.Context) error {
	return nil
}

func newDualActivationRuntime(backend *fakeStore, gate <-chan struct{}, entered chan<- struct{}) (*gor.Runtime, error) {
	rt, err := newRuntime(backend)
	if err != nil {
		return nil, err
	}
	if err := installCounterType(rt); err != nil {
		rt.Close()
		return nil, err
	}
	if err := registerCounter(rt, func(b *gor.Binder) counter {
		return &dualActivationEntity{
			value:   gor.NewState[int64](b, "value"),
			gate:    gate,
			entered: entered,
		}
	}); err != nil {
		rt.Close()
		return nil, err
	}
	return rt, nil
}

func TestSim_DoubleActivationRejectsETagConflict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		gate := make(chan struct{})
		entered := make(chan struct{}, 2)
		first, err := newDualActivationRuntime(backend, gate, entered)
		if err != nil {
			t.Fatal(err)
		}
		second, err := newDualActivationRuntime(backend, gate, entered)
		if err != nil {
			first.Close()
			t.Fatal(err)
		}
		defer first.Close()
		defer second.Close()

		id := gor.GrainId{GrainType: gor.TypeName[counter](), GrainKey: "dual"}
		firstCall := invokeAsync(first, id, 3)
		secondCall := invokeAsync(second, id, 5)
		synctest.Wait()
		<-entered
		<-entered
		close(gate)
		synctest.Wait()

		firstResult := <-firstCall
		secondResult := <-secondCall
		conflicts := 0
		successes := 0
		for _, result := range []testCallResult{firstResult, secondResult} {
			switch {
			case errors.Is(result.err, store.ErrConflict):
				conflicts++
			case result.err == nil:
				successes++
			default:
				t.Fatalf("dual activation result = (%d, %v), want success or ErrConflict", result.value, result.err)
			}
		}
		if conflicts != 1 || successes != 1 {
			t.Fatalf("dual activation results = (%v, %v), want one success and one conflict", firstResult.err, secondResult.err)
		}

		storeID := store.GrainId{GrainType: id.GrainType, GrainKey: id.GrainKey}
		record := backend.snapshot([]store.GrainId{storeID})[storeID]
		if record.ETag != 1 {
			t.Fatalf("dual activation ETag = %d, want 1", record.ETag)
		}
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 3 && value != 5 {
			t.Fatalf("dual activation value = %d, want 3 or 5", value)
		}
		if err := newObservations().check(backend, []store.GrainId{storeID}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSim_NetworkPartitionCreatesDualActivationAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newTimerTracker()
		backend := newFakeStore(tracker)
		cluster, err := newSimulationCluster(backend, clusterNodeCount, tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer cluster.close()

		cluster.advance(5 * simulationStepDuration)
		counterType := gor.TypeName[counter]()
		localID, remoteID := findPartitionIdentities(t, cluster, counterType)
		local := gor.GrainId(localID)
		remote := gor.GrainId(remoteID)

		seed := awaitCall(invokeAsync(cluster.nodes[0].rt, local, 2))
		if seed.err != nil || seed.value != 2 {
			t.Fatalf("seed call = (%d, %v), want (2, nil)", seed.value, seed.err)
		}
		cluster.advance(3 * simulationStepDuration)
		synctest.Wait()
		if activations := cluster.nodes[0].rt.Activations(); len(activations) != 0 {
			t.Fatalf("activations after idle eviction = %#v, want empty", activations)
		}

		if err := cluster.partition(map[int]int{0: 0, 1: 1}); err != nil {
			t.Fatal(err)
		}
		partitioned := awaitCall(invokeAsync(cluster.nodes[0].rt, remote, 1))
		if !errors.Is(partitioned.err, errSimNetworkPartition) {
			t.Fatalf("cross-partition forward error = %v, want %v", partitioned.err, errSimNetworkPartition)
		}
		_, _, dropped, _, _, _, _ := cluster.network.stats()
		if dropped == 0 {
			t.Fatal("partition dropped no transport messages")
		}

		cluster.advance(4 * simulationStepDuration)
		if !cluster.nodes[0].rt.Owns(localID) || !cluster.nodes[1].rt.Owns(localID) {
			t.Fatalf("partition did not create dual ownership: node0=%t node1=%t", cluster.nodes[0].rt.Owns(localID), cluster.nodes[1].rt.Owns(localID))
		}

		started := make(chan struct{}, 2)
		release := make(chan struct{})
		backend.setReadBarrier(localID, readBarrier{started: started, release: release})
		first := invokeAsync(cluster.nodes[0].rt, local, 3)
		second := invokeAsync(cluster.nodes[1].rt, local, 5)
		synctest.Wait()
		for range 2 {
			<-started
		}
		close(release)
		synctest.Wait()
		firstResult := <-first
		secondResult := <-second
		conflicts := 0
		successes := 0
		for _, result := range []testCallResult{firstResult, secondResult} {
			switch {
			case errors.Is(result.err, store.ErrConflict):
				conflicts++
			case result.err == nil:
				successes++
			default:
				t.Fatalf("partition write result = (%d, %v), want success or ErrConflict", result.value, result.err)
			}
		}
		if conflicts != 1 || successes != 1 {
			t.Fatalf("partition write results = (%v, %v), want one success and one conflict", firstResult.err, secondResult.err)
		}
		backend.setReadBarrier(localID, readBarrier{})

		cluster.heal()
		cluster.settle()
		if err := cluster.checkInvariants([]store.GrainId{localID, remoteID}); err != nil {
			t.Fatal(err)
		}
		if cluster.nodes[0].rt.Owns(localID) == cluster.nodes[1].rt.Owns(localID) {
			t.Fatalf("healed local ownership = (%t, %t), want exactly one owner", cluster.nodes[0].rt.Owns(localID), cluster.nodes[1].rt.Owns(localID))
		}

		healedRemote := findIdentityOwnedBy(t, cluster, 1, counterType)
		sendsBefore, deliveredBefore, droppedBefore, _, _, _, _ := cluster.network.stats()
		recovered := awaitCall(invokeAsync(cluster.nodes[0].rt, gor.GrainId(healedRemote), 7))
		if recovered.err != nil || recovered.value != 7 {
			t.Fatalf("healed forwarded call = (%d, %v), want (7, nil)", recovered.value, recovered.err)
		}
		sendsAfter, deliveredAfter, droppedAfter, _, _, _, _ := cluster.network.stats()
		if sendsAfter <= sendsBefore || deliveredAfter <= deliveredBefore || droppedAfter != droppedBefore {
			t.Fatalf("healed transport stats = (%d,%d,%d), before (%d,%d,%d), want one delivered send and no new drop", sendsAfter, deliveredAfter, droppedAfter, sendsBefore, deliveredBefore, droppedBefore)
		}
	})
}

func findPartitionIdentities(t *testing.T, cluster *simulationCluster, entityType string) (store.GrainId, store.GrainId) {
	t.Helper()
	var local, remote store.GrainId
	for index := 0; index < 4096 && (local == (store.GrainId{}) || remote == (store.GrainId{})); index++ {
		id := store.GrainId{GrainType: entityType, GrainKey: fmt.Sprintf("partition-%04d", index)}
		switch {
		case cluster.nodes[0].rt.Owns(id) && local == (store.GrainId{}):
			local = id
		case cluster.nodes[1].rt.Owns(id) && remote == (store.GrainId{}):
			remote = id
		}
	}
	if local == (store.GrainId{}) || remote == (store.GrainId{}) {
		t.Fatalf("could not find identities owned by separate nodes: local=%v remote=%v", local, remote)
	}
	return local, remote
}

func findIdentityOwnedBy(t *testing.T, cluster *simulationCluster, nodeID int, entityType string) store.GrainId {
	t.Helper()
	for index := 0; index < 4096; index++ {
		id := store.GrainId{GrainType: entityType, GrainKey: fmt.Sprintf("partition-%04d", index)}
		if cluster.nodes[nodeID].rt.Owns(id) {
			return id
		}
	}
	t.Fatalf("could not find identity owned by node %d", nodeID)
	return store.GrainId{}
}

func TestSim_CrashRestartRestoresState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		rt, err := newCounterRuntimeWithOptions(backend, newTimerTracker())
		if err != nil {
			t.Fatal(err)
		}

		id := gor.GrainId{GrainType: gor.TypeName[counter](), GrainKey: "restart"}
		result := awaitCall(invokeAsync(rt, id, 4))
		if result.err != nil || result.value != 4 {
			t.Fatalf("initial call = (%d, %v), want (4, nil)", result.value, result.err)
		}
		rt.Kill()
		synctest.Wait()

		rt, err = newCounterRuntimeWithOptions(backend, newTimerTracker())
		if err != nil {
			t.Fatal(err)
		}
		defer rt.Close()
		result = awaitCall(invokeAsync(rt, id, 6))
		if result.err != nil || result.value != 10 {
			t.Fatalf("restarted call = (%d, %v), want (10, nil)", result.value, result.err)
		}

		storeID := store.GrainId{GrainType: id.GrainType, GrainKey: id.GrainKey}
		record := backend.snapshot([]store.GrainId{storeID})[storeID]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 10 {
			t.Fatalf("restarted state = %d, want 10", value)
		}
		if err := newObservations().check(backend, []store.GrainId{storeID}); err != nil {
			t.Fatal(err)
		}
	})
}
