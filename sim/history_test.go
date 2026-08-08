//go:build sim

package sim

import (
	"errors"
	"testing"
	"testing/synctest"

	"github.com/anishathalye/porcupine"
	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

// TestSim_DroppedReplyEffectReadByLaterCall drops the reply half of a
// forwarded call, then runs a second call that reads the write the first
// call left behind. The first call surfaces as a transport failure, but the
// history must record it as unknown, not failed: the caller cannot tell a
// dropped request from a dropped reply, and the second call's result proves
// the write landed. A failed record would pin the state to zero and make the
// second call's result impossible, so this history only linearizes under the
// unknown mapping.
func TestSim_DroppedReplyEffectReadByLaterCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newTimerTracker()
		backend := newFakeStore(tracker)
		cluster, err := newSimulationCluster(backend, clusterNodeCount, tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer cluster.close()
		cluster.advance(2 * simulationStepDuration)

		counterType := gor.TypeName[counter]()
		_, remote := findPartitionIdentities(t, cluster, counterType)
		if !cluster.nodes[1].rt.Owns(remote) {
			t.Fatalf("remote identity %s/%s is not owned by node 1", remote.GrainType, remote.GrainKey)
		}

		history := newCounterHistory()
		cluster.network.setDrops(networkDropSpec{
			source:      "node-0",
			destination: "node-1",
			side:        dropReply,
		})
		// The literal timestamps order the two operations in real time: the
		// second call is awaited after the first, so the history's intervals
		// must not overlap.
		first := awaitCall(invokeAsync(cluster.nodes[0].rt, gor.GrainId(remote), 1))
		if !errors.Is(first.err, gor.ErrTransportFailed) {
			t.Fatalf("dropped-reply forward error = %v, want %v", first.err, gor.ErrTransportFailed)
		}
		history.add(remote, porcupine.Operation{
			Input:  int64(1),
			Call:   1,
			Return: 2,
			Output: counterOperationOutputFor(first.value, first.err),
		})
		record := backend.snapshot([]store.GrainId{remote})[remote]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 1 {
			t.Fatalf("dropped reply did not take effect: value = %d, want 1", value)
		}

		cluster.network.setDrops(networkDropSpec{})
		second := awaitCall(invokeAsync(cluster.nodes[0].rt, gor.GrainId(remote), 1))
		if second.err != nil || second.value != 2 {
			t.Fatalf("call after dropped reply = (%d, %v), want (2, nil)", second.value, second.err)
		}
		history.add(remote, porcupine.Operation{
			Input:  int64(1),
			Call:   3,
			Return: 4,
			Output: counterOperationOutputFor(second.value, second.err),
		})
		if err := history.check(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCounterHistoryTimestampsOrderNonlinearOperations(t *testing.T) {
	first := porcupine.Operation{
		Input:  int64(1),
		Output: counterOperationOutput{value: 2, status: counterOperationSucceeded},
	}
	second := porcupine.Operation{
		Input:  int64(1),
		Output: counterOperationOutput{value: 1, status: counterOperationSucceeded},
	}

	oldTimestamps := []porcupine.Operation{
		first,
		second,
	}
	if !porcupine.CheckOperations(counterModel, oldTimestamps) {
		t.Fatal("porcupine rejected the non-linear history when all timestamps were equal")
	}

	strictTimestamps := []porcupine.Operation{
		{Input: first.Input, Call: 1, Return: 2, Output: first.Output},
		{Input: second.Input, Call: 3, Return: 4, Output: second.Output},
	}
	if porcupine.CheckOperations(counterModel, strictTimestamps) {
		t.Fatal("porcupine accepted the non-linear history when timestamps ordered operations")
	}
}
