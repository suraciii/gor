//go:build sim

package sim

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

// TestSim_ForwardedReplyLandsAfterCallerDeadline holds the forwarded request
// in the fake network longer than the caller's deadline. The caller must get
// the deadline error while the message is still held, and the delivery must
// settle afterwards: the owner executes the call anyway, and the reply lands
// after the caller gave up, where it is dropped.
func TestSim_ForwardedReplyLandsAfterCallerDeadline(t *testing.T) {
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

		const hold = 3 * simulationStepDuration
		cluster.network.setDelays(map[networkPair]time.Duration{
			{source: "node-0", destination: "node-1"}: hold,
		})
		defer cluster.network.setDelays(nil)

		sendsBefore, deliveredBefore, _, _, _, heldBefore, completedBefore := cluster.network.stats()
		ctx, cancel := context.WithTimeout(context.Background(), simulationStepDuration)
		defer cancel()
		done := make(chan testCallResult, 1)
		go func() {
			var reply counterAddReply
			err := cluster.nodes[0].rt.Invoke(ctx, gor.GrainId(remote), "Add", &counterAddRequest{A0: 1}, &reply)
			done <- testCallResult{value: reply.R0, err: err}
		}()
		// The bubble clock fires the caller's deadline, then releases the held
		// message; the driver waits for the delivery to settle before observing.
		synctest.Wait()
		cluster.network.waitForIdle()
		cluster.backend.waitForIdle()

		result := <-done
		if !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("forwarded call with expired deadline = %v, want %v", result.err, context.DeadlineExceeded)
		}
		record := backend.snapshot([]store.GrainId{remote})[remote]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 1 {
			t.Fatalf("late forwarded call did not take effect: value = %d, want 1", value)
		}

		sendsAfter, deliveredAfter, _, _, _, heldAfter, completedAfter := cluster.network.stats()
		if sendsAfter != sendsBefore+1 || deliveredAfter != deliveredBefore {
			t.Fatalf("late forward transport stats = (sends %d->%d, delivered %d->%d), want one send and no delivered reply", sendsBefore, sendsAfter, deliveredBefore, deliveredAfter)
		}
		if heldAfter != heldBefore+1 || completedAfter != completedBefore+1 {
			t.Fatalf("late forward delivery stats = (held %d->%d, completed %d->%d), want one held and one completed delivery", heldBefore, heldAfter, completedBefore, completedAfter)
		}
	})
}

// TestSim_DroppedForwardedRequestNeverTakesEffect drops the request half of
// a forwarded call. The message never reaches the handler, so the entity's
// state is untouched; the caller sees a transport failure, and no delivery
// was ever started. This is the coarse side of per-message drop — partition
// already produces "nothing happened" — asserted here so the request-side
// path is pinned.
func TestSim_DroppedForwardedRequestNeverTakesEffect(t *testing.T) {
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

		cluster.network.setDrops(networkDropSpec{
			source:      "node-0",
			destination: "node-1",
			side:        dropRequest,
		})
		defer cluster.network.setDrops(networkDropSpec{})

		sendsBefore, deliveredBefore, _, dropRequestsBefore, _, heldBefore, completedBefore := cluster.network.stats()
		result := awaitCall(invokeAsync(cluster.nodes[0].rt, gor.GrainId(remote), 1))
		if !errors.Is(result.err, gor.ErrTransportFailed) {
			t.Fatalf("dropped forward error = %v, want %v", result.err, gor.ErrTransportFailed)
		}
		record := backend.snapshot([]store.GrainId{remote})[remote]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 0 {
			t.Fatalf("dropped request still took effect: value = %d, want 0", value)
		}

		sendsAfter, deliveredAfter, _, dropRequestsAfter, _, heldAfter, completedAfter := cluster.network.stats()
		if sendsAfter != sendsBefore+1 || deliveredAfter != deliveredBefore {
			t.Fatalf("dropped request transport stats = (sends %d->%d, delivered %d->%d), want one send and no delivered reply", sendsBefore, sendsAfter, deliveredBefore, deliveredAfter)
		}
		if dropRequestsAfter != dropRequestsBefore+1 || heldAfter != heldBefore || completedAfter != completedBefore {
			t.Fatalf("dropped request stats = (drop-requests %d->%d, held %d->%d, completed %d->%d), want one drop and no delivery", dropRequestsBefore, dropRequestsAfter, heldBefore, heldAfter, completedBefore, completedAfter)
		}
	})
}

// TestSim_DroppedReplyStillTakesEffect drops the reply half of a forwarded
// call. The handler already ran — the write landed — but the reply never
// reaches the caller, who sees a transport failure. The entity's state
// reflects the call while the caller does not know; this is the regime
// partition cannot produce, where the request side of the exchange was never
// silent.
func TestSim_DroppedReplyStillTakesEffect(t *testing.T) {
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

		cluster.network.setDrops(networkDropSpec{
			source:      "node-0",
			destination: "node-1",
			side:        dropReply,
		})
		defer cluster.network.setDrops(networkDropSpec{})

		sendsBefore, deliveredBefore, _, _, dropRepliesBefore, _, completedBefore := cluster.network.stats()
		result := awaitCall(invokeAsync(cluster.nodes[0].rt, gor.GrainId(remote), 1))
		if !errors.Is(result.err, gor.ErrTransportFailed) {
			t.Fatalf("dropped-reply forward error = %v, want %v", result.err, gor.ErrTransportFailed)
		}
		record := backend.snapshot([]store.GrainId{remote})[remote]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 1 {
			t.Fatalf("dropped reply did not take effect: value = %d, want 1", value)
		}

		sendsAfter, deliveredAfter, _, _, dropRepliesAfter, _, completedAfter := cluster.network.stats()
		if sendsAfter != sendsBefore+1 || deliveredAfter != deliveredBefore {
			t.Fatalf("dropped reply transport stats = (sends %d->%d, delivered %d->%d), want one send and no delivered reply", sendsBefore, sendsAfter, deliveredBefore, deliveredAfter)
		}
		if dropRepliesAfter != dropRepliesBefore+1 || completedAfter != completedBefore+1 {
			t.Fatalf("dropped reply stats = (drop-replies %d->%d, completed %d->%d), want one dropped reply after a completed delivery", dropRepliesBefore, dropRepliesAfter, completedBefore, completedAfter)
		}
	})
}

// TestSim_DroppedProbeDeclaresTargetDead drops every probe from node-1 to
// node-2. The failures accumulate to the probe-failure limit, node-1 votes
// node-2 dead, and with two active members one valid neighbor vote is the
// threshold. The reverse direction is unaffected: node-2's probes to node-1
// still succeed, so node-1 stays active.
func TestSim_DroppedProbeDeclaresTargetDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scenario := newProbeScenario(t, 2, nil)
		defer scenario.close()
		scenario.advance(2 * simulationStepDuration)

		target := "node-1"
		voter := "node-0"
		scenario.network.setDrops(networkDropSpec{
			source:      voter,
			destination: target,
			side:        dropRequest,
		})
		defer scenario.network.setDrops(networkDropSpec{})

		sendsBefore, deliveredBefore, _, dropRequestsBefore, _, _, completedBefore := scenario.network.stats()
		scenario.advance(8 * simulationStepDuration)

		sendsAfter, deliveredAfter, _, dropRequestsAfter, _, _, completedAfter := scenario.network.stats()
		if dropRequestsAfter == dropRequestsBefore {
			t.Fatal("no probe was dropped by the network")
		}
		if dropped := (sendsAfter - deliveredAfter) - (sendsBefore - deliveredBefore); dropped <= 0 {
			t.Fatalf("no probe failed while dropped: sends=%d delivered=%d, before (%d, %d)", sendsAfter, deliveredAfter, sendsBefore, deliveredBefore)
		}
		if completedAfter-completedBefore >= sendsAfter-sendsBefore {
			t.Fatalf("dropped probes were still delivered: completed %d->%d of %d sends", completedBefore, completedAfter, sendsAfter-sendsBefore)
		}

		member := scenarioMember(t, scenario, target, memberGeneration(1, 0))
		if member.Status != store.MemberDead {
			t.Fatalf("target status = %s, want dead after dropped probes", member.Status)
		}
		select {
		case <-scenario.nodes[1].rt.Done():
		default:
			t.Fatal("target Done channel remained open after dropped probes")
		}
		voterMember := scenarioMember(t, scenario, voter, memberGeneration(0, 0))
		if voterMember.Status != store.MemberActive {
			t.Fatalf("voter status = %s, want active (reverse direction probes still succeed)", voterMember.Status)
		}
	})
}

// TestSim_ProbeDelayedPastItsTimeout holds node-1's probes to node-2 in the
// fake network. The injected clock is advanced while the bubble clock stays
// frozen, so the probes' timeouts fire while their messages are still held;
// the replies are delivered late and dropped. The assertions are terminal
// consequences, not intermediate cluster state: the sender timed out (sends
// with no accepted reply), every held message still completed (nothing stuck
// in the delay), and the target stayed active (one voter's timeouts do not
// break the member).
func TestSim_ProbeDelayedPastItsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scenario := newProbeScenario(t, 3, nil)
		defer scenario.close()
		scenario.advance(2 * simulationStepDuration)

		target := "node-2"
		voter := "node-1"
		scenario.network.setDelays(map[networkPair]time.Duration{
			{source: voter, destination: target}: 3 * simulationStepDuration,
		})
		defer scenario.network.setDelays(nil)

		sendsBefore, deliveredBefore, _, _, _, heldBefore, completedBefore := scenario.network.stats()
		// Probe rounds at ticks 3..8 are held; each timeout fires while its
		// message is still in the network. The final half tick crosses the
		// last pending timeout (8.5) without firing the next probe round (9).
		scenario.advanceClock(8*simulationStepDuration + simulationStepDuration/2)

		// Settle: the bubble clock releases the held probes; their replies
		// land after their timeouts and are dropped.
		synctest.Wait()
		scenario.network.waitForIdle()
		scenario.backend.waitForIdle()

		sendsAfter, deliveredAfter, _, _, _, heldAfter, completedAfter := scenario.network.stats()
		if heldAfter == heldBefore {
			t.Fatal("no probe was held by the network delay")
		}
		if timedOut := (sendsAfter - deliveredAfter) - (sendsBefore - deliveredBefore); timedOut <= 0 {
			t.Fatalf("no probe timed out while held: sends=%d delivered=%d, before (%d, %d)", sendsAfter, deliveredAfter, sendsBefore, deliveredBefore)
		}
		if completedAfter-completedBefore < heldAfter-heldBefore {
			t.Fatalf("held probes did not all complete: held %d->%d, completed %d->%d", heldBefore, heldAfter, completedBefore, completedAfter)
		}
		member := scenarioMember(t, scenario, target, memberGeneration(2, 0))
		if member.Status != store.MemberActive {
			t.Fatalf("target status = %s, want active after timed-out probes", member.Status)
		}
		select {
		case <-scenario.nodes[2].rt.Done():
			t.Fatal("target stopped after delayed probes timed out")
		default:
		}
	})
}
