//go:build sim

package sim

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

func TestSim_ForwardedCallProducesOneObservationAtOrigin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		tracker := newTimerTracker()
		sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
		network := newSimulationNetwork(backend)
		firstMembers, firstTransport := network.addNode("node-0")
		secondMembers, secondTransport := network.addNode("node-1")
		firstEvents := make(chan gor.CallObservation, 1)
		secondEvents := make(chan gor.CallObservation, 1)
		common := []gor.Option{
			gor.WithClock(sourceClock),
			gor.WithHeartbeatInterval(simulationStepDuration),
			gor.WithViewInterval(simulationStepDuration),
			gor.WithProbeInterval(simulationStepDuration),
			gor.WithProbeTimeout(simulationStepDuration / 2),
			gor.WithProbeFailures(3),
			gor.WithVoteTTL(6 * simulationStepDuration),
			gor.WithMaxTickGap(2 * simulationStepDuration),
			gor.WithMaxTableLatency(simulationStepDuration / 2),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
		}
		firstOptions := append(append([]gor.Option{}, common...),
			gor.WithMemberStore(firstMembers),
			gor.WithNodeAddr("node-0"),
			gor.WithGeneration("generation-0-0"),
			gor.WithTransport(firstTransport),
			gor.OnCall(func(observation gor.CallObservation) {
				firstEvents <- observation
			}),
		)
		secondOptions := append(append([]gor.Option{}, common...),
			gor.WithMemberStore(secondMembers),
			gor.WithNodeAddr("node-1"),
			gor.WithGeneration("generation-1-0"),
			gor.WithTransport(secondTransport),
			gor.OnCall(func(observation gor.CallObservation) {
				secondEvents <- observation
			}),
		)
		first, err := newCounterRuntimeWithOptions(backend, tracker, firstOptions...)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		second, err := newCounterRuntimeWithOptions(backend, tracker, secondOptions...)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()

		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		for range 20 {
			sourceClock.Advance(simulationStepDuration)
			synctest.Wait()
		}

		var remote gor.Identity
		for index := 0; index < 4096; index++ {
			candidate := gor.Identity{Type: gor.TypeName[counter](), Key: fmt.Sprintf("observed-%04d", index)}
			if !first.Owns(store.Identity(candidate)) && second.Owns(store.Identity(candidate)) {
				remote = candidate
				break
			}
		}
		if remote == (gor.Identity{}) {
			t.Fatal("could not find an identity owned by node-1")
		}

		var reply counterAddReply
		if err := first.Invoke(context.Background(), remote, "Add", &counterAddRequest{A0: 1}, &reply); err != nil {
			t.Fatalf("forwarded Add error = %v", err)
		}
		if reply.R0 != 1 {
			t.Fatalf("forwarded Add reply = %d, want 1", reply.R0)
		}

		select {
		case observation := <-firstEvents:
			if observation.EntityType != gor.TypeName[counter]() || observation.Method != "Add" || observation.Err != nil {
				t.Fatalf("origin observation = %#v, want Add with nil error", observation)
			}
		default:
			t.Fatal("origin node did not observe forwarded call")
		}
		select {
		case observation := <-secondEvents:
			t.Fatalf("receiver produced duplicate observation: %#v", observation)
		default:
		}
	})
}
