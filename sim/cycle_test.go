//go:build sim

package sim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/suraciii/gor"
)

// cycleCaller walks a ring of entity keys: Chain calls the first key's Chain
// with the rest of the ring. A test closes the ring on a key already walked,
// so the runtime must detect the cycle across the two nodes.
type cycleCaller interface {
	Chain(context.Context, []string) error
}

type cycleCallerRequest struct {
	A0 []string
}

type cycleCallerReply struct{}

type cycleCallerEntity struct {
	b *gor.Binder
}

func (e *cycleCallerEntity) Chain(ctx context.Context, ring []string) error {
	if len(ring) == 0 {
		return nil
	}
	return gor.Ref[cycleCaller](e.b, ring[0]).Chain(ctx, ring[1:])
}

type cycleCallerProxy struct {
	invoker gor.Invoker
	id      gor.GrainId
}

func (p *cycleCallerProxy) Chain(ctx context.Context, ring []string) error {
	return p.invoker.Invoke(ctx, p.id, "Chain", &cycleCallerRequest{A0: ring}, &cycleCallerReply{})
}

func dispatchCycleCaller(ctx context.Context, instance cycleCaller, method string, args any, reply any) error {
	switch method {
	case "Chain":
		return instance.Chain(ctx, args.(*cycleCallerRequest).A0)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newCycleCallerCall(method string) (args any, reply any) {
	switch method {
	case "Chain":
		return &cycleCallerRequest{}, &cycleCallerReply{}
	default:
		return nil, nil
	}
}

func installCycleCaller(rt *gor.Runtime) error {
	if err := gor.InstallType[cycleCaller](rt, dispatchCycleCaller, func(invoker gor.Invoker, id gor.GrainId) cycleCaller {
		return &cycleCallerProxy{invoker: invoker, id: id}
	}, newCycleCallerCall, nil); err != nil {
		return err
	}
	return gor.Register[cycleCaller](rt, func(b *gor.Binder) cycleCaller {
		return &cycleCallerEntity{b: b}
	})
}

func TestSim_CallCycleAcrossNodesNamesTheCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		cluster, err := newSimulationCluster(backend, clusterNodeCount, newTimerTracker())
		if err != nil {
			t.Fatal(err)
		}
		defer cluster.close()
		for _, node := range cluster.nodes {
			if err := installCycleCaller(node.rt); err != nil {
				t.Fatal(err)
			}
		}
		cluster.advance(5 * simulationStepDuration)

		cycleType := gor.TypeName[cycleCaller]()
		local, remote := findPartitionIdentities(t, cluster, cycleType)
		first := gor.GrainId(local)
		second := gor.GrainId(remote)

		ctx, cancel := context.WithTimeout(context.Background(), 20*simulationStepDuration)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- cluster.nodes[0].rt.Invoke(ctx, first, "Chain", &cycleCallerRequest{A0: []string{second.GrainKey, first.GrainKey}}, &cycleCallerReply{})
		}()
		synctest.Wait()
		err = <-done
		if !errors.Is(err, gor.ErrCallCycle) {
			t.Fatalf("cross-node cycle error = %v, want %v", err, gor.ErrCallCycle)
		}
		message := err.Error()
		if !strings.Contains(message, "call cycle detected") ||
			!strings.Contains(message, local.GrainKey) || !strings.Contains(message, remote.GrainKey) {
			t.Fatalf("cycle error message = %q, want it to name %q and %q", message, local.GrainKey, remote.GrainKey)
		}
	})
}
