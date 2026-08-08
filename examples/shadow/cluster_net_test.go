//go:build net

package shadow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/cluster"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

// TestCluster_ForwardsCallAndCrossEntityCall proves, over real loopback TCP,
// that the example's business code routes through the cluster with no change:
// a call entering a node that does not own the entity is forwarded to the
// owner, and an entity's call to another entity forwards the same way. Node B
// is started after A, so B's initial view already contains A; every routing
// decision below is therefore the converged-ring decision, with no waiting on
// wall-clock convergence.
func TestCluster_ForwardsCallAndCrossEntityCall(t *testing.T) {
	shared := store.NewMemory()
	transportA, err := transport.New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transportB, err := transport.New("127.0.0.1:0")
	if err != nil {
		transportA.Close()
		t.Fatal(err)
	}

	rtA := mustClusterRuntime(t, shared, transportA, "generation-a")
	rtB := mustClusterRuntime(t, shared, transportB, "generation-b")
	defer rtA.Close()
	defer rtB.Close()
	ctx := context.Background()

	snapshot, err := shared.ListMembers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active := countActive(snapshot.Members); active != 2 {
		t.Fatalf("active members = %d, want 2", active)
	}
	view := cluster.NewView(snapshot.Members)
	addrA, addrB := transportA.Addr(), transportB.Addr()

	deviceType := gor.TypeName[domain.Device]()
	workshopType := gor.TypeName[domain.Workshop]()
	deviceOnA, ok := findKey(view, deviceType, addrA)
	if !ok {
		t.Fatal("no device key owned by A")
	}
	deviceOnB, ok := findKey(view, deviceType, addrB)
	if !ok {
		t.Fatal("no device key owned by B")
	}
	workshopOnA, ok := findKey(view, workshopType, addrA)
	if !ok {
		t.Fatal("no workshop key owned by A")
	}

	// A call entering B for an entity owned by A is forwarded: A activates it,
	// B does not. The read through B forwards the same way and returns the data.
	if err := gor.Ref[domain.Device](rtB, deviceOnA).Report(ctx, "w", "t=20"); err != nil {
		t.Fatalf("report A-owned device via B: %v", err)
	}
	if !hasActivation(rtA, deviceType, deviceOnA) {
		t.Fatal("A did not activate the A-owned device; B did not forward")
	}
	if hasActivation(rtB, deviceType, deviceOnA) {
		t.Fatal("B activated the A-owned device locally instead of forwarding")
	}
	readBack, err := gor.Ref[domain.Device](rtB, deviceOnA).Shadow(ctx)
	if err != nil {
		t.Fatalf("shadow A-owned device via B: %v", err)
	}
	if readBack.ReportedState != "t=20" {
		t.Fatalf("forwarded shadow state = %q, want t=20", readBack.ReportedState)
	}
	if hasActivation(rtB, deviceType, deviceOnA) {
		t.Fatal("B activated the device on the forwarded read")
	}

	// A device owned by B, reported through B, runs locally on B; its
	// cross-entity call to a workshop owned by A is forwarded B -> A, so the
	// workshop activates on A and a count read through B reaches A and sees it.
	if err := gor.Ref[domain.Device](rtB, deviceOnB).Report(ctx, workshopOnA, "t=21"); err != nil {
		t.Fatalf("report B-owned device via B: %v", err)
	}
	if !hasActivation(rtB, deviceType, deviceOnB) {
		t.Fatal("B did not activate its own device locally")
	}
	if !hasActivation(rtA, workshopType, workshopOnA) {
		t.Fatal("A did not activate the A-owned workshop; the cross-entity call was not forwarded")
	}
	count, err := gor.Ref[domain.Workshop](rtB, workshopOnA).OnlineCount(ctx)
	if err != nil {
		t.Fatalf("online count via B: %v", err)
	}
	if count != 1 {
		t.Fatalf("forwarded workshop online count = %d, want 1", count)
	}
}

func mustClusterRuntime(t *testing.T, shared *store.Memory, tr *transport.TCP, generation string) *gor.Runtime {
	t.Helper()
	rt, err := gor.New(
		gor.WithStore(shared),
		gor.WithMemberStore(shared),
		gor.WithNodeAddr(tr.Addr()),
		gor.WithGeneration(generation),
		gor.WithTransport(tr),
		gor.OnError(shadow.LogBackgroundError),
	)
	if err != nil {
		tr.Close()
		t.Fatalf("new runtime: %v", err)
	}
	if err := shadow.Register(rt); err != nil {
		rt.Close()
		t.Fatalf("register: %v", err)
	}
	return rt
}

func findKey(view cluster.View, entityType, owner string) (string, bool) {
	for i := 1; i <= 100000; i++ {
		key := fmt.Sprintf("probe-%06d", i)
		got, ok := cluster.Owner(view, store.GrainId{GrainType: entityType, GrainKey: key})
		if ok && got == owner {
			return key, true
		}
	}
	return "", false
}

func hasActivation(rt *gor.Runtime, entityType, key string) bool {
	for _, activation := range rt.Activations() {
		if activation.GrainId.GrainType == entityType && activation.GrainId.GrainKey == key {
			return true
		}
	}
	return false
}

func countActive(members []store.Member) int {
	n := 0
	for _, m := range members {
		if m.Status == store.MemberActive {
			n++
		}
	}
	return n
}
