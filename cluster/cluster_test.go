package cluster

import (
	"fmt"
	"testing"

	"github.com/suraciii/gor/store"
)

func TestNewViewIncludesOnlyActiveMembers(t *testing.T) {
	view := NewView([]store.Member{
		{NodeAddr: "dead", Generation: "1", Status: store.MemberDead},
		{NodeAddr: "joining", Generation: "1", Status: store.MemberJoining},
		{NodeAddr: "active", Generation: "1", Status: store.MemberActive},
	})

	for i := 0; i < 1024; i++ {
		owner, ok := Owner(view, store.GrainId{GrainType: "account", GrainKey: fmt.Sprintf("key-%d", i)})
		if !ok || owner != "active" {
			t.Fatalf("Owner(key-%d) = (%q, %t), want (active, true)", i, owner, ok)
		}
	}
}

func TestOwnerEmptyView(t *testing.T) {
	if owner, ok := Owner(NewView(nil), store.GrainId{GrainType: "account", GrainKey: "alice"}); ok || owner != "" {
		t.Fatalf("Owner(empty) = (%q, %t), want (empty, false)", owner, ok)
	}
}

func TestOwnerSingleMember(t *testing.T) {
	view := NewView([]store.Member{{
		NodeAddr:   "node-a",
		Generation: "generation-a",
		Status:     store.MemberActive,
	}})

	for i := 0; i < 128; i++ {
		owner, ok := Owner(view, store.GrainId{GrainType: "account", GrainKey: fmt.Sprintf("key-%d", i)})
		if !ok || owner != "node-a" {
			t.Fatalf("Owner(key-%d) = (%q, %t), want (node-a, true)", i, owner, ok)
		}
	}
}

func TestOwnerMemberJoinMovesKeys(t *testing.T) {
	const (
		beforeNodeCount = 7
		keyCount        = 100000
	)
	beforeMembers := make([]store.Member, 0, beforeNodeCount)
	for i := 0; i < beforeNodeCount; i++ {
		beforeMembers = append(beforeMembers, store.Member{
			NodeAddr:   fmt.Sprintf("node-%02d", i),
			Generation: fmt.Sprintf("generation-%02d", i),
			Status:     store.MemberActive,
		})
	}
	afterMembers := append([]store.Member(nil), beforeMembers...)
	afterMembers = append(afterMembers, store.Member{
		NodeAddr:   fmt.Sprintf("node-%02d", beforeNodeCount),
		Generation: fmt.Sprintf("generation-%02d", beforeNodeCount),
		Status:     store.MemberActive,
	})
	before := NewView(beforeMembers)
	after := NewView(afterMembers)

	moved := 0
	for i := 0; i < keyCount; i++ {
		identity := store.GrainId{GrainType: "account", GrainKey: fmt.Sprintf("key-%d", i)}
		beforeOwner, beforeOK := Owner(before, identity)
		afterOwner, afterOK := Owner(after, identity)
		if !beforeOK || !afterOK {
			t.Fatalf("Owner(key-%d) returned an empty view", i)
		}
		if beforeOwner != afterOwner {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("member join moved no keys")
	}
	maxMoveFraction := 2.0 / float64(beforeNodeCount+1)
	if float64(moved)/keyCount > maxMoveFraction {
		t.Fatalf("member join moved %d/%d keys (%.2f%%), want at most %.2f%%", moved, keyCount, 100*float64(moved)/keyCount, 100*maxMoveFraction)
	}
}

func TestOwnerIsDeterministicForEquivalentViews(t *testing.T) {
	snapshot := []store.Member{
		{NodeAddr: "node-c", Generation: "generation-c", Status: store.MemberActive},
		{NodeAddr: "node-a", Generation: "generation-a", Status: store.MemberActive},
		{NodeAddr: "node-b", Generation: "generation-b", Status: store.MemberActive},
	}
	first := NewView(snapshot)
	second := NewView([]store.Member{snapshot[1], snapshot[2], snapshot[0]})

	for i := 0; i < 4096; i++ {
		identity := store.GrainId{GrainType: "account", GrainKey: fmt.Sprintf("key-%d", i)}
		firstOwner, firstOK := Owner(first, identity)
		secondOwner, secondOK := Owner(second, identity)
		if firstOwner != secondOwner || firstOK != secondOK {
			t.Fatalf("Owner(key-%d) differs: first=(%q, %t), second=(%q, %t)", i, firstOwner, firstOK, secondOwner, secondOK)
		}
	}
}

func TestOwnerDistributionWithVirtualPoints(t *testing.T) {
	const (
		nodeCount = 7
		keyCount  = 100000
	)
	viewMembers := make([]store.Member, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		viewMembers = append(viewMembers, store.Member{
			NodeAddr:   fmt.Sprintf("node-%02d", i),
			Generation: fmt.Sprintf("generation-%02d", i),
			Status:     store.MemberActive,
		})
	}
	view := NewView(viewMembers)
	counts := make(map[string]int, nodeCount)
	for i := 0; i < keyCount; i++ {
		owner, ok := Owner(view, store.GrainId{GrainType: "account", GrainKey: fmt.Sprintf("key-%d", i)})
		if !ok {
			t.Fatalf("Owner(key-%d) returned no owner", i)
		}
		counts[owner]++
	}

	max := 0
	for i := 0; i < nodeCount; i++ {
		count := counts[fmt.Sprintf("node-%02d", i)]
		if count > max {
			max = count
		}
	}
	average := float64(keyCount) / nodeCount
	if float64(max) > average*1.75 {
		t.Fatalf("max node load = %d, average = %.2f, want max <= %.2fx average", max, average, 1.75)
	}
}
