package gor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
)

func TestRuntime_HandleProbeReturnsCurrentMemberIDWithoutTableAccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1500, 0).UTC()
		table := &probeMemberStore{backend: store.NewMemory()}
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), table, clock.NewFake(start), "node-a", "generation-new")...)
		defer rt.Close()
		operations := table.operations

		payload, err := rt.handle(context.Background(), []byte(`{"kind":"probe"}`))
		if err != nil {
			t.Fatalf("handle error = %v", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error != "" {
			t.Fatalf("probe response error = %q", response.Error)
		}
		var reply cluster.MemberID
		if err := json.Unmarshal(response.Reply, &reply); err != nil {
			t.Fatalf("decode probe reply: %v", err)
		}
		if reply != (cluster.MemberID{NodeAddr: "node-a", Generation: "generation-new"}) {
			t.Fatalf("probe reply = %#v, want current member ID", reply)
		}
		if got := table.operations; got != operations {
			t.Fatalf("member table operations after probe = %v, want unchanged from %v", got, operations)
		}
	})
}

type probeMemberStore struct {
	backend    store.MemberStore
	operations int
}

func (s *probeMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	s.operations++
	return s.backend.WriteMember(ctx, member)
}

func (s *probeMemberStore) ListMembers(ctx context.Context) ([]store.Member, error) {
	s.operations++
	return s.backend.ListMembers(ctx)
}

func TestRuntime_HandleProbeRejectsUnknownKind(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"unknown"}`))
	if err != nil {
		t.Fatalf("handle error = %v", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(response.Error, `unknown request kind "unknown"`) {
		t.Fatalf("response error = %q, want unknown-kind error", response.Error)
	}
}

func TestRuntime_HandleProbeRejectsStoppedNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1600, 0).UTC()
		members := store.NewMemory()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, clock.NewFake(start), "node-a", "generation-a")...)
		rt.Close()

		payload, err := rt.handle(context.Background(), []byte(`{"kind":"probe"}`))
		if err != nil {
			t.Fatalf("handle error = %v", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error == "" {
			t.Fatal("stopped node returned a probe reply")
		}
	})
}
