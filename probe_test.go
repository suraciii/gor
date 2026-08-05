package gor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

func TestTransportProberSendsProbeAndDecodesMemberID(t *testing.T) {
	target := cluster.MemberID{NodeAddr: "node-b", Generation: "generation-b"}
	payload, err := json.Marshal(callResponse{Reply: mustJSON(t, target)})
	if err != nil {
		t.Fatal(err)
	}
	probeTransport := &probeTransport{response: payload, sent: make(chan []byte, 1)}

	result := <-(transportProber{transport: probeTransport}).Probe(context.Background(), target)
	if result.Err != nil {
		t.Fatalf("probe error = %v", result.Err)
	}
	if result.ID != target {
		t.Fatalf("probe ID = %#v, want %#v", result.ID, target)
	}
	if got := string(<-probeTransport.sent); got != `{"kind":"probe"}` {
		t.Fatalf("probe request = %s, want only probe kind", got)
	}
}

func TestTransportProberPreservesTransportAndResponseErrors(t *testing.T) {
	transportErr := errors.New("network down")
	transportResult := <-(&transportProber{transport: &probeTransport{err: transportErr}}).Probe(
		context.Background(), cluster.MemberID{NodeAddr: "node-b"},
	)
	if !errors.Is(transportResult.Err, transportErr) {
		t.Fatalf("transport error = %v, want %v", transportResult.Err, transportErr)
	}

	responsePayload, err := json.Marshal(callResponse{Error: &errorEnvelope{Message: "cluster node is dead"}})
	if err != nil {
		t.Fatal(err)
	}
	responseResult := <-(&transportProber{transport: &probeTransport{response: responsePayload}}).Probe(
		context.Background(), cluster.MemberID{NodeAddr: "node-b"},
	)
	var responseErr probeResponseError
	if !errors.As(responseResult.Err, &responseErr) {
		t.Fatalf("response error = %v, want probe response error", responseResult.Err)
	}
	if errors.Is(responseResult.Err, transportErr) {
		t.Fatalf("response error = %v, incorrectly preserves transport error", responseResult.Err)
	}
}

func TestTransportProberReturnsMismatchedMemberIDAsProbeResult(t *testing.T) {
	want := cluster.MemberID{NodeAddr: "node-b", Generation: "generation-b"}
	got := cluster.MemberID{NodeAddr: "node-b", Generation: "generation-old"}
	payload, err := json.Marshal(callResponse{Reply: mustJSON(t, got)})
	if err != nil {
		t.Fatal(err)
	}

	result := <-(&transportProber{transport: &probeTransport{response: payload}}).Probe(context.Background(), want)
	if result.Err != nil {
		t.Fatalf("mismatched probe error = %v, want nil with mismatched ID", result.Err)
	}
	if result.ID != got {
		t.Fatalf("mismatched probe ID = %#v, want %#v", result.ID, got)
	}
}

func TestTransportProberRejectsMalformedMemberIDReply(t *testing.T) {
	result := <-(&transportProber{transport: &probeTransport{
		response: []byte(`{"reply":"not-a-member-id"}`),
	}}).Probe(context.Background(), cluster.MemberID{NodeAddr: "node-b", Generation: "generation-b"})
	if result.Err == nil {
		t.Fatal("malformed probe reply returned nil error")
	}
	if !strings.Contains(result.Err.Error(), "decode probe reply") {
		t.Fatalf("malformed probe reply error = %v, want decode probe reply error", result.Err)
	}
	if result.ID != (cluster.MemberID{}) {
		t.Fatalf("malformed probe reply ID = %#v, want zero ID with an error", result.ID)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type probeTransport struct {
	response []byte
	err      error
	sent     chan []byte
}

var _ transport.Transport = (*probeTransport)(nil)

func (p *probeTransport) Send(_ context.Context, _ string, payload []byte) ([]byte, error) {
	if p.sent != nil {
		p.sent <- append([]byte(nil), payload...)
	}
	return p.response, p.err
}

func (*probeTransport) Serve(context.Context, transport.Handler) error { return nil }

func (*probeTransport) Addr() string { return "probe-test" }

func (*probeTransport) Close() error { return nil }

func TestRuntime_HandleProbeReturnsCurrentMemberIDWithoutTableAccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1500, 0).UTC()
		table := &probeMemberStore{backend: store.NewMemory()}
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), table, clock.NewFake(start), "node-a", "generation-new", network.add("node-a"))...)
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
		if response.Error != nil {
			t.Fatalf("probe response error = %#v", response.Error)
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

func (s *probeMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
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
	if response.Error == nil || response.Error.Code != string(ErrInvalidRequest) || !strings.Contains(response.Error.Message, `unknown request kind "unknown"`) {
		t.Fatalf("response error = %#v, want invalid unknown-kind error", response.Error)
	}
}

func TestRuntime_HandleProbeRejectsStoppedNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1600, 0).UTC()
		members := store.NewMemory()
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, clock.NewFake(start), "node-a", "generation-a", network.add("node-a"))...)
		rt.Close()

		payload, err := rt.handleProbe()
		if err != nil {
			t.Fatalf("handle probe error = %v", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrRuntimeClosed) {
			t.Fatalf("stopped node probe response = %#v, want runtime-closed code after voluntary close", response.Error)
		}
	})
}

// TestRuntime_HandleProbeReadsRootStateDuringClosing pins the probe gate: after
// beginClose the cluster node is still active, so without reading the root
// state the probe would reply with a member id. It must instead refuse with
// the close-path stop code.
func TestRuntime_HandleProbeReadsRootStateDuringClosing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1650, 0).UTC()
		members := store.NewMemory()
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, clock.NewFake(start), "node-a", "generation-a", network.add("node-a"))...)
		defer rt.Close()

		// While running the probe replies with the current member id.
		runningPayload, err := rt.handleProbe()
		if err != nil {
			t.Fatalf("running probe error = %v", err)
		}
		var runningResponse callResponse
		if err := json.Unmarshal(runningPayload, &runningResponse); err != nil {
			t.Fatalf("decode running probe response: %v", err)
		}
		if runningResponse.Error != nil {
			t.Fatalf("running probe response = %#v, want member id", runningResponse.Error)
		}

		// Enter closing without closing the cluster node: the node is still
		// active, so only the root state check can refuse the probe.
		rt.beginClose()
		payload, err := rt.handleProbe()
		if err != nil {
			t.Fatalf("closing probe error = %v", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode closing probe response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrRuntimeClosed) {
			t.Fatalf("closing probe response = %#v, want runtime-closed code", response.Error)
		}
	})
}

// TestRuntime_HandleProbeRejectsAfterClusterDeath verifies the dead-path stop
// code: once the cluster has declared this node dead, a probe refuses with
// gor.node_dead.
func TestRuntime_HandleProbeRejectsAfterClusterDeath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1700, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		defer rt.Close()

		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := members.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		payload, err := rt.handleProbe()
		if err != nil {
			t.Fatalf("handle probe error = %v", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrNodeDead) {
			t.Fatalf("dead node probe response = %#v, want node-dead code", response.Error)
		}
	})
}
