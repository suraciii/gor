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

	responsePayload, err := json.Marshal(callResponse{Error: "cluster node is dead"})
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
		response: []byte(`{"reply":"not-a-member-id","error":""}`),
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
	if !strings.Contains(response.Error, `unknown request kind "unknown"`) {
		t.Fatalf("response error = %q, want unknown-kind error", response.Error)
	}
}

func TestRuntime_HandleProbeRejectsStoppedNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1600, 0).UTC()
		members := store.NewMemory()
		network := newTestTransportNetwork()
		rt := mustNew(t, clusterRuntimeOptions(store.NewMemory(), members, clock.NewFake(start), "node-a", "generation-a", network.add("node-a"))...)
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
