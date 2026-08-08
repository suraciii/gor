package gor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
)

func TestRequestContext_NormalizesScalarsAndPreservesSnapshots(t *testing.T) {
	base := context.Background()
	ctx := base
	values := map[string]any{
		"bool":    true,
		"string":  "value",
		"int":     int(-7),
		"int8":    int8(-8),
		"int16":   int16(-16),
		"int32":   int32(-32),
		"int64":   int64(-64),
		"uint":    uint(7),
		"uint8":   uint8(8),
		"uint16":  uint16(16),
		"uint32":  uint32(32),
		"uint64":  uint64(64),
		"float32": float32(3.5),
		"float64": float64(4.5),
		"nil":     nil,
	}
	for key, value := range values {
		var err error
		ctx, err = WithRequestContext(ctx, key, value)
		if err != nil {
			t.Fatalf("WithRequestContext(%q): %v", key, err)
		}
	}

	want := map[string]any{
		"bool":    true,
		"string":  "value",
		"int":     int64(-7),
		"int8":    int64(-8),
		"int16":   int64(-16),
		"int32":   int64(-32),
		"int64":   int64(-64),
		"uint":    uint64(7),
		"uint8":   uint64(8),
		"uint16":  uint64(16),
		"uint32":  uint64(32),
		"uint64":  uint64(64),
		"float32": float64(3.5),
		"float64": float64(4.5),
		"nil":     nil,
	}
	for key, expected := range want {
		got, ok := RequestContextValue(ctx, key)
		if !ok || fmt.Sprintf("%T:%v", got, got) != fmt.Sprintf("%T:%v", expected, expected) {
			t.Fatalf("RequestContextValue(%q) = (%T, %v, %v), want (%T, %v, true)", key, got, got, ok, expected, expected)
		}
	}
	if _, ok := RequestContextValue(ctx, "missing"); ok {
		t.Fatal("missing Request Context value reported present")
	}

	first, err := WithRequestContext(base, "trace_id", "first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := WithRequestContext(base, "trace_id", "second")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := RequestContextValue(base, "trace_id"); got != nil {
		t.Fatalf("parent trace_id = %v, want missing", got)
	}
	if got, _ := RequestContextValue(first, "trace_id"); got != "first" {
		t.Fatalf("first trace_id = %v, want first", got)
	}
	if got, _ := RequestContextValue(second, "trace_id"); got != "second" {
		t.Fatalf("second trace_id = %v, want second", got)
	}
	replaced, err := WithRequestContext(first, "trace_id", "replacement")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := RequestContextValue(first, "trace_id"); got != "first" {
		t.Fatalf("parent snapshot after replacement = %v, want first", got)
	}
	if got, _ := RequestContextValue(replaced, "trace_id"); got != "replacement" {
		t.Fatalf("replacement snapshot = %v, want replacement", got)
	}
}

func TestRequestContext_NilContextIsRejected(t *testing.T) {
	var nilContext context.Context
	if value, ok := RequestContextValue(nilContext, "missing"); value != nil || ok {
		t.Fatalf("RequestContextValue(nil) = (%#v, %v), want (nil, false)", value, ok)
	}
	ctx, err := WithRequestContext(nilContext, "key", "value")
	if ctx != nil || err == nil || !errors.Is(err, ErrRequestEncodeFailed) {
		t.Fatalf("WithRequestContext(nil) = (%v, %v), want (nil, request-encode-failed)", ctx, err)
	}
}

func TestRequestContext_ValidationLeavesParentUnchanged(t *testing.T) {
	base := context.Background()
	type namedString string
	invalidValues := []any{
		map[string]string{"value": "unsupported"},
		[]byte("unsupported"),
		uintptr(1),
		namedString("named"),
		math.NaN(),
		math.Inf(1),
	}
	for _, value := range invalidValues {
		got, err := WithRequestContext(base, "value", value)
		if err == nil || !errors.Is(err, ErrRequestEncodeFailed) {
			t.Fatalf("WithRequestContext(%T) error = %v, want request-encode-failed", value, err)
		}
		if got != base {
			t.Fatalf("WithRequestContext(%T) returned a changed parent context", value)
		}
	}
	invalidKeys := []string{"", strings.Repeat("x", maxRequestContextKeySize+1), string([]byte{0xff})}
	for _, key := range invalidKeys {
		got, err := WithRequestContext(base, key, "value")
		if err == nil || !errors.Is(err, ErrRequestEncodeFailed) {
			t.Fatalf("WithRequestContext(%q) error = %v, want request-encode-failed", key, err)
		}
		if got != base {
			t.Fatalf("WithRequestContext(%q) returned a changed parent context", key)
		}
	}

	ctx := base
	for index := 0; index < maxRequestContextEntries; index++ {
		var err error
		ctx, err = WithRequestContext(ctx, fmt.Sprintf("key-%d", index), index)
		if err != nil {
			t.Fatalf("adding entry %d: %v", index, err)
		}
	}
	tooMany, err := WithRequestContext(ctx, "too-many", true)
	if err == nil || !errors.Is(err, ErrRequestEncodeFailed) || tooMany != ctx {
		t.Fatalf("too many entries = (%v, %v), want unchanged context and request-encode-failed", tooMany, err)
	}

	tooLarge, err := WithRequestContext(base, "large", strings.Repeat("x", maxRequestContextBytes))
	if err == nil || !errors.Is(err, ErrRequestEncodeFailed) || tooLarge != base {
		t.Fatalf("oversized context = (%v, %v), want unchanged context and request-encode-failed", tooLarge, err)
	}
}

func TestRequestContext_OmittedAndEmptyWireFieldsAreDistinctFromMalformed(t *testing.T) {
	base := callRequest{
		Kind:      requestKindInvoke,
		GrainType: "gor.Account",
		GrainKey:  "alice",
		Method:    "Balance",
		Args:      json.RawMessage(`{}`),
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "request_context") {
		t.Fatalf("empty call request = %s, want omitted request_context", encoded)
	}
	var decoded callRequest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := decodeRequestContext(decoded.RequestContext); err != nil || len(snapshot.values) != 0 {
		t.Fatalf("omitted request_context = (%#v, %v), want empty snapshot", snapshot, err)
	}
	if snapshot, err := decodeRequestContext(json.RawMessage(`{}`)); err != nil || len(snapshot.values) != 0 {
		t.Fatalf("empty request_context = (%#v, %v), want empty snapshot", snapshot, err)
	}
	if _, err := decodeRequestContext(json.RawMessage(`null`)); err == nil {
		t.Fatal("null request_context was accepted")
	}
	if _, err := decodeRequestContext(json.RawMessage(`[]`)); err == nil {
		t.Fatal("array request_context was accepted")
	}
}

func TestRequestContext_WireRoundTripKeepsExactIntegers(t *testing.T) {
	ctx, err := WithRequestContext(context.Background(), "signed", int64(math.MaxInt64))
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = WithRequestContext(ctx, "unsigned", uint64(math.MaxUint64))
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = WithRequestContext(ctx, "nil", nil)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := requestContextPayload(ctx)
	if err != nil {
		t.Fatalf("requestContextPayload: %v", err)
	}
	if !strings.Contains(string(encoded), `"type":"int64"`) || !strings.Contains(string(encoded), `"type":"uint64"`) {
		t.Fatalf("encoded context = %s, want typed integer entries", encoded)
	}
	decoded, err := decodeRequestContext(encoded)
	if err != nil {
		t.Fatalf("decodeRequestContext: %v", err)
	}
	if got, ok := decoded.values["signed"].(int64); !ok || got != math.MaxInt64 {
		t.Fatalf("decoded signed = (%T, %v), want exact int64 max", decoded.values["signed"], decoded.values["signed"])
	}
	if got, ok := decoded.values["unsigned"].(uint64); !ok || got != math.MaxUint64 {
		t.Fatalf("decoded unsigned = (%T, %v), want exact uint64 max", decoded.values["unsigned"], decoded.values["unsigned"])
	}
	if got, ok := decoded.values["nil"]; !ok || got != nil {
		t.Fatalf("decoded nil = (%#v, %v), want (nil, true)", got, ok)
	}
}

func TestRequestContext_RejectsMalformedPeerBeforeActivation(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	var factoryCalls atomic.Int32
	installRequestContextProbe(t, rt, func(*Binder) requestContextProbe {
		factoryCalls.Add(1)
		return &requestContextProbeEntity{}
	})

	oversizedEntries := make(map[string]requestContextWireEntry)
	for index := 0; index < maxRequestContextEntries+1; index++ {
		oversizedEntries[fmt.Sprintf("key-%d", index)] = requestContextWireEntry{Type: "bool", Value: json.RawMessage("true")}
	}
	oversized, err := json.Marshal(oversizedEntries)
	if err != nil {
		t.Fatal(err)
	}
	largeValue, err := json.Marshal(strings.Repeat("x", maxRequestContextBytes))
	if err != nil {
		t.Fatal(err)
	}
	oversizedBytes, err := json.Marshal(map[string]requestContextWireEntry{
		"large": {Type: "string", Value: largeValue},
	})
	if err != nil {
		t.Fatal(err)
	}
	invalidUTF8Key := json.RawMessage("{\"" + string([]byte{0xff}) + "\":{\"type\":\"string\",\"value\":\"bad\"}}")
	cases := []json.RawMessage{
		json.RawMessage(`{"trace":{"type":"unknown","value":true}}`),
		json.RawMessage(`{"trace":{"type":"int64","value":1.5}}`),
		json.RawMessage(`{"trace":{"type":"bool","value":"true"}}`),
		json.RawMessage(`{"trace":{"type":"uint64","value":-1}}`),
		json.RawMessage(`{"trace":{"type":"string","value":12}}`),
		json.RawMessage(`{"":{"type":"string","value":"empty-key"}}`),
		invalidUTF8Key,
		json.RawMessage(`null`),
		oversized,
		oversizedBytes,
	}
	for _, requestContext := range cases {
		payload, err := rt.handleInvoke(context.Background(), callRequest{
			Kind:           requestKindInvoke,
			GrainType:      TypeName[requestContextProbe](),
			GrainKey:       "malformed",
			Method:         "Snapshot",
			Args:           json.RawMessage(`{}`),
			RequestContext: requestContext,
		})
		if err != nil {
			t.Fatalf("handleInvoke(%s) error = %v, want error response", requestContext, err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode error response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrInvalidRequest) {
			t.Fatalf("malformed request response = %#v, want invalid-request", response.Error)
		}
	}
	if got := factoryCalls.Load(); got != 0 {
		t.Fatalf("factory calls after malformed requests = %d, want 0", got)
	}

	rt.beginClose()
	payload, err := rt.handleInvoke(context.Background(), callRequest{
		Kind:           requestKindInvoke,
		GrainType:      TypeName[requestContextProbe](),
		GrainKey:       "closed",
		Method:         "Snapshot",
		Args:           json.RawMessage(`{}`),
		RequestContext: json.RawMessage(`null`),
	})
	if err != nil {
		t.Fatalf("closed handleInvoke error = %v, want error response", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode closed response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrRuntimeClosed) {
		t.Fatalf("closed malformed request response = %#v, want runtime-closed", response.Error)
	}
}

type requestContextSnapshotReply struct {
	Trace        string
	Bool         bool
	String       string
	Int          int64
	Uint         uint64
	Float        float64
	HasNil       bool
	HasMissing   bool
	ServerAbsent bool
}

type requestContextProbe interface {
	Snapshot(context.Context) (requestContextSnapshotReply, error)
}

type requestContextProbeEntity struct{}

type requestContextSnapshotRequest struct{}
type requestContextSnapshotResponse struct {
	R0 requestContextSnapshotReply
}

type requestContextProbeProxy struct {
	invoker Invoker
	id      GrainId
}

func (e *requestContextProbeEntity) Snapshot(ctx context.Context) (requestContextSnapshotReply, error) {
	if _, err := WithRequestContext(ctx, "server", "server-only"); err != nil {
		return requestContextSnapshotReply{}, err
	}
	var result requestContextSnapshotReply
	if value, ok := RequestContextValue(ctx, "trace_id"); ok {
		result.Trace, _ = value.(string)
	}
	if value, ok := RequestContextValue(ctx, "bool"); ok {
		result.Bool, _ = value.(bool)
	}
	if value, ok := RequestContextValue(ctx, "string"); ok {
		result.String, _ = value.(string)
	}
	if value, ok := RequestContextValue(ctx, "int"); ok {
		result.Int, _ = value.(int64)
	}
	if value, ok := RequestContextValue(ctx, "uint"); ok {
		result.Uint, _ = value.(uint64)
	}
	if value, ok := RequestContextValue(ctx, "float"); ok {
		result.Float, _ = value.(float64)
	}
	_, result.HasNil = RequestContextValue(ctx, "nil")
	_, result.HasMissing = RequestContextValue(ctx, "missing")
	_, serverPresent := RequestContextValue(ctx, "server")
	result.ServerAbsent = !serverPresent
	return result, nil
}

func dispatchRequestContextProbe(ctx context.Context, instance requestContextProbe, method string, args any, reply any) error {
	if method != "Snapshot" {
		return fmt.Errorf("unknown method %q", method)
	}
	value, err := instance.Snapshot(ctx)
	if err == nil {
		reply.(*requestContextSnapshotResponse).R0 = value
	}
	return err
}

func newRequestContextProbeCall(method string) (args any, reply any) {
	if method != "Snapshot" {
		return nil, nil
	}
	return &requestContextSnapshotRequest{}, &requestContextSnapshotResponse{}
}

func installRequestContextProbe(t *testing.T, rt *Runtime, factory func(*Binder) requestContextProbe) {
	t.Helper()
	if err := InstallType[requestContextProbe](rt, dispatchRequestContextProbe, func(invoker Invoker, id GrainId) requestContextProbe {
		return &requestContextProbeProxy{invoker: invoker, id: id}
	}, newRequestContextProbeCall, nil); err != nil {
		t.Fatal(err)
	}
	if err := Register[requestContextProbe](rt, factory); err != nil {
		t.Fatal(err)
	}
}

func (p *requestContextProbeProxy) Snapshot(ctx context.Context) (requestContextSnapshotReply, error) {
	var reply requestContextSnapshotResponse
	err := p.invoker.Invoke(ctx, p.id, "Snapshot", &requestContextSnapshotRequest{}, &reply)
	return reply.R0, err
}

func requestContextCallContext(t *testing.T) context.Context {
	t.Helper()
	ctx := context.Background()
	var err error
	for key, value := range map[string]any{
		"trace_id": "trace-42",
		"bool":     true,
		"string":   "string-value",
		"int":      int32(-42),
		"uint":     uint16(42),
		"float":    float32(4.5),
		"nil":      nil,
	} {
		ctx, err = WithRequestContext(ctx, key, value)
		if err != nil {
			t.Fatalf("WithRequestContext(%q): %v", key, err)
		}
	}
	return ctx
}

func TestRequestContext_StateRecordHasNoContext(t *testing.T) {
	backend := store.NewMemory()
	rt := mustNew(t, WithStore(backend), WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)
	ctx, err := WithRequestContext(context.Background(), "trace_id", "state-call")
	if err != nil {
		t.Fatal(err)
	}
	id := GrainId{GrainType: TypeName[Account](), GrainKey: "state-isolation"}
	var reply accountDepositReply
	if err := rt.Invoke(ctx, id, "Deposit", &accountDepositRequest{A0: 7}, &reply); err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	record, err := backend.Read(context.Background(), store.GrainId(id))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != `{"value":7}` || strings.Contains(string(record.Data), "request_context") {
		t.Fatalf("State record = %s, want only Grain State", record.Data)
	}
}

func TestRequestContext_LocalNestedAndResponseIsolation(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installRequestContextProbe(t, rt, func(*Binder) requestContextProbe {
		return &requestContextProbeEntity{}
	})
	if err := InstallType[requestContextParent](rt, dispatchRequestContextParent, func(invoker Invoker, id GrainId) requestContextParent {
		return &requestContextParentProxy{invoker: invoker, id: id}
	}, newRequestContextParentCall, nil); err != nil {
		t.Fatal(err)
	}
	if err := Register[requestContextParent](rt, func(b *Binder) requestContextParent {
		return &requestContextParentEntity{target: Ref[requestContextProbe](b, "nested-target")}
	}); err != nil {
		t.Fatal(err)
	}

	ctx := requestContextCallContext(t)
	result, err := Ref[requestContextProbe](rt, "local").Snapshot(ctx)
	if err != nil {
		t.Fatalf("local Snapshot: %v", err)
	}
	if result.Trace != "trace-42" || !result.Bool || result.String != "string-value" || result.Int != -42 || result.Uint != 42 || result.Float != 4.5 || !result.HasNil || result.HasMissing || !result.ServerAbsent {
		t.Fatalf("local Request Context result = %#v", result)
	}

	nested, err := Ref[requestContextParent](rt, "parent").Nested(ctx)
	if err != nil {
		t.Fatalf("nested call: %v", err)
	}
	if nested.First != "trace-42" || nested.Second != "nested" {
		t.Fatalf("nested Request Context result = %#v, want inherited then replaced", nested)
	}
	if got, _ := RequestContextValue(ctx, "trace_id"); got != "trace-42" {
		t.Fatalf("caller context after nested call = %v, want trace-42", got)
	}
}

type requestContextNestedReply struct {
	First  string
	Second string
}

type requestContextParent interface {
	Nested(context.Context) (requestContextNestedReply, error)
}

type requestContextParentEntity struct {
	target requestContextProbe
}

type requestContextParentRequest struct{}
type requestContextParentResponse struct {
	R0 requestContextNestedReply
}

type requestContextParentProxy struct {
	invoker Invoker
	id      GrainId
}

func (e *requestContextParentEntity) Nested(ctx context.Context) (requestContextNestedReply, error) {
	first, err := e.target.Snapshot(ctx)
	if err != nil {
		return requestContextNestedReply{}, err
	}
	replaced, err := WithRequestContext(ctx, "trace_id", "nested")
	if err != nil {
		return requestContextNestedReply{}, err
	}
	second, err := e.target.Snapshot(replaced)
	if err != nil {
		return requestContextNestedReply{}, err
	}
	return requestContextNestedReply{First: first.Trace, Second: second.Trace}, nil
}

func dispatchRequestContextParent(ctx context.Context, instance requestContextParent, method string, args any, reply any) error {
	if method != "Nested" {
		return fmt.Errorf("unknown method %q", method)
	}
	value, err := instance.Nested(ctx)
	if err == nil {
		reply.(*requestContextParentResponse).R0 = value
	}
	return err
}

func newRequestContextParentCall(method string) (args any, reply any) {
	if method != "Nested" {
		return nil, nil
	}
	return &requestContextParentRequest{}, &requestContextParentResponse{}
}

func (p *requestContextParentProxy) Nested(ctx context.Context) (requestContextNestedReply, error) {
	var reply requestContextParentResponse
	err := p.invoker.Invoke(ctx, p.id, "Nested", &requestContextParentRequest{}, &reply)
	return reply.R0, err
}

func TestRequestContext_ForwardedAndIndependentCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1800, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstTransport := network.add("node-a")
		secondTransport := network.add("node-b")
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", firstTransport)
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", secondTransport)
		first := mustNew(t, firstOptions...)
		second := mustNew(t, secondOptions...)
		defer first.Close()
		defer second.Close()
		installRequestContextProbe(t, first, func(*Binder) requestContextProbe { return &requestContextProbeEntity{} })
		installRequestContextProbe(t, second, func(*Binder) requestContextProbe { return &requestContextProbeEntity{} })
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		fakeClock.Advance(time.Second)
		synctest.Wait()

		id := findRequestContextTarget(t, first, "node-b")
		ctx := requestContextCallContext(t)
		result, err := Ref[requestContextProbe](first, id.GrainKey).Snapshot(ctx)
		if err != nil {
			t.Fatalf("forwarded Snapshot: %v", err)
		}
		if result.Trace != "trace-42" || result.Int != -42 || result.Uint != 42 || !result.HasNil || result.HasMissing || !result.ServerAbsent {
			t.Fatalf("forwarded Request Context result = %#v", result)
		}
		independent, err := Ref[requestContextProbe](first, id.GrainKey).Snapshot(context.Background())
		if err != nil {
			t.Fatalf("independent Snapshot: %v", err)
		}
		if independent.Trace != "" || independent.Bool || independent.String != "" || independent.Int != 0 || independent.Uint != 0 || independent.Float != 0 || independent.HasNil || independent.HasMissing || !independent.ServerAbsent {
			t.Fatalf("independent forwarded Request Context result = %#v, want empty", independent)
		}
		if _, ok := RequestContextValue(ctx, "server"); ok {
			t.Fatal("server-added Request Context changed the caller context")
		}
	})
}

func findRequestContextTarget(t *testing.T, rt *Runtime, owner string) GrainId {
	t.Helper()
	view := rt.clusterView.Load()
	for index := 0; index < 4096; index++ {
		candidate := GrainId{GrainType: TypeName[requestContextProbe](), GrainKey: fmt.Sprintf("context-%d", index)}
		candidateOwner, ok := cluster.Owner(*view, store.GrainId(candidate))
		if ok && candidateOwner == owner {
			return candidate
		}
	}
	t.Fatalf("no Request Context identity owned by %q", owner)
	return GrainId{}
}

func TestRequestContext_LocalCancellationKeepsValuesReadable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		defer rt.Close()
		started := make(chan context.Context, 1)
		installRequestContextCancellation(t, rt, started)

		ctx, err := WithRequestContext(context.Background(), "trace_id", "cancelled-call")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(ctx)
		callDone := make(chan error, 1)
		go func() {
			callDone <- Ref[requestContextCancellation](rt, "cancel").Block(ctx)
		}()
		methodContext := <-started
		if got, _ := RequestContextValue(methodContext, "trace_id"); got != "cancelled-call" {
			t.Fatalf("method Request Context before cancellation = %v, want cancelled-call", got)
		}
		cancel()
		if err := <-callDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call error = %v, want context.Canceled", err)
		}
	})
}

type requestContextCancellation interface {
	Block(context.Context) error
}

type requestContextCancellationEntity struct {
	started chan context.Context
}

type requestContextCancellationRequest struct{}
type requestContextCancellationResponse struct{}
type requestContextCancellationProxy struct {
	invoker Invoker
	id      GrainId
}

func (e *requestContextCancellationEntity) Block(ctx context.Context) error {
	e.started <- ctx
	<-ctx.Done()
	return ctx.Err()
}

func dispatchRequestContextCancellation(ctx context.Context, instance requestContextCancellation, method string, args any, reply any) error {
	if method != "Block" {
		return fmt.Errorf("unknown method %q", method)
	}
	return instance.Block(ctx)
}

func newRequestContextCancellationCall(method string) (args any, reply any) {
	if method != "Block" {
		return nil, nil
	}
	return &requestContextCancellationRequest{}, &requestContextCancellationResponse{}
}

func installRequestContextCancellation(t *testing.T, rt *Runtime, started chan context.Context) {
	t.Helper()
	if err := InstallType[requestContextCancellation](rt, dispatchRequestContextCancellation, func(invoker Invoker, id GrainId) requestContextCancellation {
		return &requestContextCancellationProxy{invoker: invoker, id: id}
	}, newRequestContextCancellationCall, nil); err != nil {
		t.Fatal(err)
	}
	if err := Register[requestContextCancellation](rt, func(*Binder) requestContextCancellation {
		return &requestContextCancellationEntity{started: started}
	}); err != nil {
		t.Fatal(err)
	}
}

func (p *requestContextCancellationProxy) Block(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Block", &requestContextCancellationRequest{}, &requestContextCancellationResponse{})
}

func TestRequestContext_LifecycleAndReminderUseFreshContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1900, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		entities := make(chan *requestContextLifecycleEntity, 1)
		rt := mustNew(t, WithStore(backend), WithClock(fakeClock), WithReminderInterval(time.Second), WithIdleTimeout(0), WithEvictionInterval(0))
		installRequestContextLifecycle(t, rt, entities)

		first, err := WithRequestContext(context.Background(), "trace_id", "activation")
		if err != nil {
			t.Fatal(err)
		}
		ref := Ref[requestContextLifecycle](rt, "lifecycle")
		if _, err := ref.Snapshot(first); err != nil {
			t.Fatalf("first lifecycle call: %v", err)
		}
		entity := <-entities
		second, err := WithRequestContext(context.Background(), "trace_id", "method")
		if err != nil {
			t.Fatal(err)
		}
		result, err := ref.Snapshot(second)
		if err != nil {
			t.Fatalf("second lifecycle call: %v", err)
		}
		if result.Trace != "method" {
			t.Fatalf("method Request Context = %q, want method", result.Trace)
		}
		if got, _ := RequestContextValue(entity.activateContext, "trace_id"); got != "activation" {
			t.Fatalf("OnActivate Request Context = %v, want activation", got)
		}
		rt.Close()
		if _, ok := RequestContextValue(entity.deactivateContext, "trace_id"); ok {
			t.Fatal("OnDeactivate received Request Context")
		}

		// State and Reminder storage use their existing records only. The
		// Reminder delivery below starts with the poller's empty context.
		reminderBackend := store.NewMemory()
		reminderClock := clock.NewFake(start)
		reminderRuntime := mustNew(t, WithStore(reminderBackend), WithClock(reminderClock), WithReminderInterval(time.Second), WithIdleTimeout(0), WithEvictionInterval(0))
		wakeContext := make(chan context.Context, 1)
		installRequestContextReminder(t, reminderRuntime, wakeContext)
		armContext, err := WithRequestContext(context.Background(), "trace_id", "caller")
		if err != nil {
			t.Fatal(err)
		}
		if err := Ref[requestContextReminder](reminderRuntime, "reminder").Arm(armContext); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		rows, err := reminderBackend.ListDue(context.Background(), start)
		if err != nil || len(rows) != 1 {
			t.Fatalf("stored Reminders = (%#v, %v), want one row", rows, err)
		}
		stored, _ := json.Marshal(rows[0])
		if strings.Contains(string(stored), "request_context") {
			t.Fatalf("Reminder record contains Request Context: %s", stored)
		}
		reminderClock.Advance(time.Second)
		synctest.Wait()
		select {
		case wake := <-wakeContext:
			if _, ok := RequestContextValue(wake, "trace_id"); ok {
				t.Fatal("Reminder received Request Context from the Arm Call")
			}
		default:
			t.Fatal("Reminder did not run")
		}
		reminderRuntime.Close()
	})
}

type requestContextLifecycle interface {
	Snapshot(context.Context) (requestContextSnapshotReply, error)
}

type requestContextLifecycleEntity struct {
	activateContext   context.Context
	deactivateContext context.Context
}

type requestContextLifecycleProxy struct {
	invoker Invoker
	id      GrainId
}

func (e *requestContextLifecycleEntity) OnActivate(ctx context.Context) error {
	e.activateContext = ctx
	return nil
}

func (e *requestContextLifecycleEntity) OnDeactivate(ctx context.Context, _ DeactivationReason) error {
	e.deactivateContext = ctx
	return nil
}

func (e *requestContextLifecycleEntity) Snapshot(ctx context.Context) (requestContextSnapshotReply, error) {
	trace, _ := RequestContextValue(ctx, "trace_id")
	return requestContextSnapshotReply{Trace: trace.(string)}, nil
}

func dispatchRequestContextLifecycle(ctx context.Context, instance requestContextLifecycle, method string, args any, reply any) error {
	if method != "Snapshot" {
		return fmt.Errorf("unknown method %q", method)
	}
	value, err := instance.Snapshot(ctx)
	if err == nil {
		reply.(*requestContextSnapshotResponse).R0 = value
	}
	return err
}

func newRequestContextLifecycleCall(method string) (args any, reply any) {
	if method != "Snapshot" {
		return nil, nil
	}
	return &requestContextSnapshotRequest{}, &requestContextSnapshotResponse{}
}

func installRequestContextLifecycle(t *testing.T, rt *Runtime, entities chan *requestContextLifecycleEntity) {
	t.Helper()
	if err := InstallType[requestContextLifecycle](rt, dispatchRequestContextLifecycle, func(invoker Invoker, id GrainId) requestContextLifecycle {
		return &requestContextLifecycleProxy{invoker: invoker, id: id}
	}, newRequestContextLifecycleCall, nil); err != nil {
		t.Fatal(err)
	}
	if err := Register[requestContextLifecycle](rt, func(*Binder) requestContextLifecycle {
		entity := &requestContextLifecycleEntity{}
		entities <- entity
		return entity
	}); err != nil {
		t.Fatal(err)
	}
}

func (p *requestContextLifecycleProxy) Snapshot(ctx context.Context) (requestContextSnapshotReply, error) {
	var reply requestContextSnapshotResponse
	err := p.invoker.Invoke(ctx, p.id, "Snapshot", &requestContextSnapshotRequest{}, &reply)
	return reply.R0, err
}

type requestContextReminder interface {
	Arm(context.Context) error
	Wake(context.Context, TickStatus) error
}

type requestContextReminderEntity struct {
	schedule Reminder[requestContextReminder]
	wake     chan context.Context
}

type requestContextReminderProxy struct {
	invoker Invoker
	id      GrainId
}

type requestContextReminderArmRequest struct{}
type requestContextReminderArmResponse struct{}
type requestContextReminderWakeRequest struct{ A0 TickStatus }
type requestContextReminderWakeResponse struct{}

func (e *requestContextReminderEntity) Arm(ctx context.Context) error {
	return e.schedule.Set(ctx, "wake", After(0), Handle(requestContextReminder.Wake))
}

func (e *requestContextReminderEntity) Wake(ctx context.Context, _ TickStatus) error {
	e.wake <- ctx
	return nil
}

func dispatchRequestContextReminder(ctx context.Context, instance requestContextReminder, method string, args any, reply any) error {
	switch method {
	case "Arm":
		return instance.Arm(ctx)
	case "Wake":
		return instance.Wake(ctx, args.(*requestContextReminderWakeRequest).A0)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newRequestContextReminderCall(method string) (args any, reply any) {
	switch method {
	case "Arm":
		return &requestContextReminderArmRequest{}, &requestContextReminderArmResponse{}
	case "Wake":
		return &requestContextReminderWakeRequest{}, &requestContextReminderWakeResponse{}
	default:
		return nil, nil
	}
}

func newRequestContextReminderCallFromStatus(method string, status TickStatus) (args any, reply any) {
	if method == "Wake" {
		return &requestContextReminderWakeRequest{A0: status}, &requestContextReminderWakeResponse{}
	}
	return nil, nil
}

func installRequestContextReminder(t *testing.T, rt *Runtime, wake chan context.Context) {
	t.Helper()
	if err := InstallType[requestContextReminder](rt, dispatchRequestContextReminder, func(invoker Invoker, id GrainId) requestContextReminder {
		return &requestContextReminderProxy{invoker: invoker, id: id}
	}, newRequestContextReminderCall, newRequestContextReminderCallFromStatus); err != nil {
		t.Fatal(err)
	}
	if err := Register[requestContextReminder](rt, func(b *Binder) requestContextReminder {
		return &requestContextReminderEntity{schedule: NewReminder[requestContextReminder](b), wake: wake}
	}); err != nil {
		t.Fatal(err)
	}
}

func (p *requestContextReminderProxy) Arm(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", &requestContextReminderArmRequest{}, &requestContextReminderArmResponse{})
}

func (p *requestContextReminderProxy) Wake(ctx context.Context, status TickStatus) error {
	return p.invoker.Invoke(ctx, p.id, "Wake", &requestContextReminderWakeRequest{A0: status}, &requestContextReminderWakeResponse{})
}
