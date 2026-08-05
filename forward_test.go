package gor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

func TestRuntime_HandleInvokesMethodAndEncodesReply(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":4}}`))
	if err != nil {
		t.Fatalf("Handle error = %v", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != "" {
		t.Fatalf("response error = %q, want empty", response.Error)
	}
	var reply accountDepositReply
	if err := json.Unmarshal(response.Reply, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.R0 != 4 {
		t.Fatalf("reply = %#v, want R0=4", reply)
	}
}

func TestRuntime_HandleReturnsMethodErrorInResponse(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
		if method == "Fail" {
			return errors.New("method failed")
		}
		return dispatchAccount(ctx, instance, method, args, reply)
	})
	if err := Register[Account](rt, func(b *Binder) Account {
		return &account{value: NewState[int64](b, "value")}
	}); err != nil {
		t.Fatal(err)
	}

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Fail","args":{}}`))
	if err != nil {
		t.Fatalf("Handle error = %v, want nil for method error", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != "method failed" {
		t.Fatalf("response error = %q, want method failed", response.Error)
	}
}

func TestRuntime_HandleRejectsUnknownMethod(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Missing","args":{}}`))
	if err != nil {
		t.Fatalf("Handle error = %v, want nil for method error", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == "" {
		t.Fatal("unknown method response has no error text")
	}
}

func TestRuntime_HandleRejectsUnregisteredType(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"type":"missing.Account","key":"alice","method":"Balance","args":{}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(response.Error, "entity type is not installed") {
		t.Fatalf("response error = %q, want an unregistered type error", response.Error)
	}
}

func TestRuntime_HandleRejectsBadJSON(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(response.Error, "decode invocation request") {
		t.Fatalf("response error = %q, want a request decode error", response.Error)
	}
}

func TestRuntime_HandleRejectsBadArguments(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":"wrong"}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(response.Error, "decode Deposit arguments") {
		t.Fatalf("response error = %q, want an argument decode error", response.Error)
	}
}

func TestRuntime_HandleRejectsClosedRuntime(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != runtimepkg.ErrRuntimeClosed.Error() {
		t.Fatalf("response error = %q, want %q", response.Error, runtimepkg.ErrRuntimeClosed.Error())
	}
}

func TestRuntime_HandleRejectsWhileClosing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		started := make(chan struct{})
		release := make(chan struct{})
		installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
			if method == "Block" {
				close(started)
				<-release
				return nil
			}
			return dispatchAccount(ctx, instance, method, args, reply)
		})
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		callDone := make(chan error, 1)
		go func() {
			callDone <- rt.Invoke(context.Background(), Identity{Type: TypeName[Account](), Key: "alice"}, "Block", nil, nil)
		}()
		synctest.Wait()
		<-started

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()

		payload, err := rt.handle(context.Background(), []byte(`{"type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
		if err != nil {
			t.Fatalf("handle error = %v, want nil", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error != runtimepkg.ErrRuntimeClosed.Error() {
			t.Fatalf("response error = %q, want %q", response.Error, runtimepkg.ErrRuntimeClosed.Error())
		}

		close(release)
		synctest.Wait()
		if err := <-callDone; err != nil {
			t.Fatalf("running call error = %v", err)
		}
		<-closeDone
	})
}

func TestRuntime_InvokeForwardsToOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1200, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstTransport := network.add("node-a")
		secondTransport := network.add("node-b")
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions, WithTransport(firstTransport))
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")
		secondOptions = append(secondOptions, WithTransport(secondTransport))
		first := mustNew(t, firstOptions...)
		second := mustNew(t, secondOptions...)
		defer first.Close()
		defer second.Close()
		installRoutedAccount(t, first, "node-a")
		installRoutedAccount(t, second, "node-b")
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served

		fakeClock.Advance(time.Second)
		synctest.Wait()
		remote := findForwardTarget(t, first, "node-b")
		var remoteReply routedAccountEchoReply
		if err := first.Invoke(context.Background(), remote, "Echo", &routedAccountEchoRequest{A0: "payload"}, &remoteReply); err != nil {
			t.Fatalf("forwarded invocation error = %v", err)
		}
		if remoteReply.R0 != "node-b:payload" {
			t.Fatalf("forwarded reply = %q, want node-b:payload execution", remoteReply.R0)
		}
		if got := firstTransport.sends.Load(); got != 1 {
			t.Fatalf("forward sends = %d, want 1", got)
		}

		err := first.Invoke(context.Background(), remote, "Fail", &routedAccountFailRequest{}, nil)
		if err == nil || err.Error() != "remote failure" {
			t.Fatalf("forwarded method error = %v, want remote failure", err)
		}
		if errors.Is(err, remoteFailure) {
			t.Fatal("forwarded error retained the remote sentinel")
		}
		var typedFailure *routedFailureError
		if errors.As(err, &typedFailure) {
			t.Fatal("forwarded error retained the remote error type")
		}

		local := findForwardTarget(t, first, "node-a")
		beforeLocal := firstTransport.sends.Load()
		var localReply routedAccountWhoReply
		if err := first.Invoke(context.Background(), local, "Who", &routedAccountWhoRequest{}, &localReply); err != nil {
			t.Fatalf("local invocation error = %v", err)
		}
		if localReply.R0 != "node-a" {
			t.Fatalf("local reply = %q, want node-a execution", localReply.R0)
		}
		if got := firstTransport.sends.Load(); got != beforeLocal {
			t.Fatalf("local invocation sent %d transport requests, want none", got-beforeLocal)
		}

		var unmarshalableReply routedAccountWhoReply
		if err := first.Invoke(context.Background(), local, "Who", &unmarshalableArgs{Callback: func() {}}, &unmarshalableReply); err != nil {
			t.Fatalf("local invocation with unmarshalable args error = %v", err)
		}
		if unmarshalableReply.R0 != "node-a" {
			t.Fatalf("local invocation with unmarshalable args reply = %q, want node-a", unmarshalableReply.R0)
		}
		if got := firstTransport.sends.Load(); got != beforeLocal {
			t.Fatalf("local unmarshalable invocation sent %d transport requests, want none", got-beforeLocal)
		}

		firstTransport.sendResponse = []byte("{")
		err = first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, nil)
		if err == nil || !strings.Contains(err.Error(), "decode invocation response") {
			t.Fatalf("invalid response envelope error = %v, want response decode error", err)
		}

		firstTransport.sendResponse = []byte(`{"reply":"invalid","error":""}`)
		var invalidReply routedAccountWhoReply
		err = first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, &invalidReply)
		if err == nil || !strings.Contains(err.Error(), "decode Who reply") {
			t.Fatalf("invalid response reply error = %v, want reply decode error", err)
		}
		firstTransport.sendResponse = nil

		sendError := errors.New("network down")
		firstTransport.sendError = sendError
		beforeFailure := firstTransport.sends.Load()
		err = first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, nil)
		if err != sendError {
			t.Fatalf("transport error = %v, want original error %v", err, sendError)
		}
		if got := firstTransport.sends.Load(); got != beforeFailure+1 {
			t.Fatalf("transport sends after failure = %d, want one attempt", got-beforeFailure)
		}
	})
}

func TestRuntime_HandleDoesNotRouteAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1250, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstTransport := network.add("node-a")
		secondTransport := network.add("node-b")
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions, WithTransport(firstTransport))
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")
		secondOptions = append(secondOptions, WithTransport(secondTransport))
		first := mustNew(t, firstOptions...)
		second := mustNew(t, secondOptions...)
		defer first.Close()
		defer second.Close()
		installRoutedAccount(t, first, "node-a")
		installRoutedAccount(t, second, "node-b")
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		fakeClock.Advance(time.Second)
		synctest.Wait()

		remote := findForwardTarget(t, first, "node-b")
		conflictingView := cluster.NewView([]store.Member{{
			NodeAddr:   "node-a",
			Generation: "generation-a",
			Status:     store.MemberActive,
		}})
		second.clusterView.Store(&conflictingView)
		var reply routedAccountEchoReply
		if err := first.Invoke(context.Background(), remote, "Echo", &routedAccountEchoRequest{A0: "direct"}, &reply); err != nil {
			t.Fatalf("forwarded invocation error = %v", err)
		}
		if reply.R0 != "node-b:direct" {
			t.Fatalf("forwarded reply = %q, want direct node-b execution", reply.R0)
		}
		if got := firstTransport.sends.Load(); got != 1 {
			t.Fatalf("initial transport sends = %d, want 1", got)
		}
		if got := secondTransport.sends.Load(); got != 0 {
			t.Fatalf("second transport sends = %d, want 0 after direct handling", got)
		}
	})
}

func TestRuntime_ForwardCancellationDoesNotCancelRemote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1275, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		firstTransport := network.add("node-a")
		secondTransport := network.add("node-b")
		firstOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a")
		firstOptions = append(firstOptions, WithTransport(firstTransport))
		secondOptions := clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b")
		secondOptions = append(secondOptions, WithTransport(secondTransport))
		first := mustNew(t, firstOptions...)
		second := mustNew(t, secondOptions...)
		defer first.Close()
		defer second.Close()
		installRoutedAccount(t, first, "node-a")
		started := make(chan struct{})
		release := make(chan struct{})
		observedContext := make(chan context.Context)
		finished := make(chan struct{})
		installRoutedAccountWithFactory(t, second, func(*Binder) routedAccount {
			return &routedAccountEntity{
				label:         "node-b",
				blockStarted:  started,
				blockRelease:  release,
				blockContext:  observedContext,
				blockFinished: finished,
			}
		})
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		fakeClock.Advance(time.Second)
		synctest.Wait()

		remote := findForwardTarget(t, first, "node-b")
		ctx, cancel := context.WithCancel(context.Background())
		callDone := make(chan error, 1)
		go func() {
			callDone <- first.Invoke(ctx, remote, "Block", &routedAccountBlockRequest{}, &routedAccountBlockReply{})
		}()
		<-started
		remoteContext := <-observedContext
		cancel()
		synctest.Wait()
		select {
		case err := <-callDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled forwarded invocation error = %v, want context.Canceled", err)
			}
		default:
			close(release)
			synctest.Wait()
			t.Fatal("cancelled forwarded invocation did not return")
		}
		if _, ok := remoteContext.Deadline(); ok {
			t.Fatal("remote forwarded context unexpectedly has a deadline")
		}
		if err := remoteContext.Err(); err != nil {
			t.Fatalf("remote forwarded context was cancelled: %v", err)
		}
		select {
		case <-finished:
			t.Fatal("remote invocation finished before release")
		default:
		}
		close(release)
		synctest.Wait()
		select {
		case <-finished:
		default:
			t.Fatal("remote invocation did not finish after release")
		}
	})
}

func TestRuntime_InvokeDoesNotSendWithoutOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1300, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		network := newTestTransportNetwork()
		fakeTransport := network.add("node-a")
		options := clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a")
		options = append(options, WithTransport(fakeTransport))
		rt := mustNew(t, options...)
		defer rt.Close()
		installRoutedAccount(t, rt, "node-a")
		synctest.Wait()
		<-fakeTransport.served

		self := findClusterMember(t, members, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := members.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}
		fakeClock.Advance(time.Second)
		synctest.Wait()

		var reply routedAccountWhoReply
		err := rt.Invoke(context.Background(), Identity{Type: TypeName[routedAccount](), Key: "alice"}, "Who", &routedAccountWhoRequest{}, &reply)
		var wrongOwner WrongOwnerError
		if !errors.As(err, &wrongOwner) || wrongOwner.Owner != "" {
			t.Fatalf("invocation without owner error = %v, want empty-owner WrongOwnerError", err)
		}
		if got := fakeTransport.sends.Load(); got != 0 {
			t.Fatalf("invocation without owner sent %d transport requests", got)
		}
	})
}

func TestRuntime_StartsAndClosesConfiguredTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1400, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		fakeTransport := newTestTransportNetwork().add("node-a")
		fakeTransport.serveHold = make(chan struct{})
		options := clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a")
		options = append(options, WithTransport(fakeTransport))
		rt := mustNew(t, options...)
		<-fakeTransport.served

		closeDone := make(chan struct{}, 2)
		go func() {
			rt.Close()
			closeDone <- struct{}{}
		}()
		go func() {
			rt.Close()
			closeDone <- struct{}{}
		}()
		synctest.Wait()
		close(fakeTransport.serveHold)
		synctest.Wait()
		for range 2 {
			select {
			case <-closeDone:
			default:
				t.Fatal("concurrent Runtime.Close did not return")
			}
		}
		select {
		case <-fakeTransport.closed:
		default:
			t.Fatal("configured transport was not closed")
		}
		select {
		case <-fakeTransport.serveDone:
		default:
			t.Fatal("transport Serve is still running after Runtime.Close")
		}
	})
}

type routedAccount interface {
	Who(context.Context) (string, error)
	Echo(context.Context, string) (string, error)
	Block(context.Context) error
	Fail(context.Context) error
}

type routedAccountEntity struct {
	label         string
	blockStarted  chan struct{}
	blockRelease  chan struct{}
	blockContext  chan context.Context
	blockFinished chan struct{}
}

type routedAccountWhoRequest struct{}
type routedAccountWhoReply struct {
	R0 string
}
type routedAccountEchoRequest struct {
	A0 string
}
type routedAccountEchoReply struct {
	R0 string
}
type routedAccountBlockRequest struct{}
type routedAccountBlockReply struct{}
type routedAccountFailRequest struct{}
type routedAccountFailReply struct{}

type routedFailureError struct{}

func (*routedFailureError) Error() string {
	return "remote failure"
}

var remoteFailure = &routedFailureError{}

type unmarshalableArgs struct {
	Callback func()
}

func (a *routedAccountEntity) Who(context.Context) (string, error) {
	return a.label, nil
}

func (a *routedAccountEntity) Echo(_ context.Context, value string) (string, error) {
	return a.label + ":" + value, nil
}

func (a *routedAccountEntity) Block(ctx context.Context) error {
	if a.blockStarted != nil {
		close(a.blockStarted)
	}
	if a.blockContext != nil {
		a.blockContext <- ctx
	}
	<-a.blockRelease
	if a.blockFinished != nil {
		close(a.blockFinished)
	}
	return nil
}

func (*routedAccountEntity) Fail(context.Context) error {
	return remoteFailure
}

func dispatchRoutedAccount(ctx context.Context, instance routedAccount, method string, args any, reply any) error {
	switch method {
	case "Who":
		value, err := instance.Who(ctx)
		if err == nil {
			reply.(*routedAccountWhoReply).R0 = value
		}
		return err
	case "Echo":
		value, err := instance.Echo(ctx, args.(*routedAccountEchoRequest).A0)
		if err == nil {
			reply.(*routedAccountEchoReply).R0 = value
		}
		return err
	case "Block":
		return instance.Block(ctx)
	case "Fail":
		return instance.Fail(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newRoutedAccountCall(method string) (args any, reply any) {
	switch method {
	case "Who":
		return &routedAccountWhoRequest{}, &routedAccountWhoReply{}
	case "Echo":
		return &routedAccountEchoRequest{}, &routedAccountEchoReply{}
	case "Block":
		return &routedAccountBlockRequest{}, &routedAccountBlockReply{}
	case "Fail":
		return &routedAccountFailRequest{}, &routedAccountFailReply{}
	default:
		return nil, nil
	}
}

func installRoutedAccount(t *testing.T, rt *Runtime, label string) {
	installRoutedAccountWithFactory(t, rt, func(*Binder) routedAccount {
		return &routedAccountEntity{label: label}
	})
}

func installRoutedAccountWithFactory(t *testing.T, rt *Runtime, factory func(*Binder) routedAccount) {
	t.Helper()
	if err := InstallType[routedAccount](rt, dispatchRoutedAccount, func(invoker Invoker, id Identity) routedAccount {
		return &routedAccountProxy{invoker: invoker, id: id}
	}, newRoutedAccountCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[routedAccount](rt, factory); err != nil {
		t.Fatal(err)
	}
}

type routedAccountProxy struct {
	invoker Invoker
	id      Identity
}

func (p *routedAccountProxy) Echo(ctx context.Context, value string) (string, error) {
	var reply routedAccountEchoReply
	err := p.invoker.Invoke(ctx, p.id, "Echo", &routedAccountEchoRequest{A0: value}, &reply)
	return reply.R0, err
}

func (p *routedAccountProxy) Block(ctx context.Context) error {
	var reply routedAccountBlockReply
	return p.invoker.Invoke(ctx, p.id, "Block", &routedAccountBlockRequest{}, &reply)
}

func (p *routedAccountProxy) Who(ctx context.Context) (string, error) {
	var reply routedAccountWhoReply
	err := p.invoker.Invoke(ctx, p.id, "Who", &routedAccountWhoRequest{}, &reply)
	return reply.R0, err
}

func (p *routedAccountProxy) Fail(ctx context.Context) error {
	var reply routedAccountFailReply
	return p.invoker.Invoke(ctx, p.id, "Fail", &routedAccountFailRequest{}, &reply)
}

func findForwardTarget(t *testing.T, rt *Runtime, owner string) Identity {
	t.Helper()
	view := rt.clusterView.Load()
	for index := 0; index < 4096; index++ {
		candidate := Identity{Type: TypeName[routedAccount](), Key: fmt.Sprintf("forward-%d", index)}
		candidateOwner, ok := cluster.Owner(*view, store.Identity(candidate))
		if ok && candidateOwner == owner {
			return candidate
		}
	}
	t.Fatalf("no identity owned by %q", owner)
	return Identity{}
}

type testTransportNetwork struct {
	mu         sync.Mutex
	transports map[string]*testTransport
}

func newTestTransportNetwork() *testTransportNetwork {
	return &testTransportNetwork{transports: make(map[string]*testTransport)}
}

func (n *testTransportNetwork) add(addr string) *testTransport {
	t := &testTransport{
		network:   n,
		addr:      addr,
		served:    make(chan struct{}),
		closed:    make(chan struct{}),
		serveDone: make(chan struct{}),
	}
	n.mu.Lock()
	n.transports[addr] = t
	n.mu.Unlock()
	return t
}

type testTransport struct {
	network *testTransportNetwork
	addr    string

	mu      sync.Mutex
	handler transport.Handler

	served       chan struct{}
	closed       chan struct{}
	serveDone    chan struct{}
	serveHold    chan struct{}
	serveOnce    sync.Once
	closeOnce    sync.Once
	sends        atomic.Int32
	sendError    error
	sendResponse []byte
}

var _ transport.Transport = (*testTransport)(nil)

func (t *testTransport) Addr() string {
	return t.addr
}

func (t *testTransport) Serve(ctx context.Context, handler transport.Handler) error {
	t.mu.Lock()
	t.handler = handler
	t.mu.Unlock()
	t.serveOnce.Do(func() { close(t.served) })
	defer close(t.serveDone)
	select {
	case <-ctx.Done():
	case <-t.closed:
	}
	if t.serveHold != nil {
		<-t.serveHold
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return nil
	}
}

func (t *testTransport) Send(ctx context.Context, addr string, payload []byte) ([]byte, error) {
	t.sends.Add(1)
	if t.sendError != nil {
		return nil, t.sendError
	}
	if t.sendResponse != nil {
		return t.sendResponse, nil
	}
	t.network.mu.Lock()
	peer := t.network.transports[addr]
	t.network.mu.Unlock()
	if peer == nil {
		return nil, fmt.Errorf("unknown transport address %q", addr)
	}
	peer.mu.Lock()
	handler := peer.handler
	peer.mu.Unlock()
	if handler == nil {
		return nil, fmt.Errorf("transport address %q is not serving", addr)
	}
	result := make(chan testTransportResult, 1)
	go func() {
		response, err := handler(context.Background(), payload)
		result <- testTransportResult{payload: response, err: err}
	}()
	select {
	case result := <-result:
		return result.payload, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *testTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type testTransportResult struct {
	payload []byte
	err     error
}
