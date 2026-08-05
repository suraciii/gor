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
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

func TestRuntime_HandleInvokesMethodAndEncodesReply(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":4}}`))
	if err != nil {
		t.Fatalf("Handle error = %v", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != nil {
		t.Fatalf("response error = %#v, want empty", response.Error)
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

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Fail","args":{}}`))
	if err != nil {
		t.Fatalf("Handle error = %v, want nil for method error", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Message != "method failed" || response.Error.Code != "" {
		t.Fatalf("response error = %#v, want opaque method failed", response.Error)
	}
}

func TestRuntime_HandlePrioritizesBusinessErrorOverReplyEncoding(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	installEnvelopeAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.envelopeAccount","key":"alice","method":"Fail","args":{}}`))
	if err != nil {
		t.Fatalf("handle business error = %v", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode business error response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(envelopeFailureCode) || response.Reply != nil {
		t.Fatalf("business error response = %#v, want coded error without reply", response)
	}

	payload, err = rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.envelopeAccount","key":"alice","method":"Succeed","args":{}}`))
	if err != nil {
		t.Fatalf("handle reply encoding error = %v", err)
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode reply encoding response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrReplyEncodeFailed) || response.Reply != nil {
		t.Fatalf("reply encoding response = %#v, want reply-encode-failed without reply", response)
	}
}

func TestRuntime_HandleRejectsUnknownMethod(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Missing","args":{}}`))
	if err != nil {
		t.Fatalf("Handle error = %v, want nil for method error", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrUnknownMethod) {
		t.Fatalf("response error = %#v, want unknown-method code", response.Error)
	}
}

func TestRuntime_HandleRejectsUnregisteredType(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"missing.Account","key":"alice","method":"Balance","args":{}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrTypeNotInstalled) {
		t.Fatalf("response error = %#v, want type-not-installed code", response.Error)
	}
}

func TestRuntime_HandleRejectsBadJSON(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrInvalidRequest) || !strings.Contains(response.Error.Message, "decode invocation request") {
		t.Fatalf("response error = %#v, want invalid request error", response.Error)
	}
}

func TestRuntime_HandleRejectsBadArguments(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	defer rt.Close()
	registerAccount(t, rt)

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Deposit","args":{"A0":"wrong"}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrInvalidRequest) || !strings.Contains(response.Error.Message, "decode Deposit arguments") {
		t.Fatalf("response error = %#v, want invalid argument error", response.Error)
	}
}

func TestRuntime_HandleRejectsClosedRuntime(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	rt.Close()

	payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
	if err != nil {
		t.Fatalf("handle error = %v, want nil", err)
	}
	var response callResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error == nil || response.Error.Code != string(ErrRuntimeClosed) {
		t.Fatalf("response error = %#v, want runtime-closed code", response.Error)
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

		payload, err := rt.handle(context.Background(), []byte(`{"kind":"invoke","type":"gor.Account","key":"alice","method":"Balance","args":{}}`))
		if err != nil {
			t.Fatalf("handle error = %v, want nil", err)
		}
		var response callResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Error == nil || response.Error.Code != string(ErrRuntimeClosed) {
			t.Fatalf("response error = %#v, want runtime-closed code", response.Error)
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
		if err == nil || !strings.Contains(err.Error(), "remote failure") {
			t.Fatalf("forwarded method error = %v, want remote failure diagnostic", err)
		}
		if errors.Is(err, remoteFailure) {
			t.Fatal("forwarded error retained the remote sentinel")
		}
		var typedFailure *routedFailureError
		if errors.As(err, &typedFailure) {
			t.Fatal("forwarded error retained the remote error type")
		}
		assertRoutedFailureCode(t, err)

		local := findForwardTarget(t, first, "node-a")
		assertRoutedFailureCode(t, first.Invoke(context.Background(), local, "Fail", &routedAccountFailRequest{}, nil))
		opaqueErr := first.Invoke(context.Background(), remote, "Opaque", &routedAccountOpaqueRequest{}, nil)
		if opaqueErr == nil {
			t.Fatal("opaque forwarded error was dropped")
		}
		if errors.Is(opaqueErr, remoteFailure) {
			t.Fatal("opaque forwarded error retained the remote sentinel")
		}
		if got, ok := CodeOf(opaqueErr); ok {
			t.Fatalf("CodeOf(opaque forwarded error) = (%q, true), want no code", got)
		}

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
		if err == nil || !errors.Is(err, ErrTransportFailed) || !strings.Contains(err.Error(), "decode invocation response") {
			t.Fatalf("invalid response envelope error = %v, want transport failure", err)
		}

		firstTransport.sendResponse = []byte(`{"reply":"invalid"}`)
		var invalidReply routedAccountWhoReply
		err = first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, &invalidReply)
		if err == nil || !errors.Is(err, ErrTransportFailed) || !strings.Contains(err.Error(), "decode Who reply") {
			t.Fatalf("invalid response reply error = %v, want transport failure", err)
		}
		firstTransport.sendResponse = nil

		beforeEncodeFailure := firstTransport.sends.Load()
		err = first.Invoke(context.Background(), remote, "Who", &unmarshalableArgs{Callback: func() {}}, nil)
		if err == nil || !errors.Is(err, ErrRequestEncodeFailed) {
			t.Fatalf("request encoding error = %v, want request-encode-failed", err)
		}
		if got := firstTransport.sends.Load(); got != beforeEncodeFailure {
			t.Fatalf("request encoding sent %d transport requests", got-beforeEncodeFailure)
		}

		sendError := errors.New("network down")
		firstTransport.sendError = sendError
		beforeFailure := firstTransport.sends.Load()
		err = first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, nil)
		if err == sendError || !errors.Is(err, ErrTransportFailed) || !errors.Is(err, sendError) {
			t.Fatalf("transport error = %v, want transport-failed wrapping %v", err, sendError)
		}
		if got := firstTransport.sends.Load(); got != beforeFailure+1 {
			t.Fatalf("transport sends after failure = %d, want one attempt", got-beforeFailure)
		}
	})
}

func TestRuntime_ForwardedCallsUseLocalInvokeSerialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1225, 0).UTC()
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
		installRoutedAccountWithFactory(t, second, func(*Binder) routedAccount {
			return &routedAccountEntity{
				label:        "node-b",
				blockStarted: started,
				blockRelease: release,
			}
		})
		synctest.Wait()
		<-firstTransport.served
		<-secondTransport.served
		fakeClock.Advance(time.Second)
		synctest.Wait()

		remote := findForwardTarget(t, first, "node-b")
		blockDone := make(chan error, 1)
		go func() {
			blockDone <- first.Invoke(context.Background(), remote, "Block", &routedAccountBlockRequest{}, &routedAccountBlockReply{})
		}()
		<-started

		whoDone := make(chan struct {
			reply routedAccountWhoReply
			err   error
		}, 1)
		go func() {
			var reply routedAccountWhoReply
			err := first.Invoke(context.Background(), remote, "Who", &routedAccountWhoRequest{}, &reply)
			whoDone <- struct {
				reply routedAccountWhoReply
				err   error
			}{reply: reply, err: err}
		}()
		synctest.Wait()
		select {
		case result := <-whoDone:
			t.Fatalf("forwarded call bypassed local serialization: (%q, %v)", result.reply.R0, result.err)
		default:
		}

		close(release)
		synctest.Wait()
		if err := <-blockDone; err != nil {
			t.Fatalf("forwarded blocking call error = %v", err)
		}
		result := <-whoDone
		if result.err != nil || result.reply.R0 != "node-b" {
			t.Fatalf("serialized forwarded call = (%q, %v), want (node-b, nil)", result.reply.R0, result.err)
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
		if !errors.Is(err, ErrNodeDead) {
			t.Fatalf("invocation on a dead node error = %v, want ErrNodeDead", err)
		}
		if got := fakeTransport.sends.Load(); got != 0 {
			t.Fatalf("invocation on a dead node sent %d transport requests", got)
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
		for range 2 {
			select {
			case <-closeDone:
				t.Fatal("concurrent Runtime.Close returned while transport Serve was held")
			default:
			}
		}
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
	Opaque(context.Context) error
}

type envelopeAccount interface {
	Fail(context.Context) error
	Succeed(context.Context) error
}

type envelopeAccountRequest struct{}

type envelopeAccountReply struct {
	Callback func()
}

type envelopeAccountEntity struct{}

const envelopeFailureCode Code = "test.envelope_failure"

func (*envelopeAccountEntity) Fail(context.Context) error {
	return fmt.Errorf("business failure: %w", envelopeFailureCode)
}

func (*envelopeAccountEntity) Succeed(context.Context) error {
	return nil
}

func dispatchEnvelopeAccount(ctx context.Context, instance envelopeAccount, method string, args any, reply any) error {
	typedReply := reply.(*envelopeAccountReply)
	typedReply.Callback = func() {}
	switch method {
	case "Fail":
		return instance.Fail(ctx)
	case "Succeed":
		return instance.Succeed(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newEnvelopeAccountCall(method string) (args any, reply any) {
	switch method {
	case "Fail", "Succeed":
		return &envelopeAccountRequest{}, &envelopeAccountReply{}
	default:
		return nil, nil
	}
}

func installEnvelopeAccount(t *testing.T, rt *Runtime) {
	t.Helper()
	if err := InstallType[envelopeAccount](rt, dispatchEnvelopeAccount, func(invoker Invoker, id Identity) envelopeAccount {
		return &envelopeAccountProxy{invoker: invoker, id: id}
	}, newEnvelopeAccountCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[envelopeAccount](rt, func(*Binder) envelopeAccount {
		return &envelopeAccountEntity{}
	}); err != nil {
		t.Fatal(err)
	}
}

type envelopeAccountProxy struct {
	invoker Invoker
	id      Identity
}

func (p *envelopeAccountProxy) Fail(ctx context.Context) error {
	var reply envelopeAccountReply
	return p.invoker.Invoke(ctx, p.id, "Fail", &envelopeAccountRequest{}, &reply)
}

func (p *envelopeAccountProxy) Succeed(ctx context.Context) error {
	var reply envelopeAccountReply
	return p.invoker.Invoke(ctx, p.id, "Succeed", &envelopeAccountRequest{}, &reply)
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
type routedAccountOpaqueRequest struct{}
type routedAccountOpaqueReply struct{}
type accountFailRequest struct{}
type accountFailReply struct{}

func assertRoutedFailureCode(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, routedFailureCode) {
		t.Fatalf("method error = %v, want routed failure code", err)
	}
	if got, ok := CodeOf(err); !ok || got != routedFailureCode {
		t.Fatalf("CodeOf(error) = (%q, %v), want (%q, true)", got, ok, routedFailureCode)
	}
}

const routedFailureCode Code = "test.routed_failure"

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
	return fmt.Errorf("remote failure: %w", routedFailureCode)
}

func (*routedAccountEntity) Opaque(context.Context) error {
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
	case "Opaque":
		return instance.Opaque(ctx)
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
	case "Opaque":
		return &routedAccountOpaqueRequest{}, &routedAccountOpaqueReply{}
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

func (p *routedAccountProxy) Opaque(ctx context.Context) error {
	var reply routedAccountOpaqueReply
	return p.invoker.Invoke(ctx, p.id, "Opaque", &routedAccountOpaqueRequest{}, &reply)
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
	probeSends   atomic.Int32
	sendError    error
	sendResponse []byte

	// inflight counts handler deliveries that have not reached the caller;
	// drained is armed while Close waits for them. Both are guarded by mu.
	inflight int
	drained  chan struct{}
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
	var request struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(payload, &request); err == nil && request.Kind == requestKindProbe {
		t.probeSends.Add(1)
	} else {
		t.sends.Add(1)
	}
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
	peer.inflight++
	peer.mu.Unlock()
	if handler == nil {
		peer.finishDelivery()
		return nil, fmt.Errorf("transport address %q is not serving", addr)
	}
	result := make(chan testTransportResult, 1)
	go func() {
		response, err := handler(context.Background(), payload)
		result <- testTransportResult{payload: response, err: err}
	}()
	select {
	case result := <-result:
		peer.finishDelivery()
		return result.payload, result.err
	case <-peer.closed:
		peer.finishDelivery()
		return nil, fmt.Errorf("transport %q closed during call", addr)
	case <-ctx.Done():
		peer.finishDelivery()
		return nil, ctx.Err()
	}
}

// finishDelivery releases one in-flight delivery; the last one closes the
// drain that Close is waiting on. The handoff happens before the caller sees
// the result, so a graceful Close cannot complete while a caller is still
// owed a reply.
func (t *testTransport) finishDelivery() {
	t.mu.Lock()
	t.inflight--
	if t.inflight == 0 && t.drained != nil {
		close(t.drained)
		t.drained = nil
	}
	t.mu.Unlock()
}

// Close stops serving gracefully: new deliveries keep running (the runtime's
// admission gate rejects them), but Close waits until every in-flight handler
// result has reached its caller before closing the closed signal.
func (t *testTransport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		if t.inflight == 0 {
			close(t.closed)
			t.mu.Unlock()
			return
		}
		drained := make(chan struct{})
		t.drained = drained
		t.mu.Unlock()
		<-drained
		close(t.closed)
	})
	return nil
}

// Kill stops serving abruptly: in-flight deliveries are truncated at the
// caller by the closed signal and no result is waited for.
func (t *testTransport) Kill() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

// recordingTransport wraps a testTransport and records which stop method the
// runtime invoked. The runtime-level contract — graceful stop calls Close,
// abrupt stop and the Kill escalation call Kill — is a routing decision that
// no flush-level test can observe deterministically: the reply delivery and
// the transport close race under the bubble scheduler, and the delivery
// always wins the wakeup order. Recording the method pins the routing.
type recordingTransport struct {
	*testTransport
	closeCalls chan struct{}
	killCalls  chan struct{}
}

func (r *recordingTransport) Close() error {
	r.closeCalls <- struct{}{}
	return r.testTransport.Close()
}

func (r *recordingTransport) Kill() error {
	r.killCalls <- struct{}{}
	return r.testTransport.Kill()
}

// TestRuntime_StopModeRouting pins which transport stop method each root stop
// path selects: a graceful stop calls Close and never Kill; a Kill escalation
// reaches the transport as Kill even when a Close already completed.
func TestRuntime_StopModeRouting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1750, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		network := newTestTransportNetwork()
		base := network.add("node-a")
		recorded := &recordingTransport{
			testTransport: base,
			closeCalls:    make(chan struct{}, 4),
			killCalls:     make(chan struct{}, 4),
		}
		options := clusterRuntimeOptions(store.NewMemory(), members, fakeClock, "node-a", "generation-a", recorded)
		rt := mustNew(t, options...)
		synctest.Wait()
		<-base.served

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()
		select {
		case <-recorded.closeCalls:
		default:
			t.Fatal("graceful stop did not call Transport.Close")
		}
		select {
		case <-recorded.killCalls:
			t.Fatal("graceful stop called Transport.Kill")
		default:
		}
		select {
		case <-closeDone:
		default:
			t.Fatal("runtime Close did not return")
		}

		// A Kill after a completed graceful stop still reaches the transport
		// as Kill: the transport must not treat the later Kill as a no-op.
		killDone := make(chan struct{})
		go func() {
			rt.Kill()
			close(killDone)
		}()
		synctest.Wait()
		select {
		case <-recorded.killCalls:
		default:
			t.Fatal("Kill after graceful stop did not call Transport.Kill")
		}
		select {
		case <-killDone:
		default:
			t.Fatal("runtime Kill did not return")
		}
	})
}

type testTransportResult struct {
	payload []byte
	err     error
}
