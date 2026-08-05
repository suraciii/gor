package gor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	runtimepkg "github.com/suraciii/gor/runtime"
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
		if !rt.shuttingDown.Load() {
			t.Fatal("runtime did not enter shutting down state")
		}
		select {
		case <-rt.done:
			t.Fatal("runtime done closed before Close drained the running call")
		default:
		}

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
