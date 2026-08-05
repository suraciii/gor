package gor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// TestRootLifecycle_KillDuringCloseEscalatesAndCancels covers the closing to
// killing upgrade end to end. A call admitted before Close started keeps a
// method running and blocks the graceful drain. Kill must escalate: it cancels
// the running method's context, lets the graceful drain finish, and both stops
// return.
func TestRootLifecycle_KillDuringCloseEscalatesAndCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		started := make(chan struct{})
		release := make(chan struct{})
		installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
			if method == "Block" {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return dispatchAccount(ctx, instance, method, args, reply)
		})
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		admittedDone := make(chan error, 1)
		go func() {
			admittedDone <- rt.Invoke(context.Background(), id, "Block", nil, nil)
		}()
		synctest.Wait()
		<-started

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()
		// Close has begun but cannot finish: the admitted method is still running.
		select {
		case <-closeDone:
			t.Fatal("Close returned before Kill escalated")
		default:
		}

		killDone := make(chan struct{})
		go func() {
			rt.Kill()
			close(killDone)
		}()
		synctest.Wait()

		// Escalation cancels the admitted method; it returns a cancellation error.
		if err := <-admittedDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("admitted call error after escalation = %v, want context.Canceled", err)
		}
		<-killDone
		<-closeDone

		if err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, ErrRuntimeClosed) {
			t.Fatalf("invoke after escalation = %v, want ErrRuntimeClosed", err)
		}
	})
}

// TestRootLifecycle_KillDoesNotWaitForUserMethod pins the sudden-stop rule: a
// method that ignores its context keeps running, but Kill still returns. The
// root only waits on its own infrastructure goroutines, never on user code.
// (The Invoke call itself returns a cancellation error because the mailbox sees
// the canceled context; the distinction under test is that the method body,
// running in the mailbox goroutine, is still executing when Kill returns.)
func TestRootLifecycle_KillDoesNotWaitForUserMethod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		started := make(chan struct{})
		release := make(chan struct{})
		var bodyFinished atomic.Bool
		installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
			if method == "Block" {
				close(started)
				<-release
				bodyFinished.Store(true)
				return nil
			}
			return dispatchAccount(ctx, instance, method, args, reply)
		})
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		go func() {
			_ = rt.Invoke(context.Background(), id, "Block", nil, nil)
		}()
		synctest.Wait()
		<-started

		killDone := make(chan struct{})
		go func() {
			rt.Kill()
			close(killDone)
		}()
		synctest.Wait()

		select {
		case <-killDone:
		default:
			t.Fatal("Kill did not return while a user method was still running")
		}
		if bodyFinished.Load() {
			t.Fatal("method body finished before Kill returned")
		}

		close(release)
		synctest.Wait()
		if !bodyFinished.Load() {
			t.Fatal("method body did not finish after release")
		}
	})
}

// TestScenario_ConcurrentCloseDrainsAllAdmitted is the concurrent-close
// scenario: several calls admitted across several entities before Close, more
// calls arriving during Close, and Close returns only after every admitted call
// has released. Late calls are rejected at the admission gate.
func TestScenario_ConcurrentCloseDrainsAllAdmitted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		release := make(chan struct{})
		var ran atomic.Int32
		installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
			if method == "Block" {
				ran.Add(1)
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

		ids := []Identity{
			{Type: TypeName[Account](), Key: "a"},
			{Type: TypeName[Account](), Key: "b"},
			{Type: TypeName[Account](), Key: "c"},
		}
		admitted := make([]chan error, len(ids))
		for i := range ids {
			admitted[i] = make(chan error, 1)
			go func(i int) {
				admitted[i] <- rt.Invoke(context.Background(), ids[i], "Block", nil, nil)
			}(i)
		}
		synctest.Wait()
		if got := ran.Load(); got != int32(len(ids)) {
			t.Fatalf("admitted methods running = %d, want %d", got, len(ids))
		}

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()

		// Calls arriving after Close began are rejected; their method bodies
		// never run.
		for i := range ids {
			if err := rt.Invoke(context.Background(), ids[i], "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, ErrRuntimeClosed) {
				t.Fatalf("late invoke %d = %v, want ErrRuntimeClosed", i, err)
			}
		}
		select {
		case <-closeDone:
			t.Fatal("Close returned before admitted calls released")
		default:
		}

		close(release)
		synctest.Wait()
		for i := range ids {
			if err := <-admitted[i]; err != nil {
				t.Fatalf("admitted call %d error = %v, want nil", i, err)
			}
		}
		<-closeDone
	})
}
