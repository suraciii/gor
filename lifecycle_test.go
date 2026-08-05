package gor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// TestRootLifecycle_RejectsNewCallsAfterClose covers the admission boundary:
// once Close has begun, direct Invoke calls return the stable runtime-closed
// code regardless of identity, owner, or method.
func TestRootLifecycle_RejectsNewCallsAfterClose(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	registerAccount(t, rt)
	rt.Close()

	id := Identity{Type: TypeName[Account](), Key: "alice"}
	err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{})
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("invoke after close = %v, want ErrRuntimeClosed", err)
	}
	if code, ok := CodeOf(err); !ok || code != ErrRuntimeClosed {
		t.Fatalf("CodeOf(invoke after close) = (%q, %v), want (%q, true)", code, ok, ErrRuntimeClosed)
	}
}

// TestRootLifecycle_AdmittedCallFinishesDuringClose covers the graceful drain:
// a call admitted before Close began is allowed to finish, while calls arriving
// after Close begins are rejected, and Close returns only after the admitted
// call releases.
func TestRootLifecycle_AdmittedCallFinishesDuringClose(t *testing.T) {
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

		// A call arriving after Close began is rejected at the admission gate.
		if err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, ErrRuntimeClosed) {
			t.Fatalf("invoke during close = %v, want ErrRuntimeClosed", err)
		}
		select {
		case <-closeDone:
			t.Fatal("Close returned before the admitted call released")
		default:
		}

		close(release)
		synctest.Wait()
		if err := <-admittedDone; err != nil {
			t.Fatalf("admitted call error = %v, want nil", err)
		}
		<-closeDone
	})
}

// TestRootLifecycle_QueuedCallRejectedWithoutRunning covers the mailbox rule:
// a call admitted and queued behind a running call is rejected with the stop
// error when Close closes the mailbox, and its method body never executes.
func TestRootLifecycle_QueuedCallRejectedWithoutRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithMailboxCapacity(1), WithIdleTimeout(0), WithEvictionInterval(0))
		running := make(chan struct{})
		release := make(chan struct{})
		var queuedRan atomic.Bool
		installAccountWithDispatch(t, rt, func(ctx context.Context, instance Account, method string, args any, reply any) error {
			switch method {
			case "Block":
				close(running)
				<-release
				return nil
			case "Balance":
				queuedRan.Store(true)
				return dispatchAccount(ctx, instance, method, args, reply)
			default:
				return dispatchAccount(ctx, instance, method, args, reply)
			}
		})
		if err := Register[Account](rt, func(b *Binder) Account {
			return &account{value: NewState[int64](b, "value")}
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		blockDone := make(chan error, 1)
		go func() {
			blockDone <- rt.Invoke(context.Background(), id, "Block", nil, nil)
		}()
		synctest.Wait()
		<-running

		// This call is admitted then queued behind the running Block call.
		queuedDone := make(chan error, 1)
		go func() {
			queuedDone <- rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{})
		}()
		synctest.Wait()

		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()

		// The mailbox serializes calls, so the queued call is rejected only after
		// the running call finishes and Close has closed the mailbox.
		close(release)
		synctest.Wait()
		if err := <-blockDone; err != nil {
			t.Fatalf("running call error = %v, want nil", err)
		}
		if err := <-queuedDone; !errors.Is(err, ErrRuntimeClosed) {
			t.Fatalf("queued call error = %v, want ErrRuntimeClosed", err)
		}
		if queuedRan.Load() {
			t.Fatal("queued call entered its method body before being rejected")
		}
		<-closeDone
	})
}

// TestRootLifecycle_StopIsIdempotent covers repeated and mixed stops: a second
// Close, and a Kill after Close, do not block or start another shutdown.
func TestRootLifecycle_StopIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, rt)

		rt.Close()
		rt.Close()
		rt.Kill()

		select {
		case <-rt.Done():
		default:
			t.Fatal("Done is still open after repeated stop")
		}
	})

	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, rt)

		rt.Kill()
		rt.Kill()
		rt.Close()

		select {
		case <-rt.Done():
		default:
			t.Fatal("Done is still open after repeated kill")
		}
	})
}

// TestRootLifecycle_BecomeDeadOnlyTransitionsRunning is the building block for
// watchCluster's death branch: becomeDead is atomic under the lifecycle lock
// and returns true only when it moved a still-running root to dead. A root
// that Close or Kill already left running keeps its state, so watchCluster can
// branch on this return value without a check-then-act window.
func TestRootLifecycle_BecomeDeadOnlyTransitionsRunning(t *testing.T) {
	rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	if !rt.becomeDead() {
		t.Fatal("becomeDead from running = false, want true")
	}
	if rt.becomeDead() {
		t.Fatal("second becomeDead = true, want false")
	}
	if rt.beginClose() {
		t.Fatal("beginClose after dead = true, want false")
	}
	if rt.beginKill() {
		t.Fatal("beginKill after dead = true, want false")
	}

	closed := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
	closed.beginClose()
	if closed.becomeDead() {
		t.Fatal("becomeDead after beginClose = true, want false (root stays closing)")
	}
}

// TestRootLifecycle_AdmitGatesBeforeEngineClose proves the root admission gate,
// not the engine's own closed check, is the boundary: after beginClose the
// engine is still open, so only admit can reject a call that would otherwise
// execute and succeed.
func TestRootLifecycle_AdmitGatesBeforeEngineClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, rt)
		id := Identity{Type: TypeName[Account](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Deposit", &accountDepositRequest{A0: 5}, &accountDepositReply{}); err != nil {
			t.Fatal(err)
		}

		// Enter closing without closing the engine: Done is closed and admit
		// rejects, but engine.Close has not run.
		rt.beginClose()

		err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{})
		if !errors.Is(err, ErrRuntimeClosed) {
			t.Fatalf("invoke after beginClose with engine still open = %v, want ErrRuntimeClosed", err)
		}
		rt.Close()
	})
}

// TestRootLifecycle_KillAfterCloseEscalates covers the closing-to-killing
// upgrade: Close then Kill both return, and the runtime ends in a stopped
// state that rejects new calls.
func TestRootLifecycle_KillAfterCloseEscalates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := mustNew(t, WithIdleTimeout(0), WithEvictionInterval(0))
		registerAccount(t, rt)

		rt.Close()
		rt.Kill()

		id := Identity{Type: TypeName[Account](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Balance", &accountBalanceRequest{}, &accountBalanceReply{}); !errors.Is(err, ErrRuntimeClosed) {
			t.Fatalf("invoke after close-then-kill = %v, want ErrRuntimeClosed", err)
		}
	})
}
