package runtime

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/mail"
)

type testEntity struct{}

func TestRuntime_ConcurrentFirstCallsDeduplicateActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 4})
		defer rt.Close()

		factoryStarted := make(chan struct{})
		releaseFactory := make(chan struct{})
		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				factoryCalls.Add(1)
				close(factoryStarted)
				<-releaseFactory
				return &testEntity{}, nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				if method != "Ping" {
					return errors.New("unknown method")
				}
				*(reply.(*int)) = int(factoryCalls.Load())
				if instance == nil {
					return errors.New("missing instance")
				}
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		firstReply := new(int)
		secondReply := new(int)
		go func() {
			firstDone <- rt.Invoke(context.Background(), id, "Ping", nil, firstReply)
		}()
		synctest.Wait()
		<-factoryStarted
		go func() {
			secondDone <- rt.Invoke(context.Background(), id, "Ping", nil, secondReply)
		}()
		synctest.Wait()
		if got := factoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls while activation is pending = %d, want 1", got)
		}

		close(releaseFactory)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first invocation error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second invocation error = %v", err)
		}
		if *firstReply != 1 || *secondReply != 1 {
			t.Fatalf("replies = %d, %d; want both calls to see one activation", *firstReply, *secondReply)
		}
	})
}

func TestRuntime_DifferentKeysRunConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 2})
		defer rt.Close()

		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(_ context.Context, instance any, method string, _ any, _ any) error {
				if method != "Block" {
					return errors.New("unknown method")
				}
				_ = instance.(*testEntity)
				entered <- struct{}{}
				<-release
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		go func() {
			firstDone <- rt.Invoke(context.Background(), Identity{Type: "account", Key: "alice"}, "Block", nil, nil)
		}()
		go func() {
			secondDone <- rt.Invoke(context.Background(), Identity{Type: "account", Key: "bob"}, "Block", nil, nil)
		}()

		synctest.Wait()
		select {
		case <-entered:
		default:
			t.Fatal("no call entered")
		}
		select {
		case <-entered:
		default:
			t.Fatal("different keys did not execute concurrently")
		}

		close(release)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first invocation error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second invocation error = %v", err)
		}
	})
}

func TestRuntime_EvictsIdleActivationAndReactivates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		rt := New(Config{
			Clock:            fakeClock,
			MailboxCapacity:  2,
			IdleTimeout:      10 * time.Second,
			EvictionInterval: time.Second,
		})
		defer rt.Close()

		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				if method != "Value" {
					return errors.New("unknown method")
				}
				*(reply.(*int)) = int(instance.(int))
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", nil, new(int)); err != nil {
			t.Fatal(err)
		}
		fakeClock.Advance(11 * time.Second)
		synctest.Wait()
		if got := factoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls before reactivation = %d, want 1", got)
		}

		if err := rt.Invoke(context.Background(), id, "Value", nil, new(int)); err != nil {
			t.Fatal(err)
		}
		if got := factoryCalls.Load(); got != 2 {
			t.Fatalf("factory calls after reactivation = %d, want 2", got)
		}
	})
}

func TestRuntime_DeactivateStopsActivationAndReactivates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{
			Clock:           clock.Real{},
			MailboxCapacity: 2,
		})
		defer rt.Close()

		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				if method != "Value" {
					return errors.New("unknown method")
				}
				*(reply.(*int)) = instance.(int)
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", nil, new(int)); err != nil {
			t.Fatalf("initial Invoke: %v", err)
		}
		if identities := rt.Identities(); len(identities) != 1 || identities[0] != id {
			t.Fatalf("Identities = %#v, want [%#v]", identities, id)
		}
		rt.Deactivate(id)
		synctest.Wait()
		if identities := rt.Identities(); len(identities) != 0 {
			t.Fatalf("Identities after Deactivate = %#v, want empty", identities)
		}
		if err := rt.Invoke(context.Background(), id, "Value", nil, new(int)); err != nil {
			t.Fatalf("reactivated Invoke: %v", err)
		}
		if got := factoryCalls.Load(); got != 2 {
			t.Fatalf("factory calls = %d, want 2", got)
		}
	})
}

func TestRuntime_CloseWaitsForRunningCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})

		started := make(chan struct{})
		release := make(chan struct{})
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(_ context.Context, _ any, _ string, _ any, _ any) error {
				close(started)
				<-release
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		callDone := make(chan error, 1)
		go func() {
			callDone <- rt.Invoke(context.Background(), Identity{Type: "account", Key: "alice"}, "Run", nil, nil)
		}()
		synctest.Wait()
		<-started
		closeDone := make(chan struct{})
		go func() {
			rt.Close()
			close(closeDone)
		}()
		synctest.Wait()
		select {
		case <-closeDone:
			t.Fatal("runtime close returned before running call completed")
		default:
		}
		close(release)
		synctest.Wait()
		if err := <-callDone; err != nil {
			t.Fatalf("running call error = %v", err)
		}
		<-closeDone
	})
}

func TestRuntime_KillCancelsRunningCallAndRejectsQueuedCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})

		started := make(chan struct{})
		cancelObserved := make(chan struct{})
		methodDone := make(chan struct{})
		release := make(chan struct{})
		queuedStarted := make(chan struct{})
		var calls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(ctx context.Context, _ any, method string, _ any, _ any) error {
				if method != "Block" {
					return errors.New("unknown method")
				}
				if calls.Add(1) == 1 {
					close(started)
					<-ctx.Done()
					close(cancelObserved)
					<-release
					close(methodDone)
					return nil
				}
				close(queuedStarted)
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- rt.Invoke(context.Background(), id, "Block", nil, nil)
		}()
		synctest.Wait()
		<-started

		queuedDone := make(chan error, 1)
		go func() {
			queuedDone <- rt.Invoke(context.Background(), id, "Block", nil, nil)
		}()
		synctest.Wait()

		rt.Kill()
		synctest.Wait()

		select {
		case <-methodDone:
			t.Fatal("runtime kill waited for the running method to drain")
		default:
		}
		select {
		case <-queuedStarted:
			t.Fatal("queued call ran after runtime kill")
		default:
		}
		select {
		case <-cancelObserved:
		default:
			t.Fatal("running call did not observe runtime cancellation")
		}
		if err := <-firstDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("running call error = %v, want context.Canceled", err)
		}
		if err := <-queuedDone; err == nil {
			t.Fatal("queued call returned nil after runtime kill")
		}

		close(release)
		synctest.Wait()
		select {
		case <-methodDone:
		default:
			t.Fatal("running method did not eventually exit")
		}
		rt.mu.Lock()
		_, active := rt.activations[id]
		rt.mu.Unlock()
		if active {
			t.Fatal("killed activation remained in the runtime")
		}
	})
}

func TestRuntime_KillSkipsPendingDeactivationHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		rt := New(Config{
			Clock:           fakeClock,
			MailboxCapacity: 1,
			IdleTimeout:     time.Second,
		})
		defer rt.Close()
		hookCalled := make(chan struct{}, 1)
		if err := rt.Register("account", Registration{
			Factory:  func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(context.Context, any, string, any, any) error { return nil },
			OnDeactivate: func(context.Context, Identity, any) {
				hookCalled <- struct{}{}
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", nil, nil); err != nil {
			t.Fatalf("initial Invoke: %v", err)
		}

		// This is the state after idle eviction has started its waiter. Keep the
		// mailbox open so Kill can mark the instance before the waiter wakes.
		rt.mu.Lock()
		act := rt.activations[id]
		if !beginDeactivation(act) {
			rt.mu.Unlock()
			t.Fatal("activation did not enter deactivating state")
		}
		rt.startDeactivationWaiterLocked(act)
		rt.mu.Unlock()

		rt.Kill()
		synctest.Wait()
		select {
		case <-hookCalled:
			t.Fatal("OnDeactivate ran after Kill marked the pending deactivation")
		default:
		}
	})
}

func TestRuntime_PanicStopsActivationAndQueuedCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 2})
		defer rt.Close()

		entered := make(chan struct{})
		release := make(chan struct{})
		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				switch method {
				case "Panic":
					close(entered)
					<-release
					panic("broken state")
				case "Value":
					*(reply.(*int)) = int(instance.(int))
					return nil
				default:
					return errors.New("unknown method")
				}
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		panicDone := make(chan error, 1)
		queuedDone := make(chan error, 1)
		go func() { panicDone <- rt.Invoke(context.Background(), id, "Panic", nil, nil) }()
		synctest.Wait()
		<-entered
		go func() { queuedDone <- rt.Invoke(context.Background(), id, "Value", nil, new(int)) }()
		synctest.Wait()
		close(release)
		synctest.Wait()
		if err := <-panicDone; err == nil {
			t.Fatal("panic call returned nil error")
		}
		if err := <-queuedDone; !errors.Is(err, mail.ErrClosed) {
			t.Fatalf("queued call error = %v, want ErrClosed", err)
		}

		var value int
		if err := rt.Invoke(context.Background(), id, "Value", nil, &value); err != nil {
			t.Fatalf("new activation call error = %v", err)
		}
		if value != 2 || factoryCalls.Load() != 2 {
			t.Fatalf("reactivated value/count = %d/%d, want 2/2", value, factoryCalls.Load())
		}
	})
}

func TestRuntime_FactoryPanicReleasesActivationWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				close(started)
				<-release
				panic("factory failure")
			},
			Dispatch: func(context.Context, any, string, any, any) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		creatorDone := make(chan error, 1)
		waiterDone := make(chan error, 1)
		go func() {
			creatorDone <- rt.Invoke(context.Background(), id, "Value", nil, nil)
		}()
		synctest.Wait()
		<-started
		go func() { waiterDone <- rt.Invoke(context.Background(), id, "Value", nil, nil) }()
		synctest.Wait()
		close(release)
		synctest.Wait()

		if err := <-creatorDone; err == nil {
			t.Fatal("factory panic did not become an error for creator")
		}
		if err := <-waiterDone; err == nil {
			t.Fatal("activation waiter returned nil after factory panic")
		}
	})
}

func TestRuntime_FactoryErrorIsReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		factoryErr := errors.New("factory failed")
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return nil, factoryErr
			},
			Dispatch: func(context.Context, any, string, any, any) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}

		err := rt.Invoke(context.Background(), Identity{Type: "account", Key: "alice"}, "Value", nil, nil)
		if !errors.Is(err, factoryErr) {
			t.Fatalf("factory error = %v, want %v", err, factoryErr)
		}
	})
}

func TestRuntime_DiscardStopsActivationAndReturnsCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		var factoryCalls atomic.Int32
		discardErr := errors.New("discard activation")
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				if method == "Discard" {
					return Discard{Err: discardErr}
				}
				*(reply.(*int)) = instance.(int)
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Discard", nil, nil); !errors.Is(err, discardErr) {
			t.Fatalf("discard error = %v, want %v", err, discardErr)
		}

		var value int
		if err := rt.Invoke(context.Background(), id, "Value", nil, &value); err != nil {
			t.Fatalf("reactivated invoke error = %v", err)
		}
		if value != 2 || factoryCalls.Load() != 2 {
			t.Fatalf("reactivated value/count = %d/%d, want 2/2", value, factoryCalls.Load())
		}
	})
}

func TestRuntime_DiscardWithNilErrorStillStopsActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				if method == "Discard" {
					return Discard{}
				}
				*(reply.(*int)) = instance.(int)
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Discard", nil, nil); err != nil {
			t.Fatalf("nil discard error = %v, want nil", err)
		}

		var value int
		if err := rt.Invoke(context.Background(), id, "Value", nil, &value); err != nil {
			t.Fatalf("reactivated invoke error = %v", err)
		}
		if value != 2 || factoryCalls.Load() != 2 {
			t.Fatalf("reactivated value/count = %d/%d, want 2/2", value, factoryCalls.Load())
		}
	})
}

func TestDiscard_ErrorHandlesNil(t *testing.T) {
	if got := (Discard{}).Error(); got == "" {
		t.Fatal("nil Discard error returned an empty message")
	}
}

func TestRuntime_ReactivatesCallsArrivingDuringDeactivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				return int(factoryCalls.Add(1)), nil
			},
			Dispatch: func(_ context.Context, instance any, method string, _ any, reply any) error {
				switch method {
				case "Block":
					close(started)
					<-release
					return nil
				case "Value":
					*(reply.(*int)) = int(instance.(int))
					return nil
				default:
					return errors.New("unknown method")
				}
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		oldDone := make(chan error, 1)
		go func() { oldDone <- rt.Invoke(context.Background(), id, "Block", nil, nil) }()
		synctest.Wait()
		<-started

		rt.mu.Lock()
		old := rt.activations[id]
		if !beginDeactivation(old) {
			rt.mu.Unlock()
			t.Fatal("activation did not enter deactivating state")
		}
		rt.mu.Unlock()
		old.mailbox.Close()
		go rt.waitForDeactivation(old)

		newDone := make(chan struct {
			value int
			err   error
		}, 1)
		go func() {
			var value int
			err := rt.Invoke(context.Background(), id, "Value", nil, &value)
			newDone <- struct {
				value int
				err   error
			}{value: value, err: err}
		}()
		synctest.Wait()
		select {
		case <-newDone:
			t.Fatal("call completed before old activation finished deactivating")
		default:
		}

		close(release)
		synctest.Wait()
		if err := <-oldDone; err != nil {
			t.Fatalf("old call error = %v", err)
		}
		result := <-newDone
		if result.err != nil {
			t.Fatalf("reactivated call error = %v", result.err)
		}
		if result.value != 2 || factoryCalls.Load() != 2 {
			t.Fatalf("reactivated value/count = %d/%d, want 2/2", result.value, factoryCalls.Load())
		}
	})
}

func TestRuntime_SerializesConcurrentCallsPerKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 2})
		defer rt.Close()

		firstStarted := make(chan struct{})
		secondStarted := make(chan struct{})
		release := make(chan struct{})
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(_ context.Context, _ any, method string, _ any, _ any) error {
				if method != "Block" {
					return errors.New("unknown method")
				}
				select {
				case <-firstStarted:
					close(secondStarted)
				default:
					close(firstStarted)
				}
				<-release
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		go func() { firstDone <- rt.Invoke(context.Background(), id, "Block", nil, nil) }()
		synctest.Wait()
		<-firstStarted
		go func() { secondDone <- rt.Invoke(context.Background(), id, "Block", nil, nil) }()
		synctest.Wait()
		select {
		case <-secondStarted:
			t.Fatal("second call ran before first call completed")
		default:
		}

		close(release)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first invocation error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second invocation error = %v", err)
		}
	})
}

func TestRuntime_ActivationsReportsQueuedCallsAndSorts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		var blockCalls atomic.Int32
		registration := Registration{
			Factory: func(context.Context, Identity) (any, error) { return &testEntity{}, nil },
			Dispatch: func(_ context.Context, _ any, method string, _ any, _ any) error {
				if method == "Block" && blockCalls.Add(1) == 1 {
					close(started)
					<-release
				}
				return nil
			},
		}
		for _, name := range []string{"account", "alpha", "zeta"} {
			if err := rt.Register(name, registration); err != nil {
				t.Fatal(err)
			}
		}

		firstDone := make(chan error, 1)
		queuedDone := make(chan error, 1)
		go func() {
			firstDone <- rt.Invoke(context.Background(), Identity{Type: "account", Key: "zulu"}, "Block", nil, nil)
		}()
		synctest.Wait()
		<-started
		go func() {
			queuedDone <- rt.Invoke(context.Background(), Identity{Type: "account", Key: "zulu"}, "Block", nil, nil)
		}()
		synctest.Wait()

		if got, want := rt.Activations(), []Activation{{Identity: Identity{Type: "account", Key: "zulu"}, Queued: 1}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Activations while call is blocked = %#v, want %#v", got, want)
		}

		close(release)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first call error = %v", err)
		}
		if err := <-queuedDone; err != nil {
			t.Fatalf("queued call error = %v", err)
		}

		ids := []Identity{
			{Type: "zeta", Key: "a"},
			{Type: "alpha", Key: "z"},
			{Type: "alpha", Key: "a"},
		}
		for _, id := range ids {
			if err := rt.Invoke(context.Background(), id, "Done", nil, nil); err != nil {
				t.Fatalf("Invoke(%v): %v", id, err)
			}
		}
		want := []Activation{
			{Identity: Identity{Type: "account", Key: "zulu"}},
			{Identity: Identity{Type: "alpha", Key: "a"}},
			{Identity: Identity{Type: "alpha", Key: "z"}},
			{Identity: Identity{Type: "zeta", Key: "a"}},
		}
		if got := rt.Activations(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Activations = %#v, want %#v", got, want)
		}
	})
}

func TestRuntime_ActivationsExcludesNonActiveStates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := New(Config{Clock: clock.Real{}, MailboxCapacity: 1})
		defer rt.Close()

		creating := make(chan struct{})
		releaseCreate := make(chan struct{})
		deactivating := make(chan struct{})
		releaseDeactivate := make(chan struct{})
		var factoryCalls atomic.Int32
		if err := rt.Register("account", Registration{
			Factory: func(context.Context, Identity) (any, error) {
				if factoryCalls.Add(1) == 1 {
					close(creating)
					<-releaseCreate
				}
				return &testEntity{}, nil
			},
			Dispatch: func(context.Context, any, string, any, any) error { return nil },
			OnDeactivate: func(context.Context, Identity, any) {
				close(deactivating)
				<-releaseDeactivate
			},
		}); err != nil {
			t.Fatal(err)
		}

		id := Identity{Type: "account", Key: "alice"}
		callDone := make(chan error, 1)
		go func() { callDone <- rt.Invoke(context.Background(), id, "Value", nil, nil) }()
		synctest.Wait()
		<-creating
		if got := rt.Activations(); len(got) != 0 {
			t.Fatalf("Activations while creating = %#v, want empty", got)
		}

		close(releaseCreate)
		synctest.Wait()
		if err := <-callDone; err != nil {
			t.Fatalf("initial invocation error = %v", err)
		}
		if got := rt.Activations(); len(got) != 1 {
			t.Fatalf("Activations after creation = %#v, want one active activation", got)
		}

		rt.Deactivate(id)
		synctest.Wait()
		<-deactivating
		if got := rt.Activations(); len(got) != 0 {
			t.Fatalf("Activations while deactivating = %#v, want empty", got)
		}

		close(releaseDeactivate)
		synctest.Wait()
		if got := rt.Activations(); len(got) != 0 {
			t.Fatalf("Activations after stopping = %#v, want empty", got)
		}
	})
}
