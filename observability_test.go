package gor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type observedAccount interface {
	Success(context.Context) error
	Failure(context.Context) error
	Block(context.Context) error
}

type observedAccountEntity struct {
	clock         *clock.Fake
	activationErr error
	methodErr     error
	started       chan<- struct{}
	release       <-chan struct{}
	finished      chan<- struct{}
	deactivated   chan<- struct{}
}

func (e *observedAccountEntity) OnActivate(context.Context) error {
	return e.activationErr
}

func (e *observedAccountEntity) OnDeactivate(_ context.Context, _ DeactivationReason) error {
	if e.deactivated != nil {
		e.deactivated <- struct{}{}
	}
	return nil
}

func (e *observedAccountEntity) Success(context.Context) error {
	if e.clock != nil {
		e.clock.Advance(3 * time.Second)
	}
	return nil
}

func (e *observedAccountEntity) Failure(context.Context) error {
	return e.methodErr
}

func (e *observedAccountEntity) Block(context.Context) error {
	if e.started != nil {
		e.started <- struct{}{}
	}
	<-e.release
	if e.finished != nil {
		e.finished <- struct{}{}
	}
	return nil
}

func dispatchObservedAccount(ctx context.Context, instance observedAccount, method string, _ any, _ any) error {
	switch method {
	case "Success":
		return instance.Success(ctx)
	case "Failure":
		return instance.Failure(ctx)
	case "Block":
		return instance.Block(ctx)
	default:
		return errors.New("unknown method")
	}
}

func newObservedRuntime(t *testing.T, sourceClock clock.Clock, events chan<- CallObservation, factory func(*Binder) observedAccount, options ...Option) *Runtime {
	t.Helper()
	base := []Option{
		WithClock(sourceClock),
		WithIdleTimeout(0),
		WithEvictionInterval(0),
		WithScheduleInterval(0),
	}
	if events != nil {
		base = append(base, OnCall(func(observation CallObservation) {
			events <- observation
		}))
	}
	rt := mustNew(t, append(base, options...)...)
	if err := InstallType[observedAccount](rt, dispatchObservedAccount, func(Invoker, GrainId) observedAccount {
		return nil
	}, func(string) (any, any) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := Register[observedAccount](rt, factory); err != nil {
		t.Fatal(err)
	}
	return rt
}

func assertObservation(t *testing.T, got CallObservation, method string, wantErr error) {
	t.Helper()
	if got.GrainType != TypeName[observedAccount]() {
		t.Fatalf("GrainType = %q, want %q", got.GrainType, TypeName[observedAccount]())
	}
	if got.Method != method {
		t.Fatalf("Method = %q, want %q", got.Method, method)
	}
	if !errors.Is(got.Err, wantErr) {
		t.Fatalf("Err = %v, want %v", got.Err, wantErr)
	}
}

func TestOnCallReportsSuccessfulInvocationWithInjectedDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		events := make(chan CallObservation, 1)
		rt := newObservedRuntime(t, fakeClock, events, func(*Binder) observedAccount {
			return &observedAccountEntity{clock: fakeClock}
		})
		defer rt.Close()

		if err := rt.Invoke(context.Background(), GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}, "Success", nil, nil); err != nil {
			t.Fatalf("Invoke error = %v", err)
		}
		got := <-events
		assertObservation(t, got, "Success", nil)
		if got.Duration != 3*time.Second {
			t.Fatalf("Duration = %s, want 3s", got.Duration)
		}
	})
}

func TestOnCallDoesNotReportDeactivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan CallObservation, 1)
		deactivated := make(chan struct{}, 1)
		fakeClock := clock.NewFake(time.Unix(0, 0).UTC())
		rt := newObservedRuntime(t, fakeClock, events, func(*Binder) observedAccount {
			return &observedAccountEntity{deactivated: deactivated}
		}, WithIdleTimeout(time.Second), WithEvictionInterval(time.Second))
		defer rt.Close()
		id := GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}

		if err := rt.Invoke(context.Background(), id, "Success", nil, nil); err != nil {
			t.Fatalf("Invoke error = %v", err)
		}
		assertObservation(t, <-events, "Success", nil)
		fakeClock.Advance(time.Second)
		synctest.Wait()
		select {
		case <-deactivated:
		default:
			t.Fatal("idle eviction did not call OnDeactivate")
		}
		select {
		case got := <-events:
			t.Fatalf("OnDeactivate produced observation: %#v", got)
		default:
		}
	})
}

func TestOnCallReportsScheduledInvocation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		events := make(chan CallObservation, 2)
		rt := mustNew(t,
			WithStore(store.NewMemory()),
			WithClock(fakeClock),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(time.Second),
			OnCall(func(observation CallObservation) {
				events <- observation
			}),
		)
		defer rt.Close()
		installScheduledAccount(t, rt, new(atomic.Int32))
		id := GrainId{GrainType: TypeName[scheduledAccount](), GrainKey: "alice"}

		if err := Ref[scheduledAccount](rt, id.GrainKey).Arm(context.Background()); err != nil {
			t.Fatalf("Arm error = %v", err)
		}
		arm := <-events
		if arm.GrainType != id.GrainType || arm.Method != "Arm" || arm.Err != nil {
			t.Fatalf("arm observation = %#v, want %s/Arm with nil error", arm, id.GrainType)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		got := <-events
		if got.GrainType != id.GrainType || got.Method != "Wake" || got.Err != nil {
			t.Fatalf("scheduled observation = %#v, want %s/Wake with nil error", got, id.GrainType)
		}
		select {
		case duplicate := <-events:
			t.Fatalf("scheduled invocation produced an extra observation: %#v", duplicate)
		default:
		}
	})
}

func TestOnCallReportsMethodError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		methodErr := errors.New("method failed")
		events := make(chan CallObservation, 1)
		rt := newObservedRuntime(t, clock.NewFake(time.Unix(0, 0).UTC()), events, func(*Binder) observedAccount {
			return &observedAccountEntity{methodErr: methodErr}
		})
		defer rt.Close()

		err := rt.Invoke(context.Background(), GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}, "Failure", nil, nil)
		if !errors.Is(err, methodErr) {
			t.Fatalf("Invoke error = %v, want %v", err, methodErr)
		}
		got := <-events
		assertObservation(t, got, "Failure", methodErr)
	})
}

func TestOnCallReportsActivationFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		activationErr := errors.New("activation failed")
		events := make(chan CallObservation, 1)
		rt := newObservedRuntime(t, clock.NewFake(time.Unix(0, 0).UTC()), events, func(*Binder) observedAccount {
			return &observedAccountEntity{activationErr: activationErr}
		})
		defer rt.Close()

		err := rt.Invoke(context.Background(), GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}, "Success", nil, nil)
		if !errors.Is(err, activationErr) {
			t.Fatalf("Invoke error = %v, want %v", err, activationErr)
		}
		got := <-events
		assertObservation(t, got, "Success", activationErr)
	})
}

func TestOnCallReportsOverload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		events := make(chan CallObservation, 3)
		rt := newObservedRuntime(t, clock.NewFake(time.Unix(0, 0).UTC()), events, func(*Binder) observedAccount {
			return &observedAccountEntity{started: started, release: release}
		}, WithMailboxCapacity(1))
		defer rt.Close()
		id := GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}

		firstDone := make(chan error, 1)
		go func() { firstDone <- rt.Invoke(context.Background(), id, "Block", nil, nil) }()
		synctest.Wait()
		<-started
		secondDone := make(chan error, 1)
		go func() { secondDone <- rt.Invoke(context.Background(), id, "Block", nil, nil) }()
		synctest.Wait()

		err := rt.Invoke(context.Background(), id, "Block", nil, nil)
		if !errors.Is(err, ErrOverloaded) {
			t.Fatalf("overloaded Invoke error = %v, want %v", err, ErrOverloaded)
		}
		if got, ok := CodeOf(err); !ok || got != ErrOverloaded {
			t.Fatalf("CodeOf(overloaded error) = (%q, %v), want (%q, true)", got, ok, ErrOverloaded)
		}
		got := <-events
		assertObservation(t, got, "Block", ErrOverloaded)

		close(release)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first Invoke error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second Invoke error = %v", err)
		}
		for range 2 {
			assertObservation(t, <-events, "Block", nil)
		}
	})
}

func TestOnCallReportsCancellationOnceAfterMethodFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		finished := make(chan struct{}, 1)
		events := make(chan CallObservation, 2)
		rt := newObservedRuntime(t, clock.NewFake(time.Unix(0, 0).UTC()), events, func(*Binder) observedAccount {
			return &observedAccountEntity{started: started, release: release, finished: finished}
		})
		defer rt.Close()
		id := GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}
		ctx, cancel := context.WithCancel(context.Background())
		callDone := make(chan error, 1)
		go func() { callDone <- rt.Invoke(ctx, id, "Block", nil, nil) }()
		synctest.Wait()
		<-started

		cancel()
		synctest.Wait()
		if err := <-callDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Invoke error = %v, want context.Canceled", err)
		}
		got := <-events
		assertObservation(t, got, "Block", context.Canceled)
		select {
		case <-events:
			t.Fatal("cancellation produced a second observation before method finished")
		default:
		}

		close(release)
		synctest.Wait()
		<-finished
		select {
		case got := <-events:
			t.Fatalf("method completion produced a second observation: %#v", got)
		default:
		}
	})
}

type countingClock struct {
	clock.Clock
	nowCalls atomic.Int32
}

func (c *countingClock) Now() time.Time {
	c.nowCalls.Add(1)
	return c.Clock.Now()
}

func TestOnCallDisabledDoesNotReadAnExtraClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		counting := &countingClock{Clock: clock.NewFake(time.Unix(0, 0).UTC())}
		rt := newObservedRuntime(t, counting, nil, func(*Binder) observedAccount {
			return &observedAccountEntity{}
		})
		defer rt.Close()

		if err := rt.Invoke(context.Background(), GrainId{GrainType: TypeName[observedAccount](), GrainKey: "alice"}, "Success", nil, nil); err != nil {
			t.Fatalf("Invoke error = %v", err)
		}
		if got := counting.nowCalls.Load(); got != 2 {
			t.Fatalf("Clock.Now calls with OnCall disabled = %d, want 2", got)
		}
	})
}
