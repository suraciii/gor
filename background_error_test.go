package gor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

// failingScheduleStore fails schedule listing or claiming on demand. It exists
// to pin the boundary of the background error exit: list and claim failures
// are scheduler state, not application callback failures.
type failingScheduleStore struct {
	*store.Memory
	failList  atomic.Bool
	failClaim atomic.Bool
}

func (s *failingScheduleStore) ListDue(ctx context.Context, now time.Time) ([]store.Schedule, error) {
	if s.failList.Load() {
		return nil, errors.New("simulated list failure")
	}
	return s.Memory.ListDue(ctx, now)
}

func (s *failingScheduleStore) Claim(ctx context.Context, schedule store.Schedule, nextDueAt time.Time) (bool, error) {
	if s.failClaim.Load() {
		return false, errors.New("simulated claim failure")
	}
	return s.Memory.Claim(ctx, schedule, nextDueAt)
}

// misnamedSchedule is an entity whose interface contains a method literally
// named OnDeactivate. Its signature differs from the Deactivatable hook, so it
// does not implement Deactivatable; a scheduled failure of this method must
// still be reported as a ScheduledInvocation, not as a Deactivation.
type misnamedSchedule interface {
	Arm(context.Context) error
	OnDeactivate(context.Context) error
}

type misnamedScheduleArmRequest struct{}
type misnamedScheduleArmReply struct{}
type misnamedScheduleDeactivateRequest struct{}
type misnamedScheduleDeactivateReply struct{}

type misnamedScheduleEntity struct {
	schedule Schedule
	wakeErr  error
}

type misnamedScheduleProxy struct {
	invoker Invoker
	id      Identity
}

func (e *misnamedScheduleEntity) Arm(ctx context.Context) error {
	return e.schedule.Set(ctx, "wake", After(12*time.Second), "OnDeactivate")
}

func (e *misnamedScheduleEntity) OnDeactivate(context.Context) error {
	return e.wakeErr
}

func (p *misnamedScheduleProxy) Arm(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", &misnamedScheduleArmRequest{}, &misnamedScheduleArmReply{})
}

func (p *misnamedScheduleProxy) OnDeactivate(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "OnDeactivate", &misnamedScheduleDeactivateRequest{}, &misnamedScheduleDeactivateReply{})
}

func dispatchMisnamedSchedule(ctx context.Context, instance misnamedSchedule, method string, _ any, _ any) error {
	switch method {
	case "Arm":
		return instance.Arm(ctx)
	case "OnDeactivate":
		return instance.OnDeactivate(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newMisnamedScheduleCall(method string) (any, any) {
	switch method {
	case "Arm":
		return &misnamedScheduleArmRequest{}, &misnamedScheduleArmReply{}
	case "OnDeactivate":
		return &misnamedScheduleDeactivateRequest{}, &misnamedScheduleDeactivateReply{}
	default:
		return nil, nil
	}
}

func installMisnamedSchedule(t *testing.T, rt *Runtime, wakeErr error) {
	t.Helper()
	if err := InstallType[misnamedSchedule](rt, dispatchMisnamedSchedule, func(invoker Invoker, id Identity) misnamedSchedule {
		return &misnamedScheduleProxy{invoker: invoker, id: id}
	}, newMisnamedScheduleCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[misnamedSchedule](rt, func(b *Binder) misnamedSchedule {
		return &misnamedScheduleEntity{schedule: NewSchedule(b), wakeErr: wakeErr}
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBackgroundError_ScheduledMethodNamedOnDeactivate pins the sealed-source
// contract against the exact trap the old method-string API had: a scheduled
// method deliberately named "OnDeactivate" must still be reported as a
// ScheduledInvocation.
func TestBackgroundError_ScheduledMethodNamedOnDeactivate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		wakeErr := errors.New("scheduled OnDeactivate failed")
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		defer rt.Close()
		installMisnamedSchedule(t, rt, wakeErr)

		if err := Ref[misnamedSchedule](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		select {
		case got := <-errorsSeen:
			wantID := Identity{Type: TypeName[misnamedSchedule](), Key: "alice"}
			source, ok := got.Source.(ScheduledInvocation)
			if !ok {
				t.Fatalf("source = %#v, want ScheduledInvocation", got.Source)
			}
			if got.Identity != wantID || source.Method != "OnDeactivate" || !errors.Is(got.Err, wakeErr) {
				t.Fatalf("event = %#v, want identity %v, ScheduledInvocation{Method: OnDeactivate}, error %v", got, wantID, wakeErr)
			}
			if _, isDeactivation := got.Source.(Deactivation); isDeactivation {
				t.Fatalf("source = %#v, must not be Deactivation", got.Source)
			}
		default:
			t.Fatal("scheduled failure did not reach OnError")
		}
	})
}

// TestBackgroundError_CancelShapedErrorFromLivePoller pins the exclusion's
// exact boundary: the shutdown rule excludes a canceled poller context, not
// errors that merely look like cancellations. A method that fabricates a
// cancel-shaped error while the poller context is alive must still be reported.
func TestBackgroundError_CancelShapedErrorFromLivePoller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		defer rt.Close()
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{cancelShapedErr: true})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		select {
		case got := <-errorsSeen:
			source, ok := got.Source.(ScheduledInvocation)
			if !ok {
				t.Fatalf("source = %#v, want ScheduledInvocation", got.Source)
			}
			if source.Method != "Wake" || !errors.Is(got.Err, context.Canceled) {
				t.Fatalf("event = %#v, want ScheduledInvocation{Method: Wake} with a cancel-shaped error", got)
			}
		default:
			t.Fatal("cancel-shaped error from a live poller did not reach OnError")
		}
	})
}

// TestBackgroundError_DeactivationCarriesStopReason covers a hook failure
// during a graceful close: the event carries Deactivation with the reason of
// that deactivation (RuntimeClosed), not a fabricated method name.
func TestBackgroundError_DeactivationCarriesStopReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deactivateErr := errors.New("deactivate failed")
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateErr = deactivateErr
		})

		id := Identity{Type: TypeName[lifecycleAccount](), Key: "alice"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		rt.Close()

		select {
		case got := <-errorsSeen:
			source, ok := got.Source.(Deactivation)
			if !ok {
				t.Fatalf("source = %#v, want Deactivation", got.Source)
			}
			if got.Identity != id || source.Reason != RuntimeClosed || !errors.Is(got.Err, deactivateErr) {
				t.Fatalf("event = %#v, want identity %v, Deactivation{Reason: RuntimeClosed}, error %v", got, id, deactivateErr)
			}
		default:
			t.Fatal("deactivation hook failure did not reach OnError")
		}
	})
}

// TestBackgroundError_ScheduleFaultsAreSilent pins the boundary that list and
// claim failures are scheduler and store state, not application callback
// failures: neither produces an event.
func TestBackgroundError_ScheduleFaultsAreSilent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := &failingScheduleStore{Memory: store.NewMemory()}
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithStore(backend),
			WithClock(fakeClock),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		defer rt.Close()
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}

		backend.failList.Store(true)
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()
		select {
		case got := <-errorsSeen:
			t.Fatalf("OnError received an event for a list failure: %#v", got)
		default:
		}

		backend.failList.Store(false)
		backend.failClaim.Store(true)
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()
		select {
		case got := <-errorsSeen:
			t.Fatalf("OnError received an event for a claim failure: %#v", got)
		default:
		}

		backend.failList.Store(false)
		backend.failClaim.Store(false)
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()
		// Positive control: with the faults cleared, the same poller must
		// deliver the row that stayed due through both silent phases. The
		// no-event assertions above are observations of a running poller, not
		// of a stopped one.
		if got, err := Ref[scheduledAccount](rt, "alice").Value(context.Background()); err != nil {
			t.Fatalf("Value: %v", err)
		} else if got != 1 {
			t.Fatalf("value after delivery = %d, want 1", got)
		}
	})
}

// TestBackgroundError_CanceledDeliveryNotReported pins the shutdown rule: a
// scheduled delivery canceled because the poller's context was canceled is not
// a callback failure and must not reach OnError, while a hook failure during
// the same close still does.
func TestBackgroundError_CanceledDeliveryNotReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		wakeStarted := make(chan struct{})
		deactivateErr := errors.New("deactivate failed")
		errorsSeen := make(chan BackgroundError, 2)
		rt := mustNew(t,
			WithClock(fakeClock),
			WithIdleTimeout(0),
			WithEvictionInterval(0),
			WithScheduleInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{wakeStarted: wakeStarted})
		installLifecycleAccount(t, rt, new(atomic.Int32), func(entity *lifecycleAccountEntity) {
			entity.deactivateErr = deactivateErr
		})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		id := Identity{Type: TypeName[lifecycleAccount](), Key: "bob"}
		if err := rt.Invoke(context.Background(), id, "Value", &lifecycleAccountValueRequest{}, &lifecycleAccountValueReply{}); err != nil {
			t.Fatalf("initial Value: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()
		select {
		case <-wakeStarted:
		default:
			t.Fatal("scheduled Wake did not start")
		}

		rt.Close()
		synctest.Wait()

		// The only event is the hook failure of the graceful close. The
		// canceled delivery must not have produced one.
		select {
		case got := <-errorsSeen:
			source, ok := got.Source.(Deactivation)
			if !ok || got.Identity != id || source.Reason != RuntimeClosed || !errors.Is(got.Err, deactivateErr) {
				t.Fatalf("event = %#v, want identity %v, Deactivation{Reason: RuntimeClosed}, error %v", got, id, deactivateErr)
			}
		default:
			t.Fatal("deactivation hook failure did not reach OnError")
		}
		select {
		case got := <-errorsSeen:
			t.Fatalf("canceled scheduled delivery was reported: %#v", got)
		default:
		}
	})
}
