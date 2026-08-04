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

type scheduledAccount interface {
	Arm(context.Context) error
	Wake(context.Context) error
	Value(context.Context) (int64, error)
}

type scheduledAccountEntity struct {
	value    State[int64]
	schedule Schedule
}

type scheduledAccountProxy struct {
	invoker Invoker
	id      Identity
}

func (a *scheduledAccountEntity) Arm(ctx context.Context) error {
	return a.schedule.Set(ctx, "wake", After(12*time.Second), "Wake")
}

func (a *scheduledAccountEntity) Wake(ctx context.Context) error {
	return a.value.Set(ctx, a.value.Get()+1)
}

func (a *scheduledAccountEntity) Value(context.Context) (int64, error) {
	return a.value.Get(), nil
}

func (p *scheduledAccountProxy) Arm(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", nil, nil)
}

func (p *scheduledAccountProxy) Wake(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Wake", nil, nil)
}

func (p *scheduledAccountProxy) Value(ctx context.Context) (int64, error) {
	var value int64
	err := p.invoker.Invoke(ctx, p.id, "Value", nil, &value)
	return value, err
}

func dispatchScheduledAccount(ctx context.Context, instance scheduledAccount, method string, _ []any, reply any) error {
	switch method {
	case "Arm":
		return instance.Arm(ctx)
	case "Wake":
		return instance.Wake(ctx)
	case "Value":
		value, err := instance.Value(ctx)
		if err == nil {
			*(reply.(*int64)) = value
		}
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func installScheduledAccount(t *testing.T, rt *Runtime, factoryCalls *atomic.Int32) {
	t.Helper()
	if err := InstallType[scheduledAccount](rt, dispatchScheduledAccount, func(invoker Invoker, id Identity) scheduledAccount {
		return &scheduledAccountProxy{invoker: invoker, id: id}
	}); err != nil {
		t.Fatal(err)
	}
	if err := Register[scheduledAccount](rt, func(b *Binder) scheduledAccount {
		factoryCalls.Add(1)
		return &scheduledAccountEntity{
			value:    NewState[int64](b, "value"),
			schedule: NewSchedule(b),
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func newScheduledRuntime(t *testing.T, backend store.Store, sourceClock clock.Clock) *Runtime {
	t.Helper()
	rt := mustNew(t,
		WithStore(backend),
		WithClock(sourceClock),
		WithIdleTimeout(5*time.Second),
		WithEvictionInterval(time.Second),
		WithScheduleInterval(time.Second),
	)
	return rt
}

func TestSchedule_SetOverwritesAndCancelDeletes(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	fakeClock := clock.NewFake(start)
	backend := store.NewMemory()
	schedule := NewSchedule(newBinder(Identity{Type: "account", Key: "alice"}, backend, backend, fakeClock))

	if err := schedule.Set(context.Background(), "wake", After(time.Second), "Wake"); err != nil {
		t.Fatal(err)
	}
	if err := schedule.Set(context.Background(), "wake", Every(2*time.Second), "WakeAgain"); err != nil {
		t.Fatal(err)
	}
	rows, err := backend.ListDue(context.Background(), start.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Method != "WakeAgain" || rows[0].Interval != 2*time.Second || !rows[0].DueAt.Equal(start.Add(2*time.Second)) {
		t.Fatalf("overwritten schedule = %#v", rows)
	}

	if err := schedule.Cancel(context.Background(), "wake"); err != nil {
		t.Fatal(err)
	}
	rows, err = backend.ListDue(context.Background(), start.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("schedules after cancel = %#v, want none", rows)
	}
}

func TestSchedule_ReturnsUnavailableWithoutScheduleStore(t *testing.T) {
	schedule := NewSchedule(newBinder(Identity{Type: "account", Key: "alice"}, failingWriteStore{}, nil, clock.Real{}))
	if err := schedule.Set(context.Background(), "wake", After(time.Second), "Wake"); !errors.Is(err, ErrScheduleStoreUnavailable) {
		t.Fatalf("Set error = %v, want ErrScheduleStoreUnavailable", err)
	}
	if err := schedule.Cancel(context.Background(), "wake"); !errors.Is(err, ErrScheduleStoreUnavailable) {
		t.Fatalf("Cancel error = %v, want ErrScheduleStoreUnavailable", err)
	}
}

func TestNew_ScheduleStoreOptionIsOrderIndependent(t *testing.T) {
	explicit := store.NewMemory()
	first := mustNew(t, WithScheduleStore(explicit), WithStore(failingWriteStore{}), WithScheduleInterval(0), WithEvictionInterval(0))
	if first.scheduleStore != explicit {
		t.Fatal("WithStore replaced an earlier explicit ScheduleStore")
	}
	first.Close()

	second := mustNew(t, WithStore(failingWriteStore{}), WithScheduleStore(explicit), WithScheduleInterval(0), WithEvictionInterval(0))
	if second.scheduleStore != explicit {
		t.Fatal("WithScheduleStore did not replace the default ScheduleStore")
	}
	second.Close()

	backend := store.NewMemory()
	third := mustNew(t, WithStore(backend), WithScheduleInterval(0), WithEvictionInterval(0))
	if third.scheduleStore != backend {
		t.Fatal("New did not derive ScheduleStore from Store")
	}
	third.Close()
}

func TestSchedule_ReactivatesEvictedEntity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		factoryCalls := new(atomic.Int32)
		rt := newScheduledRuntime(t, backend, fakeClock)
		defer rt.Close()
		installScheduledAccount(t, rt, factoryCalls)

		account := Ref[scheduledAccount](rt, "alice")
		if err := account.Arm(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := factoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls after arm = %d, want 1", got)
		}

		fakeClock.Advance(6 * time.Second)
		synctest.Wait()
		if got := factoryCalls.Load(); got != 1 {
			t.Fatalf("factory calls after eviction = %d, want 1", got)
		}

		fakeClock.Advance(6 * time.Second)
		synctest.Wait()
		if got := factoryCalls.Load(); got != 2 {
			t.Fatalf("factory calls after scheduled wake = %d, want 2", got)
		}
		if got, err := account.Value(context.Background()); err != nil || got != 1 {
			t.Fatalf("value after scheduled wake = (%d, %v), want (1, nil)", got, err)
		}
	})
}

func TestSchedule_SurvivesRuntimeRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		factoryCalls := new(atomic.Int32)

		first := newScheduledRuntime(t, backend, fakeClock)
		installScheduledAccount(t, first, factoryCalls)
		if err := Ref[scheduledAccount](first, "alice").Arm(context.Background()); err != nil {
			t.Fatal(err)
		}
		first.Kill()

		second := newScheduledRuntime(t, backend, fakeClock)
		defer second.Close()
		installScheduledAccount(t, second, factoryCalls)
		synctest.Wait()
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		account := Ref[scheduledAccount](second, "alice")
		if got, err := account.Value(context.Background()); err != nil || got != 1 {
			t.Fatalf("value after runtime restart = (%d, %v), want (1, nil)", got, err)
		}
	})
}
