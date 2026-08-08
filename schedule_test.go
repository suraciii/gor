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
	Wake(context.Context, TickStatus) error
	Value(context.Context) (int64, error)
}

type scheduledAccountArmRequest struct{}

type scheduledAccountArmReply struct{}

type scheduledAccountWakeRequest struct {
	A0 TickStatus
}

type scheduledAccountWakeReply struct{}

type scheduledAccountValueRequest struct{}

type scheduledAccountValueReply struct {
	R0 int64
}

type scheduledAccountEntity struct {
	value           State[int64]
	schedule        Reminder[scheduledAccount]
	wakeErr         error
	wakeStarted     chan struct{}
	wakeCalls       *atomic.Int32
	cancelShapedErr bool
}

type scheduledAccountProxy struct {
	invoker Invoker
	id      GrainId
}

func (a *scheduledAccountEntity) Arm(ctx context.Context) error {
	return a.schedule.Set(ctx, "wake", After(12*time.Second), Handle(scheduledAccount.Wake))
}

func (a *scheduledAccountEntity) Wake(ctx context.Context, _ TickStatus) error {
	if a.wakeCalls != nil {
		a.wakeCalls.Add(1)
	}
	if a.wakeStarted != nil {
		close(a.wakeStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	if a.cancelShapedErr {
		child, cancel := context.WithCancel(ctx)
		cancel()
		return child.Err()
	}
	if a.wakeErr != nil {
		return a.wakeErr
	}
	return a.value.Set(ctx, a.value.Get()+1)
}

func (a *scheduledAccountEntity) Value(context.Context) (int64, error) {
	return a.value.Get(), nil
}

func (p *scheduledAccountProxy) Arm(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", &scheduledAccountArmRequest{}, &scheduledAccountArmReply{})
}

func (p *scheduledAccountProxy) Wake(ctx context.Context, tick TickStatus) error {
	return p.invoker.Invoke(ctx, p.id, "Wake", &scheduledAccountWakeRequest{A0: tick}, &scheduledAccountWakeReply{})
}

func (p *scheduledAccountProxy) Value(ctx context.Context) (int64, error) {
	var reply scheduledAccountValueReply
	err := p.invoker.Invoke(ctx, p.id, "Value", &scheduledAccountValueRequest{}, &reply)
	return reply.R0, err
}

func dispatchScheduledAccount(ctx context.Context, instance scheduledAccount, method string, args any, reply any) error {
	switch method {
	case "Arm":
		return instance.Arm(ctx)
	case "Wake":
		return instance.Wake(ctx, args.(*scheduledAccountWakeRequest).A0)
	case "Value":
		typedReply := reply.(*scheduledAccountValueReply)
		value, err := instance.Value(ctx)
		if err == nil {
			typedReply.R0 = value
		}
		return err
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newScheduledAccountCall(method string) (args any, reply any) {
	switch method {
	case "Arm":
		return &scheduledAccountArmRequest{}, &scheduledAccountArmReply{}
	case "Wake":
		return &scheduledAccountWakeRequest{}, &scheduledAccountWakeReply{}
	case "Value":
		return &scheduledAccountValueRequest{}, &scheduledAccountValueReply{}
	default:
		return nil, nil
	}
}

func newScheduledAccountReminderCall(method string, status TickStatus) (args any, reply any) {
	if method == "Wake" {
		return &scheduledAccountWakeRequest{A0: status}, &scheduledAccountWakeReply{}
	}
	return nil, nil
}

type scheduledAccountConfig struct {
	wakeErr         error
	wakeStarted     chan struct{}
	wakeCalls       *atomic.Int32
	cancelShapedErr bool
}

func installScheduledAccount(t *testing.T, rt *Runtime, factoryCalls *atomic.Int32, configs ...scheduledAccountConfig) {
	t.Helper()
	var config scheduledAccountConfig
	if len(configs) > 0 {
		config = configs[0]
	}
	if err := InstallType[scheduledAccount](rt, dispatchScheduledAccount, func(invoker Invoker, id GrainId) scheduledAccount {
		return &scheduledAccountProxy{invoker: invoker, id: id}
	}, newScheduledAccountCall, newScheduledAccountReminderCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[scheduledAccount](rt, func(b *Binder) scheduledAccount {
		factoryCalls.Add(1)
		return &scheduledAccountEntity{
			value:           NewState[int64](b, "value"),
			schedule:        NewReminder[scheduledAccount](b),
			wakeErr:         config.wakeErr,
			wakeStarted:     config.wakeStarted,
			wakeCalls:       config.wakeCalls,
			cancelShapedErr: config.cancelShapedErr,
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSchedule_OnErrorReceivesInvocationFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		wakeErr := errors.New("scheduled wake failed")
		wakeCalls := new(atomic.Int32)
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithStore(backend),
			WithClock(fakeClock),
			WithIdleTimeout(5*time.Second),
			WithEvictionInterval(time.Second),
			WithReminderInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		defer rt.Close()
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{wakeErr: wakeErr, wakeCalls: wakeCalls})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		select {
		case got := <-errorsSeen:
			source, ok := got.Source.(ReminderInvocation)
			wantID := GrainId{GrainType: TypeName[scheduledAccount](), GrainKey: "alice"}
			if !ok || got.GrainId != wantID || source.Method != "Wake" || !errors.Is(got.Err, wakeErr) {
				t.Fatalf("OnError event = %#v, want identity %v, ReminderInvocation{Method: Wake}, error %v", got, wantID, wakeErr)
			}
		default:
			t.Fatal("OnError did not receive Reminder invocation failure")
		}
		if got := wakeCalls.Load(); got != 1 {
			t.Fatalf("Wake calls = %d, want 1 without automatic retry", got)
		}
	})
}

func TestSchedule_DropsInvocationFailureWithoutOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		rt := newScheduledRuntime(t, backend, fakeClock)
		defer rt.Close()
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{wakeErr: errors.New("scheduled wake failed")})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()

		rows, err := backend.ListDue(context.Background(), start.Add(12*time.Second))
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("reminders after failed one-shot invocation = %#v, want none", rows)
		}
	})
}

func TestSchedule_DropsCancellationOnRuntimeClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		wakeStarted := make(chan struct{})
		errorsSeen := make(chan BackgroundError, 1)
		rt := mustNew(t,
			WithStore(backend),
			WithClock(fakeClock),
			WithIdleTimeout(5*time.Second),
			WithEvictionInterval(time.Second),
			WithReminderInterval(time.Second),
			OnError(func(event BackgroundError) {
				errorsSeen <- event
			}),
		)
		installScheduledAccount(t, rt, new(atomic.Int32), scheduledAccountConfig{wakeStarted: wakeStarted})

		if err := Ref[scheduledAccount](rt, "alice").Arm(context.Background()); err != nil {
			t.Fatalf("Arm: %v", err)
		}
		fakeClock.Advance(12 * time.Second)
		synctest.Wait()
		select {
		case <-wakeStarted:
		default:
			t.Fatal("scheduled Wake did not start")
		}

		rt.Close()
		select {
		case got := <-errorsSeen:
			t.Fatalf("OnError received shutdown cancellation: %#v", got)
		default:
		}
	})
}

func newScheduledRuntime(t *testing.T, backend store.Store, sourceClock clock.Clock) *Runtime {
	t.Helper()
	rt := mustNew(t,
		WithStore(backend),
		WithClock(sourceClock),
		WithIdleTimeout(5*time.Second),
		WithEvictionInterval(time.Second),
		WithReminderInterval(time.Second),
	)
	return rt
}

func TestSchedule_SetOverwritesAndCancelDeletes(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	fakeClock := clock.NewFake(start)
	backend := store.NewMemory()
	schedule := NewReminder[scheduledAccount](newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, backend, backend, fakeClock))

	if err := schedule.Set(context.Background(), "wake", After(time.Second), Handle(scheduledAccount.Wake)); err != nil {
		t.Fatal(err)
	}
	if err := schedule.Set(context.Background(), "wake", Every(2*time.Second), Handle(scheduledAccount.Wake)); err != nil {
		t.Fatal(err)
	}
	rows, err := backend.ListDue(context.Background(), start.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Method != "Wake" || rows[0].Interval != 2*time.Second || !rows[0].FirstTickTime.Equal(start.Add(2*time.Second)) || !rows[0].DueAt.Equal(start.Add(2*time.Second)) {
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
		t.Fatalf("reminders after cancel = %#v, want none", rows)
	}
}

func TestSchedule_ReturnsUnavailableWithoutReminderStore(t *testing.T) {
	schedule := NewReminder[scheduledAccount](newTestBinder(GrainId{GrainType: "account", GrainKey: "alice"}, failingWriteStore{}, nil, clock.Real{}))
	if err := schedule.Set(context.Background(), "wake", After(time.Second), Handle(scheduledAccount.Wake)); !errors.Is(err, ErrReminderStoreUnavailable) {
		t.Fatalf("Set error = %v, want ErrReminderStoreUnavailable", err)
	}
	if err := schedule.Cancel(context.Background(), "wake"); !errors.Is(err, ErrReminderStoreUnavailable) {
		t.Fatalf("Cancel error = %v, want ErrReminderStoreUnavailable", err)
	}
}

func TestNew_ReminderStoreOptionIsOrderIndependent(t *testing.T) {
	explicit := store.NewMemory()
	first := mustNew(t, WithReminderStore(explicit), WithStore(failingWriteStore{}), WithReminderInterval(0), WithEvictionInterval(0))
	if first.reminderStore != explicit {
		t.Fatal("WithStore replaced an earlier explicit ReminderStore")
	}
	first.Close()

	second := mustNew(t, WithStore(failingWriteStore{}), WithReminderStore(explicit), WithReminderInterval(0), WithEvictionInterval(0))
	if second.reminderStore != explicit {
		t.Fatal("WithReminderStore did not replace the default ReminderStore")
	}
	second.Close()

	backend := store.NewMemory()
	third := mustNew(t, WithStore(backend), WithReminderInterval(0), WithEvictionInterval(0))
	if third.reminderStore != backend {
		t.Fatal("New did not derive ReminderStore from Store")
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
