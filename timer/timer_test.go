package timer

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type fakeTable struct {
	rows     []store.Reminder
	claimWon bool

	recorder  *stepRecorder
	nextDueAt []time.Time
}

func (t *fakeTable) ListDue(context.Context, time.Time) ([]store.Reminder, error) {
	t.recorder.record("list")
	return append([]store.Reminder(nil), t.rows...), nil
}

func (t *fakeTable) Claim(_ context.Context, schedule store.Reminder, nextDueAt time.Time) (bool, error) {
	t.recorder.mu.Lock()
	t.recorder.steps = append(t.recorder.steps, "claim")
	t.nextDueAt = append(t.nextDueAt, nextDueAt)
	t.recorder.mu.Unlock()
	return t.claimWon, nil
}

type recordingInvoker struct {
	recorder *stepRecorder
	calls    []store.GrainId
}

func testReminderCall(store.GrainId, string, time.Time, time.Duration, time.Time) (any, any) {
	return &struct{}{}, &struct{}{}
}

func (i *recordingInvoker) Invoke(_ context.Context, id store.GrainId, method string, _, _ any) error {
	i.recorder.mu.Lock()
	i.recorder.steps = append(i.recorder.steps, "invoke")
	i.calls = append(i.calls, id)
	i.recorder.mu.Unlock()
	return nil
}

func (i *recordingInvoker) Owns(store.GrainId) bool {
	return true
}

type blockingInvoker struct {
	started  chan struct{}
	finished chan struct{}
}

type stepRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *stepRecorder) record(step string) {
	r.mu.Lock()
	r.steps = append(r.steps, step)
	r.mu.Unlock()
}

func (i *blockingInvoker) Invoke(ctx context.Context, _ store.GrainId, _ string, _, _ any) error {
	close(i.started)
	<-ctx.Done()
	close(i.finished)
	return ctx.Err()
}

func (i *blockingInvoker) Owns(store.GrainId) bool {
	return true
}

type ownershipInvoker struct {
	owns  bool
	calls atomic.Int32
}

func (i *ownershipInvoker) Owns(store.GrainId) bool {
	return i.owns
}

func (i *ownershipInvoker) Invoke(context.Context, store.GrainId, string, any, any) error {
	i.calls.Add(1)
	return nil
}

func TestPoller_ClaimsBeforeInvoking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0).UTC()
		recorder := &stepRecorder{}
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId:  store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:     "wake",
				Method:   "Wake",
				DueAt:    start.Add(-time.Second),
				Interval: time.Hour,
				ETag:     1,
			}},
			claimWon: true,
			recorder: recorder,
		}
		fakeClock := clock.NewFake(start)
		invoker := &recordingInvoker{recorder: recorder}
		poller := New(backend, fakeClock, time.Second, invoker, testReminderCall)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if got, want := recorder.steps, []string{"list", "claim", "invoke"}; !slices.Equal(got, want) {
			t.Fatalf("steps = %v, want %v", got, want)
		}
	})
}

func TestPoller_AdvancesToFirstFutureTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(200, 0).UTC()
		interval := time.Hour
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId:  store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:     "wake",
				Method:   "Wake",
				DueAt:    start.Add(-3 * interval),
				Interval: interval,
				ETag:     1,
			}},
			claimWon: true,
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		invoker := &recordingInvoker{recorder: backend.recorder}
		poller := New(backend, fakeClock, time.Second, invoker, testReminderCall)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if len(backend.nextDueAt) != 1 {
			t.Fatalf("next due times = %v, want one claim", backend.nextDueAt)
		}
		want := start.Add(interval)
		if !backend.nextDueAt[0].Equal(want) {
			t.Fatalf("next due time = %s, want %s", backend.nextDueAt[0], want)
		}
	})
}

func TestNextDueAt_LargeDowntimeReturnsPromptly(t *testing.T) {
	period := time.Nanosecond
	dueAt := time.Unix(0, 0).UTC()
	now := dueAt.Add(time.Hour)
	reminder := store.Reminder{DueAt: dueAt, Interval: period}

	got := nextDueAt(reminder, now)
	want := now.Add(period)
	if !got.After(now) || !got.Equal(want) {
		t.Fatalf("next due time = %s, want %s strictly after now", got, want)
	}

	future := now.Add(time.Hour)
	reminder.DueAt = future
	if got := nextDueAt(reminder, now); !got.Equal(future) {
		t.Fatalf("future due time = %s, want unchanged %s", got, future)
	}

	reminder.DueAt = now
	reminder.Interval = 2 * time.Nanosecond
	if got := nextDueAt(reminder, now); !got.Equal(now.Add(reminder.Interval)) {
		t.Fatalf("due-now next time = %s, want %s", got, now.Add(reminder.Interval))
	}

	for _, interval := range []time.Duration{0, -time.Nanosecond} {
		reminder.Interval = interval
		if got := nextDueAt(reminder, now); !got.IsZero() {
			t.Fatalf("interval %s next time = %s, want zero", interval, got)
		}
	}

	const maxElapsed = time.Duration(1<<63 - 1)
	overflowDueAt := time.Unix(0, 0).UTC()
	overflowNow := overflowDueAt.Add(maxElapsed)
	reminder.DueAt = overflowDueAt
	reminder.Interval = 2 * time.Nanosecond
	if got := nextDueAt(reminder, overflowNow); !got.After(overflowNow) {
		t.Fatalf("overflow fallback = %s, want strictly after %s", got, overflowNow)
	}
}

func TestPoller_PassesPeriodicTickStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(250, 0).UTC()
		period := time.Hour
		first := start.Add(-3 * period)
		due := start.Add(-period)
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId:       store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:          "wake",
				Method:        "Wake",
				FirstTickTime: first,
				DueAt:         due,
				Interval:      period,
				ETag:          1,
			}},
			claimWon: true,
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		var gotFirst, gotCurrent time.Time
		var gotPeriod time.Duration
		factory := func(_ store.GrainId, _ string, firstTick time.Time, tickPeriod time.Duration, current time.Time) (any, any) {
			gotFirst = firstTick
			gotPeriod = tickPeriod
			gotCurrent = current
			return &struct{}{}, &struct{}{}
		}
		poller := New(backend, fakeClock, time.Second, &recordingInvoker{recorder: backend.recorder}, factory)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if !gotFirst.Equal(first) || gotPeriod != period || !gotCurrent.Equal(due) {
			t.Fatalf("TickStatus = first %s period %s current %s, want %s %s %s", gotFirst, gotPeriod, gotCurrent, first, period, due)
		}
	})
}

func TestPoller_PassesZeroPeriodForOneShot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(275, 0).UTC()
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId:       store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:          "wake",
				Method:        "Wake",
				FirstTickTime: start,
				DueAt:         start,
				ETag:          1,
			}},
			claimWon: true,
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		var gotPeriod time.Duration
		factory := func(_ store.GrainId, _ string, _ time.Time, period time.Duration, _ time.Time) (any, any) {
			gotPeriod = period
			return &struct{}{}, &struct{}{}
		}
		poller := New(backend, fakeClock, time.Second, &recordingInvoker{recorder: backend.recorder}, factory)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if gotPeriod != 0 {
			t.Fatalf("one-shot Period = %s, want 0", gotPeriod)
		}
	})
}

func TestPoller_ClaimFailureDoesNotInvoke(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(300, 0).UTC()
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId: store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:    "wake",
				Method:  "Wake",
				DueAt:   start.Add(-time.Second),
			}},
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		invoker := &recordingInvoker{recorder: backend.recorder}
		poller := New(backend, fakeClock, time.Second, invoker, testReminderCall)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if len(invoker.calls) != 0 {
			t.Fatalf("invocations = %v, want none", invoker.calls)
		}
	})
}

func TestPoller_SkipsSchedulesNotOwnedByInvoker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(350, 0).UTC()
		backend := store.NewMemory()
		schedule := store.Reminder{
			GrainId: store.GrainId{GrainType: "account", GrainKey: "alice"},
			Name:    "wake",
			Method:  "Wake",
			DueAt:   start.Add(-time.Second),
		}
		if err := backend.Put(context.Background(), schedule); err != nil {
			t.Fatalf("Put: %v", err)
		}

		nonOwner := &ownershipInvoker{}
		owner := &ownershipInvoker{owns: true}
		nonOwnerClock := clock.NewFake(start)
		ownerClock := clock.NewFake(start)
		nonOwnerPoller := New(backend, nonOwnerClock, time.Second, nonOwner, testReminderCall)
		ownerPoller := New(backend, ownerClock, time.Second, owner, testReminderCall)
		synctest.Wait()

		nonOwnerClock.Advance(time.Second)
		synctest.Wait()
		if got := nonOwner.calls.Load(); got != 0 {
			t.Fatalf("non-owner invocations = %d, want 0", got)
		}

		ownerClock.Advance(time.Second)
		synctest.Wait()
		if got := owner.calls.Load(); got != 1 {
			t.Fatalf("owner invocations = %d, want 1", got)
		}
		if got := nonOwner.calls.Load(); got != 0 {
			t.Fatalf("non-owner invocations after owner poll = %d, want 0", got)
		}
		nonOwnerPoller.Close()
		ownerPoller.Close()
	})
}

func TestPoller_CloseStopsTheGoroutine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(400, 0).UTC()
		backend := &fakeTable{
			rows: []store.Reminder{{
				GrainId: store.GrainId{GrainType: "account", GrainKey: "alice"},
				Name:    "wake",
				Method:  "Wake",
				DueAt:   start.Add(-time.Second),
			}},
			claimWon: true,
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		invoker := &blockingInvoker{started: make(chan struct{}), finished: make(chan struct{})}
		poller := New(backend, fakeClock, time.Second, invoker, testReminderCall)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()
		poller.Close()
		select {
		case <-invoker.finished:
		default:
			t.Fatal("invocation is still running after Close")
		}
	})
}

var _ Table = (*fakeTable)(nil)
var _ Invoker = (*recordingInvoker)(nil)
var _ Invoker = (*blockingInvoker)(nil)
