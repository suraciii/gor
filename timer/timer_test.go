package timer

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type fakeTable struct {
	rows     []store.Schedule
	claimWon bool

	recorder  *stepRecorder
	nextDueAt []time.Time
}

func (t *fakeTable) ListDue(context.Context, time.Time) ([]store.Schedule, error) {
	t.recorder.record("list")
	return append([]store.Schedule(nil), t.rows...), nil
}

func (t *fakeTable) Claim(_ context.Context, schedule store.Schedule, nextDueAt time.Time) (bool, error) {
	t.recorder.mu.Lock()
	t.recorder.steps = append(t.recorder.steps, "claim")
	t.nextDueAt = append(t.nextDueAt, nextDueAt)
	t.recorder.mu.Unlock()
	return t.claimWon, nil
}

type recordingInvoker struct {
	recorder *stepRecorder
	calls    []store.Identity
}

func (i *recordingInvoker) Invoke(_ context.Context, id store.Identity, method string) error {
	i.recorder.mu.Lock()
	i.recorder.steps = append(i.recorder.steps, "invoke")
	i.calls = append(i.calls, id)
	i.recorder.mu.Unlock()
	return nil
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

func (i *blockingInvoker) Invoke(ctx context.Context, _ store.Identity, _ string) error {
	close(i.started)
	<-ctx.Done()
	close(i.finished)
	return ctx.Err()
}

func TestPoller_ClaimsBeforeInvoking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0).UTC()
		recorder := &stepRecorder{}
		backend := &fakeTable{
			rows: []store.Schedule{{
				Identity: store.Identity{Type: "account", Key: "alice"},
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
		poller := New(backend, fakeClock, time.Second, invoker)
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
			rows: []store.Schedule{{
				Identity: store.Identity{Type: "account", Key: "alice"},
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
		poller := New(backend, fakeClock, time.Second, invoker)
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

func TestPoller_ClaimFailureDoesNotInvoke(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(300, 0).UTC()
		backend := &fakeTable{
			rows: []store.Schedule{{
				Identity: store.Identity{Type: "account", Key: "alice"},
				Name:     "wake",
				Method:   "Wake",
				DueAt:    start.Add(-time.Second),
			}},
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		invoker := &recordingInvoker{recorder: backend.recorder}
		poller := New(backend, fakeClock, time.Second, invoker)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()
		poller.Close()

		if len(invoker.calls) != 0 {
			t.Fatalf("invocations = %v, want none", invoker.calls)
		}
	})
}

func TestPoller_CloseStopsTheGoroutine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(400, 0).UTC()
		backend := &fakeTable{
			rows: []store.Schedule{{
				Identity: store.Identity{Type: "account", Key: "alice"},
				Name:     "wake",
				Method:   "Wake",
				DueAt:    start.Add(-time.Second),
			}},
			claimWon: true,
			recorder: &stepRecorder{},
		}
		fakeClock := clock.NewFake(start)
		invoker := &blockingInvoker{started: make(chan struct{}), finished: make(chan struct{})}
		poller := New(backend, fakeClock, time.Second, invoker)
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
