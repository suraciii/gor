//go:build sim

package sim

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

type dualActivationEntity struct {
	value   gor.State[int64]
	gate    <-chan struct{}
	entered chan<- struct{}
}

func (c *dualActivationEntity) Add(ctx context.Context, delta int64) (int64, error) {
	c.entered <- struct{}{}
	<-c.gate
	next := c.value.Get() + delta
	if err := c.value.Set(ctx, next); err != nil {
		return 0, err
	}
	return next, nil
}

func (*dualActivationEntity) Arm(context.Context, string, time.Duration, time.Duration) error {
	return nil
}

func (*dualActivationEntity) Disarm(context.Context, string) error {
	return nil
}

func (*dualActivationEntity) Tick(context.Context) error {
	return nil
}

func newDualActivationRuntime(backend *fakeStore, gate <-chan struct{}, entered chan<- struct{}) (*gor.Runtime, error) {
	rt, err := newRuntime(backend)
	if err != nil {
		return nil, err
	}
	if err := installCounterType(rt); err != nil {
		rt.Close()
		return nil, err
	}
	if err := registerCounter(rt, func(b *gor.Binder) counter {
		return &dualActivationEntity{
			value:   gor.NewState[int64](b, "value"),
			gate:    gate,
			entered: entered,
		}
	}); err != nil {
		rt.Close()
		return nil, err
	}
	return rt, nil
}

func TestSim_DoubleActivationRejectsETagConflict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		gate := make(chan struct{})
		entered := make(chan struct{}, 2)
		first, err := newDualActivationRuntime(backend, gate, entered)
		if err != nil {
			t.Fatal(err)
		}
		second, err := newDualActivationRuntime(backend, gate, entered)
		if err != nil {
			first.Close()
			t.Fatal(err)
		}
		defer first.Close()
		defer second.Close()

		id := gor.Identity{Type: gor.TypeName[counter](), Key: "dual"}
		firstCall := invokeAsync(first, id, 3)
		secondCall := invokeAsync(second, id, 5)
		synctest.Wait()
		<-entered
		<-entered
		close(gate)
		synctest.Wait()

		firstResult := <-firstCall
		secondResult := <-secondCall
		conflicts := 0
		successes := 0
		for _, result := range []testCallResult{firstResult, secondResult} {
			switch {
			case errors.Is(result.err, store.ErrConflict):
				conflicts++
			case result.err == nil:
				successes++
			default:
				t.Fatalf("dual activation result = (%d, %v), want success or ErrConflict", result.value, result.err)
			}
		}
		if conflicts != 1 || successes != 1 {
			t.Fatalf("dual activation results = (%v, %v), want one success and one conflict", firstResult.err, secondResult.err)
		}

		storeID := store.Identity{Type: id.Type, Key: id.Key}
		record := backend.snapshot([]store.Identity{storeID})[storeID]
		if record.ETag != 1 {
			t.Fatalf("dual activation ETag = %d, want 1", record.ETag)
		}
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 3 && value != 5 {
			t.Fatalf("dual activation value = %d, want 3 or 5", value)
		}
		if err := newObservations().check(backend, []store.Identity{storeID}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSim_CrashRestartRestoresState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		rt, err := newCounterRuntimeWithOptions(backend, newTimerTracker())
		if err != nil {
			t.Fatal(err)
		}

		id := gor.Identity{Type: gor.TypeName[counter](), Key: "restart"}
		result := awaitCall(invokeAsync(rt, id, 4))
		if result.err != nil || result.value != 4 {
			t.Fatalf("initial call = (%d, %v), want (4, nil)", result.value, result.err)
		}
		rt.Kill()
		synctest.Wait()

		rt, err = newCounterRuntimeWithOptions(backend, newTimerTracker())
		if err != nil {
			t.Fatal(err)
		}
		defer rt.Close()
		result = awaitCall(invokeAsync(rt, id, 6))
		if result.err != nil || result.value != 10 {
			t.Fatalf("restarted call = (%d, %v), want (10, nil)", result.value, result.err)
		}

		storeID := store.Identity{Type: id.Type, Key: id.Key}
		record := backend.snapshot([]store.Identity{storeID})[storeID]
		value, err := counterValue(record)
		if err != nil {
			t.Fatal(err)
		}
		if value != 10 {
			t.Fatalf("restarted state = %d, want 10", value)
		}
		if err := newObservations().check(backend, []store.Identity{storeID}); err != nil {
			t.Fatal(err)
		}
	})
}
