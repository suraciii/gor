//go:build sim

package sim

import (
	"context"
	"errors"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/store"
)

type gatedCounterEntity struct {
	value gor.State[int64]
	gate  <-chan struct{}
}

func (c *gatedCounterEntity) Add(ctx context.Context, delta int64) (int64, error) {
	<-c.gate
	next := c.value.Get() + delta
	if err := c.value.Set(ctx, next); err != nil {
		return 0, err
	}
	return next, nil
}

type testCallResult struct {
	value int64
	err   error
}

func TestSim_FaultsAndMailbox(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore()
		gate := make(chan struct{})
		rt := gor.New(
			gor.WithStore(backend),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
			gor.WithMailboxCapacity(4),
		)
		defer rt.Close()
		if err := installCounterType(rt); err != nil {
			t.Fatal(err)
		}
		if err := registerCounter(rt, func(b *gor.Binder) counter {
			return &gatedCounterEntity{
				value: gor.NewState[int64](b, "value"),
				gate:  gate,
			}
		}); err != nil {
			t.Fatal(err)
		}

		counterType := gor.TypeName[counter]()
		id := gor.Identity{Type: counterType, Key: "mailbox"}
		storeID := store.Identity{Type: id.Type, Key: id.Key}
		backend.setFaultPlans(nil)

		first := invokeAsync(rt, id, 1)
		second := invokeAsync(rt, id, 1)
		synctest.Wait()
		select {
		case <-first:
			t.Fatal("first same-entity call completed before the gate opened")
		default:
		}
		select {
		case <-second:
			t.Fatal("second same-entity call completed before the gate opened")
		default:
		}
		close(gate)
		synctest.Wait()
		firstResult := <-first
		secondResult := <-second
		if firstResult.err != nil || secondResult.err != nil {
			t.Fatalf("same-entity calls returned errors: %v, %v", firstResult.err, secondResult.err)
		}
		values := []int64{firstResult.value, secondResult.value}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		if values[0] != 1 || values[1] != 2 {
			t.Fatalf("same-entity results = %v, want [1 2]", values)
		}

		observed := newObservations()
		if err := observed.check(backend, []store.Identity{storeID}); err != nil {
			t.Fatal(err)
		}

		backend.setFaultPlans(map[store.Identity]faultPlan{
			storeID: {write: faultSpec{kind: faultWriteError}},
		})
		result := awaitCall(invokeAsync(rt, id, 1))
		if !errors.Is(result.err, errWriteFailure) {
			t.Fatalf("write failure = %v, want %v", result.err, errWriteFailure)
		}
		if err := observed.check(backend, []store.Identity{storeID}); err != nil {
			t.Fatal(err)
		}

		backend.setFaultPlans(nil)
		result = awaitCall(invokeAsync(rt, id, 1))
		if result.err != nil || result.value != 3 {
			t.Fatalf("reactivated call = (%d, %v), want (3, nil)", result.value, result.err)
		}
		if err := observed.check(backend, []store.Identity{storeID}); err != nil {
			t.Fatal(err)
		}

		readID := gor.Identity{Type: counterType, Key: "read-failure"}
		readStoreID := store.Identity{Type: readID.Type, Key: readID.Key}
		backend.setFaultPlans(map[store.Identity]faultPlan{
			readStoreID: {read: faultSpec{kind: faultReadError}},
		})
		result = awaitCall(invokeAsync(rt, readID, 4))
		if !errors.Is(result.err, errReadFailure) {
			t.Fatalf("read failure = %v, want %v", result.err, errReadFailure)
		}
		backend.setFaultPlans(nil)
		result = awaitCall(invokeAsync(rt, readID, 4))
		if result.err != nil || result.value != 4 {
			t.Fatalf("read recovery call = (%d, %v), want (4, nil)", result.value, result.err)
		}
		if err := observed.check(backend, []store.Identity{storeID, readStoreID}); err != nil {
			t.Fatal(err)
		}

		appliedID := gor.Identity{Type: counterType, Key: "applied-error"}
		appliedStoreID := store.Identity{Type: appliedID.Type, Key: appliedID.Key}
		backend.setFaultPlans(map[store.Identity]faultPlan{
			appliedStoreID: {write: faultSpec{kind: faultWriteAppliedError}},
		})
		result = awaitCall(invokeAsync(rt, appliedID, 2))
		if !errors.Is(result.err, errAppliedWriteFailure) {
			t.Fatalf("applied write failure = %v, want %v", result.err, errAppliedWriteFailure)
		}
		backend.setFaultPlans(nil)
		result = awaitCall(invokeAsync(rt, appliedID, 1))
		if result.err != nil || result.value != 3 {
			t.Fatalf("applied write recovery call = (%d, %v), want (3, nil)", result.value, result.err)
		}
		if err := observed.check(backend, []store.Identity{storeID, readStoreID, appliedStoreID}); err != nil {
			t.Fatal(err)
		}

		delayID := gor.Identity{Type: counterType, Key: "delay"}
		delayStoreID := store.Identity{Type: delayID.Type, Key: delayID.Key}
		backend.setFaultPlans(map[store.Identity]faultPlan{
			delayStoreID: {read: faultSpec{kind: faultDelay, delay: time.Millisecond}},
		})
		result = awaitCall(invokeAsync(rt, delayID, 1))
		if result.err != nil || result.value != 1 {
			t.Fatalf("delayed call = (%d, %v), want (1, nil)", result.value, result.err)
		}
		if backend.delayCount() == 0 {
			t.Fatal("delay fault was not observed by the fake store")
		}
		if err := observed.check(backend, []store.Identity{storeID, readStoreID, appliedStoreID, delayStoreID}); err != nil {
			t.Fatal(err)
		}
	})
}

func invokeAsync(rt *gor.Runtime, id gor.Identity, delta int64) <-chan testCallResult {
	done := make(chan testCallResult, 1)
	go func() {
		var value int64
		err := rt.Invoke(context.Background(), id, "Add", []any{delta}, &value)
		done <- testCallResult{value: value, err: err}
	}()
	return done
}

func awaitCall(done <-chan testCallResult) testCallResult {
	synctest.Wait()
	return <-done
}
