//go:build sim

package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/mail"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
)

const simulationSeed uint64 = 0x8f3c2a1b

const (
	simulationSteps = 12
	callsPerStep    = 2
)

type counter interface {
	Add(context.Context, int64) (int64, error)
}

type counterEntity struct {
	value gor.State[int64]
}

func (c *counterEntity) Add(ctx context.Context, delta int64) (int64, error) {
	next := c.value.Get() + delta
	if err := c.value.Set(ctx, next); err != nil {
		return 0, err
	}
	return next, nil
}

type counterProxy struct {
	invoker gor.Invoker
	id      gor.Identity
}

func (p *counterProxy) Add(ctx context.Context, delta int64) (int64, error) {
	var value int64
	err := p.invoker.Invoke(ctx, p.id, "Add", []any{delta}, &value)
	return value, err
}

func dispatchCounter(ctx context.Context, instance counter, method string, args []any, reply any) error {
	if method != "Add" {
		return fmt.Errorf("unknown method %q", method)
	}
	value, err := instance.Add(ctx, args[0].(int64))
	if err != nil {
		return err
	}
	*(reply.(*int64)) = value
	return nil
}

func installCounterType(rt *gor.Runtime) error {
	return gor.InstallType[counter](rt, dispatchCounter, func(invoker gor.Invoker, id gor.Identity) counter {
		return &counterProxy{invoker: invoker, id: id}
	})
}

func registerCounter(rt *gor.Runtime, factory func(*gor.Binder) counter) error {
	return gor.Register[counter](rt, factory)
}

func installCounter(rt *gor.Runtime) error {
	if err := installCounterType(rt); err != nil {
		return err
	}
	return registerCounter(rt, func(b *gor.Binder) counter {
		return &counterEntity{
			value: gor.NewState[int64](b, "value"),
		}
	})
}

type decision struct {
	id    gor.Identity
	delta int64
}

func executeDecisions(rt *gor.Runtime, decisions []decision) ([]string, error) {
	results := make(chan error, len(decisions))
	for _, selected := range decisions {
		selected := selected
		go func() {
			results <- rt.Invoke(context.Background(), selected.id, "Add", []any{selected.delta}, new(int64))
		}()
	}
	synctest.Wait()

	outcomes := make([]string, 0, len(decisions))
	for range decisions {
		outcome, err := classifyOutcome(<-results)
		if err != nil {
			return nil, err
		}
		outcomes = append(outcomes, outcome)
	}
	sort.Strings(outcomes)
	return outcomes, nil
}

func runSimulation(seed uint64) (string, error) {
	log := eventLog{}
	log.add("seed=%08x", seed)

	backend := newFakeStore()
	rt := gor.New(
		gor.WithStore(backend),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithMailboxCapacity(4),
	)
	defer rt.Close()
	if err := installCounter(rt); err != nil {
		return log.String(), err
	}

	counterType := gor.TypeName[counter]()
	entities := []store.Identity{
		{Type: counterType, Key: "a"},
		{Type: counterType, Key: "b"},
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	observations := newObservations()

	for step := 0; step < simulationSteps; step++ {
		entity := entities[rng.IntN(len(entities))]
		id := gor.Identity{Type: entity.Type, Key: entity.Key}
		plan := chooseFaultPlan(rng)
		storeID := storeIdentity(id)
		backend.setFaultPlans(map[store.Identity]faultPlan{storeID: plan})

		decisions := make([]decision, callsPerStep)
		deltas := make([]int64, callsPerStep)
		for index := range decisions {
			delta := int64(rng.IntN(9) + 1)
			decisions[index] = decision{id: id, delta: delta}
			deltas[index] = delta
		}
		log.addDecision(storeID, plan, deltas)
		outcomes, err := executeDecisions(rt, decisions)
		if err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		log.addOutcomes(outcomes)
		if err := observations.check(backend, entities); err != nil {
			return log.String(), err
		}
		if err := logEntityStates(&log, backend, entities); err != nil {
			return log.String(), err
		}
	}
	return log.String(), nil
}

func storeIdentity(id gor.Identity) store.Identity {
	return store.Identity{Type: id.Type, Key: id.Key}
}

var randomFaultKinds = [...]faultKind{
	faultNone,
	faultReadError,
	faultWriteError,
	faultWriteAppliedError,
	faultDelay,
}

func chooseFaultPlan(rng *rand.Rand) faultPlan {
	kind := randomFaultKinds[rng.IntN(len(randomFaultKinds))]
	switch kind {
	case faultNone:
		return faultPlan{}
	case faultReadError:
		return faultPlan{read: faultSpec{kind: faultReadError}}
	case faultWriteError:
		return faultPlan{write: faultSpec{kind: faultWriteError}}
	case faultWriteAppliedError:
		return faultPlan{write: faultSpec{kind: faultWriteAppliedError}}
	case faultDelay:
		delay := time.Duration(rng.IntN(3)+1) * time.Millisecond
		if rng.IntN(2) == 0 {
			return faultPlan{read: faultSpec{kind: faultDelay, delay: delay}}
		}
		return faultPlan{write: faultSpec{kind: faultDelay, delay: delay}}
	}
	return faultPlan{}
}

func classifyOutcome(err error) (string, error) {
	switch {
	case err == nil:
		return "ok", nil
	case errors.Is(err, errReadFailure):
		return "store-read-error", nil
	case errors.Is(err, errWriteFailure):
		return "store-write-error", nil
	case errors.Is(err, errAppliedWriteFailure):
		return "store-write-applied-then-error", nil
	case errors.Is(err, store.ErrConflict):
		return "store-conflict", nil
	case errors.Is(err, mail.ErrClosed), errors.Is(err, runtimepkg.ErrRuntimeClosed):
		return "closed", nil
	case errors.Is(err, mail.ErrOverloaded):
		return "overloaded", nil
	case errors.Is(err, context.Canceled):
		return "canceled", nil
	default:
		return "", fmt.Errorf("unclassified invocation error: %w", err)
	}
}

func logEntityStates(log *eventLog, backend *fakeStore, ids []store.Identity) error {
	records := backend.snapshot(ids)
	ordered := append([]store.Identity(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Type != ordered[j].Type {
			return ordered[i].Type < ordered[j].Type
		}
		return ordered[i].Key < ordered[j].Key
	})
	for _, id := range ordered {
		value, err := counterValue(records[id])
		if err != nil {
			return fmt.Errorf("decode state for %s/%s: %w", id.Type, id.Key, err)
		}
		log.addState(id, value)
	}
	return nil
}

func counterValue(record store.Record) (int64, error) {
	if len(record.Data) == 0 {
		return 0, nil
	}
	var document struct {
		Value int64 `json:"value"`
	}
	if err := json.Unmarshal(record.Data, &document); err != nil {
		return 0, err
	}
	return document.Value, nil
}

type observations struct {
	commitOffset int
	lastETag     map[store.Identity]store.ETag
	knownData    map[store.Identity]map[string]struct{}
}

func newObservations() *observations {
	return &observations{
		lastETag:  make(map[store.Identity]store.ETag),
		knownData: make(map[store.Identity]map[string]struct{}),
	}
}

func (o *observations) check(backend *fakeStore, ids []store.Identity) error {
	writes, offset := backend.committedWritesSince(o.commitOffset)
	for _, write := range writes {
		if o.knownData[write.id] == nil {
			o.knownData[write.id] = make(map[string]struct{})
		}
		o.knownData[write.id][string(write.data)] = struct{}{}
	}
	o.commitOffset = offset

	records := backend.snapshot(ids)
	for _, id := range ids {
		record := records[id]
		if record.ETag < o.lastETag[id] {
			return fmt.Errorf("etag regressed for %s/%s: got %d after %d", id.Type, id.Key, record.ETag, o.lastETag[id])
		}
		o.lastETag[id] = record.ETag
		if record.ETag > 0 {
			if _, ok := o.knownData[id][string(record.Data)]; !ok {
				return fmt.Errorf("content was not observed in a committed write for %s/%s: %q", id.Type, id.Key, record.Data)
			}
		}
	}
	return nil
}
