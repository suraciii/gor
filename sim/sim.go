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
	simulationSteps        = 24
	callsPerStep           = 2
	simulationStepDuration = time.Millisecond
)

type counter interface {
	Add(context.Context, int64) (int64, error)
	Arm(context.Context, string, time.Duration, time.Duration) error
	Disarm(context.Context, string) error
	Tick(context.Context) error
}

type counterEntity struct {
	value    gor.State[int64]
	id       gor.Identity
	schedule gor.Schedule
	tracker  *timerTracker
}

func (c *counterEntity) Add(ctx context.Context, delta int64) (int64, error) {
	next := c.value.Get() + delta
	if err := c.value.Set(ctx, next); err != nil {
		return 0, err
	}
	return next, nil
}

func (c *counterEntity) Arm(ctx context.Context, name string, delay, interval time.Duration) error {
	when := gor.After(delay)
	if interval > 0 {
		when = gor.Every(interval)
	}
	return c.schedule.Set(ctx, name, when, "Tick")
}

func (c *counterEntity) Disarm(ctx context.Context, name string) error {
	return c.schedule.Cancel(ctx, name)
}

func (c *counterEntity) Tick(context.Context) error {
	c.tracker.deliver(storeIdentity(c.id))
	return nil
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

func (p *counterProxy) Arm(ctx context.Context, name string, delay, interval time.Duration) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", []any{name, delay, interval}, nil)
}

func (p *counterProxy) Disarm(ctx context.Context, name string) error {
	return p.invoker.Invoke(ctx, p.id, "Disarm", []any{name}, nil)
}

func (p *counterProxy) Tick(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Tick", nil, nil)
}

func dispatchCounter(ctx context.Context, instance counter, method string, args []any, reply any) error {
	switch method {
	case "Add":
		value, err := instance.Add(ctx, args[0].(int64))
		if err != nil {
			return err
		}
		*(reply.(*int64)) = value
		return nil
	case "Arm":
		return instance.Arm(ctx, args[0].(string), args[1].(time.Duration), args[2].(time.Duration))
	case "Disarm":
		return instance.Disarm(ctx, args[0].(string))
	case "Tick":
		return instance.Tick(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func installCounterType(rt *gor.Runtime) error {
	return gor.InstallType[counter](rt, dispatchCounter, func(invoker gor.Invoker, id gor.Identity) counter {
		return &counterProxy{invoker: invoker, id: id}
	})
}

func registerCounter(rt *gor.Runtime, factory func(*gor.Binder) counter) error {
	return gor.Register[counter](rt, factory)
}

func installCounterWithTracker(rt *gor.Runtime, tracker *timerTracker) error {
	if err := installCounterType(rt); err != nil {
		return err
	}
	return registerCounter(rt, func(b *gor.Binder) counter {
		return &counterEntity{
			value:    gor.NewState[int64](b, "value"),
			id:       gor.Self(b),
			schedule: gor.NewSchedule(b),
			tracker:  tracker,
		}
	})
}

func newRuntime(backend *fakeStore) (*gor.Runtime, error) {
	return gor.New(
		gor.WithStore(backend),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithScheduleInterval(simulationStepDuration),
		gor.WithMailboxCapacity(4),
	)
}

func newCounterRuntime(backend *fakeStore, tracker *timerTracker) (*gor.Runtime, error) {
	rt, err := newRuntime(backend)
	if err != nil {
		return nil, err
	}
	if err := installCounterWithTracker(rt, tracker); err != nil {
		rt.Close()
		return nil, err
	}
	return rt, nil
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

func chooseScheduleFault(rng *rand.Rand) scheduleFaultKind {
	switch rng.IntN(5) {
	case 0:
		return scheduleFaultNone
	case 1:
		return scheduleListError
	case 2:
		return scheduleListDelay
	case 3:
		return scheduleClaimError
	default:
		return scheduleClaimAppliedError
	}
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

func runSimulation(seed uint64, nodeCount int) (string, error) {
	log := eventLog{}
	log.add("seed=%08x", seed)

	timerTracker := newTimerTracker()
	backend := newFakeStore(timerTracker)
	cluster, err := newSimulationCluster(backend, nodeCount, timerTracker)
	if err != nil {
		return log.String(), err
	}
	defer cluster.close()

	counterType := gor.TypeName[counter]()
	entities := []store.Identity{
		{Type: counterType, Key: "a"},
		{Type: counterType, Key: "b"},
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))
	observations := newObservations()
	history := newCounterHistory()

	for step := 0; step < simulationSteps; step++ {
		switch chooseClusterAction(rng, cluster) {
		case clusterCall:
			entity := entities[rng.IntN(len(entities))]
			id := gor.Identity{Type: entity.Type, Key: entity.Key}
			plan := chooseFaultPlan(rng)
			storeID := storeIdentity(id)
			cluster.backend.setFaultPlans(map[store.Identity]faultPlan{storeID: plan})

			liveIDs := cluster.liveNodeIDs()
			decisions := make([]decision, callsPerStep)
			nodes := make([]int, callsPerStep)
			deltas := make([]int64, callsPerStep)
			for index := range decisions {
				nodeID := liveIDs[rng.IntN(len(liveIDs))]
				delta := int64(rng.IntN(9) + 1)
				decisions[index] = decision{
					rt:    cluster.nodes[nodeID].rt,
					id:    id,
					delta: delta,
				}
				nodes[index] = nodeID
				deltas[index] = delta
			}
			log.addCallDecision(nodes, storeID, plan, deltas)
			var crashNode *int
			if rng.IntN(2) == 0 {
				node := nodes[rng.IntN(len(nodes))]
				crashNode = &node
				log.addCrashDecision(node)
			}
			outcomes, err := executeDecisions(cluster, decisions, crashNode, history)
			if err != nil {
				return log.String(), fmt.Errorf("step %d: %w", step, err)
			}
			log.addOutcomes(outcomes)
		case clusterCrash:
			liveIDs := cluster.liveNodeIDs()
			nodeID := liveIDs[rng.IntN(len(liveIDs))]
			log.addCrashDecision(nodeID)
			if err := cluster.crash(nodeID); err != nil {
				return log.String(), fmt.Errorf("step %d: %w", step, err)
			}
		case clusterRestart:
			stoppedIDs := cluster.stoppedNodeIDs()
			nodeID := stoppedIDs[rng.IntN(len(stoppedIDs))]
			log.addRestartDecision(nodeID)
			if err := cluster.restart(nodeID); err != nil {
				return log.String(), fmt.Errorf("step %d: %w", step, err)
			}
		case clusterSchedule:
			liveIDs := cluster.liveNodeIDs()
			nodeID := liveIDs[rng.IntN(len(liveIDs))]
			entity := entities[rng.IntN(len(entities))]
			id := gor.Identity{Type: entity.Type, Key: entity.Key}
			backend.setFaultPlans(nil)
			name := "wake-" + entity.Key
			delay := time.Duration(rng.IntN(3)+1) * simulationStepDuration
			interval := time.Duration(0)
			if rng.IntN(2) == 1 {
				interval = time.Duration(rng.IntN(2)+2) * simulationStepDuration
				delay = interval
			}
			fault := chooseScheduleFault(rng)
			log.addScheduleDecision(nodeID, entity, name, delay, interval, fault)
			err := cluster.nodes[nodeID].rt.Invoke(context.Background(), id, "Arm", []any{name, delay, interval}, nil)
			outcome, classifyErr := classifyOutcome(err)
			if classifyErr != nil {
				return log.String(), fmt.Errorf("step %d: schedule %s: %w", step, name, classifyErr)
			}
			log.addScheduleOutcome("schedule", nodeID, outcome)
			if err == nil {
				backend.setScheduleFault(storeIdentity(id), fault)
			}
		case clusterDisarm:
			liveIDs := cluster.liveNodeIDs()
			nodeID := liveIDs[rng.IntN(len(liveIDs))]
			entity := entities[rng.IntN(len(entities))]
			id := gor.Identity{Type: entity.Type, Key: entity.Key}
			name := "wake-" + entity.Key
			backend.setFaultPlans(nil)
			log.addDisarmDecision(nodeID, entity, name)
			err := cluster.nodes[nodeID].rt.Invoke(context.Background(), id, "Disarm", []any{name}, nil)
			outcome, classifyErr := classifyOutcome(err)
			if classifyErr != nil {
				return log.String(), fmt.Errorf("step %d: disarm %s: %w", step, name, classifyErr)
			}
			log.addScheduleOutcome("disarm", nodeID, outcome)
		}

		synctest.Wait()
		if err := observations.check(backend, entities); err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		if err := logEntityStates(&log, backend, entities); err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		if err := history.check(); err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		log.addScheduleObservation(backend.scheduleStats(), timerTracker.deliveryCount())
		if err := timerTracker.check(); err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		time.Sleep(simulationStepDuration)
		synctest.Wait()
	}
	if err := timerTracker.check(); err != nil {
		log.addScheduleObservation(backend.scheduleStats(), timerTracker.deliveryCount())
		return log.String(), err
	}
	return log.String(), nil
}
