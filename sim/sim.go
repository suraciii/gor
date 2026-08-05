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

type counterAddRequest struct {
	A0 int64
}

type counterAddReply struct {
	R0 int64
}

type counterArmRequest struct {
	A0 string
	A1 time.Duration
	A2 time.Duration
}

type counterArmReply struct{}

type counterDisarmRequest struct {
	A0 string
}

type counterDisarmReply struct{}

type counterTickRequest struct{}

type counterTickReply struct{}

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
	var reply counterAddReply
	err := p.invoker.Invoke(ctx, p.id, "Add", &counterAddRequest{A0: delta}, &reply)
	return reply.R0, err
}

func (p *counterProxy) Arm(ctx context.Context, name string, delay, interval time.Duration) error {
	return p.invoker.Invoke(ctx, p.id, "Arm", &counterArmRequest{A0: name, A1: delay, A2: interval}, &counterArmReply{})
}

func (p *counterProxy) Disarm(ctx context.Context, name string) error {
	return p.invoker.Invoke(ctx, p.id, "Disarm", &counterDisarmRequest{A0: name}, &counterDisarmReply{})
}

func (p *counterProxy) Tick(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Tick", &counterTickRequest{}, &counterTickReply{})
}

func dispatchCounter(ctx context.Context, instance counter, method string, args any, reply any) error {
	switch method {
	case "Add":
		typedArgs := args.(*counterAddRequest)
		typedReply := reply.(*counterAddReply)
		value, err := instance.Add(ctx, typedArgs.A0)
		if err != nil {
			return err
		}
		typedReply.R0 = value
		return nil
	case "Arm":
		typedArgs := args.(*counterArmRequest)
		return instance.Arm(ctx, typedArgs.A0, typedArgs.A1, typedArgs.A2)
	case "Disarm":
		typedArgs := args.(*counterDisarmRequest)
		return instance.Disarm(ctx, typedArgs.A0)
	case "Tick":
		return instance.Tick(ctx)
	default:
		return fmt.Errorf("unknown method %q", method)
	}
}

func newCounterCall(method string) (args any, reply any) {
	switch method {
	case "Add":
		return &counterAddRequest{}, &counterAddReply{}
	case "Arm":
		return &counterArmRequest{}, &counterArmReply{}
	case "Disarm":
		return &counterDisarmRequest{}, &counterDisarmReply{}
	case "Tick":
		return &counterTickRequest{}, &counterTickReply{}
	default:
		return nil, nil
	}
}

func installCounterType(rt *gor.Runtime) error {
	return gor.InstallType[counter](rt, dispatchCounter, func(invoker gor.Invoker, id gor.Identity) counter {
		return &counterProxy{invoker: invoker, id: id}
	}, newCounterCall)
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

func baseRuntimeOptions(backend *fakeStore) []gor.Option {
	return []gor.Option{
		gor.WithStore(backend),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithScheduleInterval(simulationStepDuration),
		gor.WithMailboxCapacity(4),
	}
}

func newRuntime(backend *fakeStore) (*gor.Runtime, error) {
	return gor.New(baseRuntimeOptions(backend)...)
}

func newCounterRuntimeWithOptions(backend *fakeStore, tracker *timerTracker, options ...gor.Option) (*gor.Runtime, error) {
	rt, err := gor.New(append(baseRuntimeOptions(backend), options...)...)
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

const probeCount = 64

func probeIdentities(entityType string) []store.Identity {
	probes := make([]store.Identity, probeCount)
	for index := range probes {
		probes[index] = store.Identity{Type: entityType, Key: fmt.Sprintf("probe-%03d", index)}
	}
	return probes
}

var randomFaultKinds = [...]faultKind{
	faultNone,
	faultReadError,
	faultWriteError,
	faultWriteAppliedError,
	faultDelay,
}

var randomMemberFaultKinds = [...]memberFaultKind{
	memberFaultNone,
	memberListError,
	memberCASFailure,
	memberCASAppliedError,
	memberDelay,
}

func chooseFaultPlan(rng *rand.Rand) faultPlan {
	plan := faultPlan{}
	kind := randomFaultKinds[rng.IntN(len(randomFaultKinds))]
	switch kind {
	case faultNone:
		return plan
	case faultReadError:
		plan.read = faultSpec{kind: faultReadError}
	case faultWriteError:
		plan.write = faultSpec{kind: faultWriteError}
	case faultWriteAppliedError:
		plan.write = faultSpec{kind: faultWriteAppliedError}
	case faultDelay:
		delay := time.Duration(rng.IntN(3)+1) * time.Millisecond
		if rng.IntN(2) == 0 {
			plan.read = faultSpec{kind: faultDelay, delay: delay}
			return plan
		}
		plan.write = faultSpec{kind: faultDelay, delay: delay}
	}
	return plan
}

func chooseMemberFault(rng *rand.Rand) memberFaultSpec {
	kind := randomMemberFaultKinds[rng.IntN(len(randomMemberFaultKinds))]
	if kind == memberDelay {
		return memberFaultSpec{kind: kind, delay: time.Duration(rng.IntN(3)+4) * time.Millisecond}
	}
	return memberFaultSpec{kind: kind}
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
	case simErrorIs(err, errReadFailure):
		return "store-read-error", nil
	case simErrorIs(err, errWriteFailure):
		return "store-write-error", nil
	case simErrorIs(err, errAppliedWriteFailure):
		return "store-write-applied-then-error", nil
	case simErrorIs(err, errMemberListFailure):
		return "member-list-error", nil
	case simErrorIs(err, errMemberAppliedFailure):
		return "member-write-applied-then-error", nil
	case simErrorIs(err, gor.ErrNodeDead):
		return "cluster-node-dead", nil
	case simErrorIs(err, gor.ErrPersistenceConflict), simErrorIs(err, store.ErrConflict):
		return "store-conflict", nil
	case simErrorIs(err, gor.ErrPersistenceFailed):
		return "store-write-unknown", nil
	case simErrorIs(err, gor.ErrNoOwner):
		return "wrong-owner", nil
	case simErrorIs(err, gor.ErrRuntimeClosed):
		return "closed", nil
	case simErrorIs(err, gor.ErrOverloaded):
		return "overloaded", nil
	case simErrorIs(err, context.Canceled):
		return "canceled", nil
	default:
		return "", fmt.Errorf("unclassified invocation error: %w", err)
	}
}

func simErrorIs(err, target error) bool {
	return errors.Is(err, target)
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
	probes := probeIdentities(counterType)
	checkedIdentities := append(append([]store.Identity(nil), entities...), probes...)
	rng := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))
	observations := newObservations()
	history := newCounterHistory()

	for step := 0; step < simulationSteps; step++ {
		cluster.backend.setFaultPlans(nil)
		action := chooseClusterAction(rng, cluster)
		memberFault := chooseMemberFault(rng)
		cluster.backend.setMemberFault(memberFault)
		switch action {
		case clusterCall:
			entity := entities[rng.IntN(len(entities))]
			id := gor.Identity{Type: entity.Type, Key: entity.Key}
			plan := chooseFaultPlan(rng)
			plan.member = memberFault
			storeID := storeIdentity(id)
			cluster.backend.setFaultPlans(map[store.Identity]faultPlan{storeID: plan})
			cluster.backend.setMemberFault(plan.member)

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
			log.addCrashDecision(nodeID, memberFault)
			if err := cluster.crash(nodeID); err != nil {
				return log.String(), fmt.Errorf("step %d: %w", step, err)
			}
		case clusterRestart:
			stoppedIDs := cluster.stoppedNodeIDs()
			nodeID := stoppedIDs[rng.IntN(len(stoppedIDs))]
			log.addRestartDecision(nodeID, memberFault)
			if err := cluster.restart(nodeID); err != nil {
				outcome, classifyErr := classifyOutcome(err)
				if classifyErr != nil {
					return log.String(), fmt.Errorf("step %d: restart node %d: %w", step, nodeID, classifyErr)
				}
				log.addClusterOutcome("restart", nodeID, outcome)
			} else {
				log.addClusterOutcome("restart", nodeID, "ok")
			}
		case clusterLeave:
			liveIDs := cluster.liveNodeIDs()
			nodeID := liveIDs[rng.IntN(len(liveIDs))]
			log.addLeaveDecision(nodeID, memberFault)
			if err := cluster.leave(nodeID); err != nil {
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
			log.addScheduleDecision(nodeID, entity, name, delay, interval, fault, memberFault)
			err := cluster.nodes[nodeID].rt.Invoke(context.Background(), id, "Arm", &counterArmRequest{A0: name, A1: delay, A2: interval}, &counterArmReply{})
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
			log.addDisarmDecision(nodeID, entity, name, memberFault)
			err := cluster.nodes[nodeID].rt.Invoke(context.Background(), id, "Disarm", &counterDisarmRequest{A0: name}, &counterDisarmReply{})
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
		cluster.advance(simulationStepDuration)
		cluster.settle()
		if err := cluster.checkInvariants(checkedIdentities); err != nil {
			return log.String(), fmt.Errorf("step %d: %w", step, err)
		}
		log.addMemberObservation(backend.memberStatsSnapshot())
	}
	if err := timerTracker.check(); err != nil {
		log.addScheduleObservation(backend.scheduleStats(), timerTracker.deliveryCount())
		return log.String(), err
	}
	return log.String(), nil
}
