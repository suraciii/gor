package cluster

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

var (
	ErrProbeTimeout = errors.New("cluster probe timed out")
	ErrProbeClosed  = errors.New("cluster probe reply channel closed")
)

type ProbeResult struct {
	ID  MemberID
	Err error
}

type Prober interface {
	Probe(context.Context, MemberID) <-chan ProbeResult
}

type probePoint struct {
	hash uint64
	id   MemberID
}

type probeTask struct {
	cancel context.CancelFunc
	token  uint64
}

type probeEvent struct {
	target MemberID
	token  uint64
	result ProbeResult
}

func activeMemberIDs(members []store.Member) []MemberID {
	ids := make([]MemberID, 0, len(members))
	for _, member := range members {
		if member.Status != store.MemberActive {
			continue
		}
		ids = append(ids, MemberID{NodeAddr: member.NodeAddr, Generation: member.Generation})
	}
	sort.Slice(ids, func(i, j int) bool {
		return memberIDLess(ids[i], ids[j])
	})
	return ids
}

func probeTargets(members []MemberID, self MemberID) []MemberID {
	points := make([]probePoint, 0, len(members))
	for _, member := range members {
		points = append(points, probePoint{hash: hashParts(member.NodeAddr, member.Generation), id: member})
	}
	sortProbePoints(points)

	selfIndex := -1
	for index, point := range points {
		if point.id == self {
			selfIndex = index
			break
		}
	}
	if selfIndex < 0 || len(points) < 2 {
		return nil
	}

	next := points[(selfIndex+1)%len(points)].id
	previous := points[(selfIndex+len(points)-1)%len(points)].id
	targets := []MemberID{next}
	if previous != next {
		targets = append(targets, previous)
	}
	return targets
}

func sortProbePoints(points []probePoint) {
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		return memberIDLess(points[i].id, points[j].id)
	})
}

func memberIDLess(left, right MemberID) bool {
	if left.NodeAddr != right.NodeAddr {
		return left.NodeAddr < right.NodeAddr
	}
	return left.Generation < right.Generation
}

func reconcileProbeState(targets []MemberID, tasks map[MemberID]probeTask, failures map[MemberID]int) {
	current := make(map[MemberID]struct{}, len(targets))
	for _, target := range targets {
		current[target] = struct{}{}
	}
	for target, task := range tasks {
		if _, ok := current[target]; ok {
			continue
		}
		task.cancel()
		delete(tasks, target)
	}
	for target := range failures {
		if _, ok := current[target]; !ok {
			delete(failures, target)
		}
	}
}

func recordProbeEvent(event probeEvent, tasks map[MemberID]probeTask, failures map[MemberID]int) {
	task, ok := tasks[event.target]
	if !ok || task.token != event.token {
		return
	}
	delete(tasks, event.target)
	if event.result.Err == nil && event.result.ID == event.target {
		failures[event.target] = 0
		return
	}
	failures[event.target]++
}

func waitForProbe(ctx context.Context, sourceClock clock.Clock, prober Prober, target MemberID, timeout time.Duration) ProbeResult {
	probeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	replies := prober.Probe(probeContext, target)
	timer := sourceClock.NewTicker(timeout)
	defer timer.Stop()
	select {
	case <-probeContext.Done():
		return ProbeResult{Err: probeContext.Err()}
	case result, ok := <-replies:
		if !ok {
			return ProbeResult{Err: ErrProbeClosed}
		}
		return result
	case <-timer.C():
		return ProbeResult{Err: ErrProbeTimeout}
	}
}
