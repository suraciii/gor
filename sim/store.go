//go:build sim

package sim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type faultKind uint8

const (
	faultNone faultKind = iota
	faultReadError
	faultWriteError
	faultWriteAppliedError
	faultDelay
)

type faultSpec struct {
	kind  faultKind
	delay time.Duration
}

type faultPlan struct {
	read   faultSpec
	write  faultSpec
	member memberFaultSpec
}

type memberFaultKind uint8

const (
	memberFaultNone memberFaultKind = iota
	memberListError
	memberCASFailure
	memberCASAppliedError
	memberDelay
)

// memberFaultTarget is the seed-drawn target of a member fault. The list kind
// binds a node, matched by caller; the write and delay kinds bind a member
// row, matched by key.
type memberFaultTarget struct {
	addr string
	row  fakeMemberKey
}

type memberFaultSpec struct {
	kind    memberFaultKind
	target  memberFaultTarget
	delay   time.Duration
	started chan struct{}
}

type readBarrier struct {
	started chan<- struct{}
	release <-chan struct{}
}

type scheduleFaultKind uint8

const (
	scheduleFaultNone scheduleFaultKind = iota
	scheduleListError
	scheduleListDelay
	scheduleClaimError
	scheduleClaimAppliedError
)

// scheduleFaultSpec carries the seed-drawn target for the list kinds: the
// fault fires only on a ListDue issued by targetNode's poller. Claim kinds are
// keyed by entity identity and take no target.
type scheduleFaultSpec struct {
	kind       scheduleFaultKind
	targetNode int
}

const scheduleListDelayDuration = 100 * time.Microsecond

type scheduleStats struct {
	listCalls          int
	claimWon           int
	claimLost          int
	listErrors         int
	listDelays         int
	claimErrors        int
	claimAppliedErrors int
}

type memberStats struct {
	listCalls        int
	writeCalls       int
	listErrors       int
	casConflicts     int
	casAppliedErrors int
	delays           int
	deadWrites       int
	delayedDeadCAS   int
}

var (
	errReadFailure                 = errors.New("sim store read failure")
	errWriteFailure                = errors.New("sim store write failure")
	errAppliedWriteFailure         = errors.New("sim store write applied before failure")
	errScheduleListFailure         = errors.New("sim schedule list failure")
	errScheduleClaimFailure        = errors.New("sim schedule claim failure")
	errScheduleClaimAppliedFailure = errors.New("sim schedule claim applied before failure")
	errMemberListFailure           = errors.New("sim member list failure")
	errMemberAppliedFailure        = errors.New("sim member write applied before failure")
)

type scheduleKey struct {
	identity store.GrainId
	name     string
}

type writeEvent struct {
	id   store.GrainId
	data []byte
}

type fakeMemberKey struct {
	nodeAddr   string
	generation string
}

type memberWriteEvent struct {
	key    fakeMemberKey
	status store.MemberStatus
}

type fakeStore struct {
	mu                  sync.Mutex
	records             map[store.GrainId]store.Record
	plans               map[store.GrainId]faultPlan
	readBarriers        map[store.GrainId]readBarrier
	members             map[fakeMemberKey]store.Member
	memberFault         memberFaultSpec
	schedules           map[scheduleKey]store.Schedule
	scheduleListFault   scheduleFaultSpec
	scheduleClaimFaults map[store.GrainId]scheduleFaultKind
	timerTracker        *timerTracker
	memberClock         clock.Clock
	stats               scheduleStats
	memberStats         memberStats
	writes              []writeEvent
	memberWrites        []memberWriteEvent
	delays              int
	active              int
	idle                chan struct{}
}

var _ store.Store = (*fakeStore)(nil)
var _ store.ScheduleStore = (*fakeStore)(nil)
var _ store.MemberStore = (*fakeStore)(nil)

func newFakeStore(tracker *timerTracker) *fakeStore {
	idle := make(chan struct{})
	close(idle)
	return &fakeStore{
		records:             make(map[store.GrainId]store.Record),
		plans:               make(map[store.GrainId]faultPlan),
		readBarriers:        make(map[store.GrainId]readBarrier),
		members:             make(map[fakeMemberKey]store.Member),
		schedules:           make(map[scheduleKey]store.Schedule),
		scheduleClaimFaults: make(map[store.GrainId]scheduleFaultKind),
		timerTracker:        tracker,
		memberClock:         clock.Real{},
		idle:                idle,
	}
}

func (s *fakeStore) setMemberClock(memberClock clock.Clock) {
	s.mu.Lock()
	s.memberClock = memberClock
	s.mu.Unlock()
}

func (s *fakeStore) memberTableNow() time.Time {
	s.mu.Lock()
	memberClock := s.memberClock
	s.mu.Unlock()
	return memberClock.Now()
}

func (s *fakeStore) setFaultPlans(plans map[store.GrainId]faultPlan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans = make(map[store.GrainId]faultPlan, len(plans))
	for id, plan := range plans {
		s.plans[id] = plan
	}
}

func (s *fakeStore) setMemberFault(fault memberFaultSpec) {
	s.mu.Lock()
	s.memberFault = fault
	s.mu.Unlock()
}

func (s *fakeStore) setScheduleFault(id store.GrainId, fault scheduleFaultSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleListFault = scheduleFaultSpec{}
	delete(s.scheduleClaimFaults, id)
	switch fault.kind {
	case scheduleListError, scheduleListDelay:
		s.scheduleListFault = fault
	case scheduleClaimError, scheduleClaimAppliedError:
		s.scheduleClaimFaults[id] = fault.kind
	}
}

func (s *fakeStore) setScheduleListFault(fault scheduleFaultSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleListFault = fault
}

func nodeAddress(node int) string {
	return fmt.Sprintf("node-%d", node)
}

func (s *fakeStore) faultPlan(id store.GrainId) faultPlan {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plans[id]
}

func (s *fakeStore) setReadBarrier(id store.GrainId, barrier readBarrier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if barrier.release == nil {
		delete(s.readBarriers, id)
		return
	}
	s.readBarriers[id] = barrier
}

func (s *fakeStore) readBarrier(id store.GrainId) readBarrier {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readBarriers[id]
}

func (s *fakeStore) Read(_ context.Context, id store.GrainId) (store.Record, error) {
	defer s.endOperation(s.beginOperation())
	plan := s.faultPlan(id).read
	if plan.kind == faultDelay {
		s.recordDelay()
		time.Sleep(plan.delay)
	}
	if plan.kind == faultReadError {
		return store.Record{}, errReadFailure
	}

	s.mu.Lock()
	record := s.records[id]
	record.Data = cloneBytes(record.Data)
	s.mu.Unlock()

	barrier := s.readBarrier(id)
	if barrier.started != nil {
		barrier.started <- struct{}{}
	}
	if barrier.release != nil {
		<-barrier.release
	}
	return record, nil
}

func (s *fakeStore) Write(_ context.Context, id store.GrainId, data []byte, expect store.ETag) (store.ETag, error) {
	defer s.endOperation(s.beginOperation())
	plan := s.faultPlan(id).write
	if plan.kind == faultDelay {
		s.recordDelay()
		time.Sleep(plan.delay)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.records[id]
	if current.ETag != expect {
		return 0, store.ErrConflict
	}
	if plan.kind == faultWriteError {
		return 0, errWriteFailure
	}

	newETag := current.ETag + 1
	data = cloneBytes(data)
	s.records[id] = store.Record{Data: data, ETag: newETag}
	s.writes = append(s.writes, writeEvent{id: id, data: cloneBytes(data)})
	if plan.kind == faultWriteAppliedError {
		return newETag, errAppliedWriteFailure
	}
	return newETag, nil
}

func (s *fakeStore) WriteMember(_ context.Context, member store.Member) (store.ETag, error) {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	s.memberStats.writeCalls++
	fault := s.memberFault
	key := fakeMemberKey{nodeAddr: member.NodeAddr, generation: member.Generation}
	addressesTarget := fault.kind != memberFaultNone && key == fault.target.row
	delayedActiveWrite := addressesTarget && fault.kind == memberDelay && member.Status == store.MemberActive && member.ETag > 1
	if addressesTarget && fault.kind == memberCASFailure {
		s.memberFault = memberFaultSpec{}
		s.memberStats.casConflicts++
		s.mu.Unlock()
		return 0, store.ErrConflict
	}
	if delayedActiveWrite {
		s.memberFault = memberFaultSpec{}
		s.memberStats.delays++
		s.mu.Unlock()
		if fault.started != nil {
			close(fault.started)
		}
		s.recordDelay()
		time.Sleep(fault.delay)
		s.mu.Lock()
	}

	current := s.members[key]
	if current.ETag != member.ETag {
		if delayedActiveWrite && current.Status == store.MemberDead {
			s.memberStats.delayedDeadCAS++
		}
		s.mu.Unlock()
		return 0, store.ErrConflict
	}
	if addressesTarget && fault.kind == memberCASAppliedError {
		s.memberFault = memberFaultSpec{}
	}

	member.ETag = current.ETag + 1
	s.members[key] = member
	s.memberWrites = append(s.memberWrites, memberWriteEvent{key: key, status: member.Status})
	if member.Status == store.MemberDead {
		s.memberStats.deadWrites++
	}
	if addressesTarget && fault.kind == memberCASAppliedError {
		s.memberStats.casAppliedErrors++
		s.mu.Unlock()
		return member.ETag, errMemberAppliedFailure
	}
	s.mu.Unlock()
	return member.ETag, nil
}

func (s *fakeStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	return s.listMembersFor(ctx, "")
}

func (s *fakeStore) listMembersFor(_ context.Context, caller string) (store.MemberSnapshot, error) {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	s.memberStats.listCalls++
	fault := s.memberFault
	if fault.kind == memberListError && caller == fault.target.addr {
		s.memberFault = memberFaultSpec{}
		s.memberStats.listErrors++
		s.mu.Unlock()
		return store.MemberSnapshot{}, errMemberListFailure
	}
	members := make([]store.Member, 0, len(s.members))
	for _, member := range s.members {
		members = append(members, member)
	}
	s.mu.Unlock()
	sort.Slice(members, func(i, j int) bool {
		if members[i].NodeAddr != members[j].NodeAddr {
			return members[i].NodeAddr < members[j].NodeAddr
		}
		return members[i].Generation < members[j].Generation
	})
	return store.MemberSnapshot{Members: members, TableNow: s.memberTableNow()}, nil
}

func (s *fakeStore) memberStatsSnapshot() memberStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memberStats
}

func (s *fakeStore) checkMemberStatuses() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dead := make(map[fakeMemberKey]bool)
	for _, event := range s.memberWrites {
		if dead[event.key] && event.status != store.MemberDead {
			return errors.New("dead member became active again")
		}
		if event.status == store.MemberDead {
			dead[event.key] = true
		}
	}
	return nil
}

func (s *fakeStore) snapshotDue(now time.Time, caller string) ([]store.Schedule, scheduleFaultSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.listCalls++
	result := make([]store.Schedule, 0)
	for _, schedule := range s.schedules {
		if !schedule.DueAt.After(now) {
			result = append(result, schedule)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].DueAt.Equal(result[j].DueAt) {
			return result[i].DueAt.Before(result[j].DueAt)
		}
		if result[i].GrainId.GrainType != result[j].GrainId.GrainType {
			return result[i].GrainId.GrainType < result[j].GrainId.GrainType
		}
		if result[i].GrainId.GrainKey != result[j].GrainId.GrainKey {
			return result[i].GrainId.GrainKey < result[j].GrainId.GrainKey
		}
		return result[i].Name < result[j].Name
	})
	fault := s.scheduleListFault
	if fault.kind != scheduleFaultNone && caller != nodeAddress(fault.targetNode) {
		return result, scheduleFaultSpec{}
	}
	if fault.kind == scheduleListDelay && len(result) == 0 {
		return result, scheduleFaultSpec{}
	}
	s.scheduleListFault = scheduleFaultSpec{}
	switch fault.kind {
	case scheduleListError:
		s.stats.listErrors++
	case scheduleListDelay:
		s.stats.listDelays++
	}
	return result, fault
}

func (s *fakeStore) ListDue(ctx context.Context, now time.Time) ([]store.Schedule, error) {
	return s.listDueFor(ctx, "", now)
}

func (s *fakeStore) listDueFor(_ context.Context, caller string, now time.Time) ([]store.Schedule, error) {
	defer s.endOperation(s.beginOperation())
	result, fault := s.snapshotDue(now, caller)
	if fault.kind == scheduleListError {
		return nil, errScheduleListFailure
	}
	if fault.kind == scheduleListDelay {
		s.recordDelay()
		time.Sleep(scheduleListDelayDuration)
	}
	return result, nil
}

func (s *fakeStore) Claim(_ context.Context, schedule store.Schedule, nextDueAt time.Time) (bool, error) {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	current, ok := s.schedules[scheduleKey{identity: schedule.GrainId, name: schedule.Name}]
	if !ok || current.ETag != schedule.ETag {
		s.stats.claimLost++
		s.mu.Unlock()
		return false, nil
	}
	fault := s.scheduleClaimFaults[schedule.GrainId]
	delete(s.scheduleClaimFaults, schedule.GrainId)
	if fault == scheduleClaimError {
		s.stats.claimErrors++
		s.mu.Unlock()
		return false, errScheduleClaimFailure
	}
	if nextDueAt.IsZero() {
		delete(s.schedules, scheduleKey{identity: schedule.GrainId, name: schedule.Name})
	} else {
		current.DueAt = nextDueAt
		current.ETag++
		s.schedules[scheduleKey{identity: schedule.GrainId, name: schedule.Name}] = current
	}
	s.stats.claimWon++
	if fault == scheduleClaimAppliedError {
		s.stats.claimAppliedErrors++
	}
	s.mu.Unlock()
	s.timerTracker.claim(schedule.GrainId, fault != scheduleClaimAppliedError)
	if fault == scheduleClaimAppliedError {
		return false, errScheduleClaimAppliedFailure
	}
	return true, nil
}

func (s *fakeStore) Put(_ context.Context, schedule store.Schedule) error {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	key := scheduleKey{identity: schedule.GrainId, name: schedule.Name}
	current, ok := s.schedules[key]
	schedule.ETag = 1
	if ok {
		schedule.ETag = current.ETag + 1
	}
	s.schedules[key] = schedule
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) Delete(_ context.Context, id store.GrainId, name string) error {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	delete(s.schedules, scheduleKey{identity: id, name: name})
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) recordDelay() {
	s.mu.Lock()
	s.delays++
	s.mu.Unlock()
}

func (s *fakeStore) delayCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delays
}

func (s *fakeStore) scheduleStats() scheduleStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *fakeStore) beginOperation() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == 0 {
		s.idle = make(chan struct{})
	}
	s.active++
	return s.idle
}

func (s *fakeStore) endOperation(idle chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active == 0 {
		close(idle)
	}
}

func (s *fakeStore) waitForIdle() {
	s.mu.Lock()
	idle := s.idle
	s.mu.Unlock()
	<-idle
}

func (s *fakeStore) snapshot(ids []store.GrainId) map[store.GrainId]store.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[store.GrainId]store.Record, len(ids))
	for _, id := range ids {
		record := s.records[id]
		record.Data = cloneBytes(record.Data)
		result[id] = record
	}
	return result
}

func (s *fakeStore) committedWritesSince(offset int) ([]writeEvent, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]writeEvent, len(s.writes)-offset)
	for index, event := range s.writes[offset:] {
		events[index] = writeEvent{id: event.id, data: cloneBytes(event.data)}
	}
	return events, len(s.writes)
}

func cloneBytes(data []byte) []byte {
	return append([]byte(nil), data...)
}
