//go:build sim

package sim

import (
	"context"
	"errors"
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

type memberFaultSpec struct {
	kind    memberFaultKind
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
	identity store.Identity
	name     string
}

type writeEvent struct {
	id   store.Identity
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
	records             map[store.Identity]store.Record
	plans               map[store.Identity]faultPlan
	readBarriers        map[store.Identity]readBarrier
	members             map[fakeMemberKey]store.Member
	memberFault         memberFaultSpec
	schedules           map[scheduleKey]store.Schedule
	scheduleListFault   scheduleFaultKind
	scheduleClaimFaults map[store.Identity]scheduleFaultKind
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
		records:             make(map[store.Identity]store.Record),
		plans:               make(map[store.Identity]faultPlan),
		readBarriers:        make(map[store.Identity]readBarrier),
		members:             make(map[fakeMemberKey]store.Member),
		schedules:           make(map[scheduleKey]store.Schedule),
		scheduleClaimFaults: make(map[store.Identity]scheduleFaultKind),
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

func (s *fakeStore) setFaultPlans(plans map[store.Identity]faultPlan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans = make(map[store.Identity]faultPlan, len(plans))
	for id, plan := range plans {
		s.plans[id] = plan
	}
}

func (s *fakeStore) setMemberFault(fault memberFaultSpec) {
	s.mu.Lock()
	s.memberFault = fault
	s.mu.Unlock()
}

func (s *fakeStore) setScheduleFault(id store.Identity, fault scheduleFaultKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleListFault = scheduleFaultNone
	delete(s.scheduleClaimFaults, id)
	switch fault {
	case scheduleListError, scheduleListDelay:
		s.scheduleListFault = fault
	case scheduleClaimError, scheduleClaimAppliedError:
		s.scheduleClaimFaults[id] = fault
	}
}

func (s *fakeStore) faultPlan(id store.Identity) faultPlan {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plans[id]
}

func (s *fakeStore) setReadBarrier(id store.Identity, barrier readBarrier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if barrier.release == nil {
		delete(s.readBarriers, id)
		return
	}
	s.readBarriers[id] = barrier
}

func (s *fakeStore) refreshActiveMembers(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, member := range s.members {
		if member.Status != store.MemberActive {
			continue
		}
		member.IamAliveAt = now
		member.ETag++
		s.members[key] = member
	}
}

func (s *fakeStore) readBarrier(id store.Identity) readBarrier {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readBarriers[id]
}

func (s *fakeStore) Read(_ context.Context, id store.Identity) (store.Record, error) {
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

func (s *fakeStore) Write(_ context.Context, id store.Identity, data []byte, expect store.ETag) (store.ETag, error) {
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
	current := s.members[key]
	delayedActiveWrite := fault.kind == memberDelay && member.Status == store.MemberActive && member.ETag > 1
	if fault.kind == memberCASFailure {
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

	current = s.members[key]
	if current.ETag != member.ETag {
		if delayedActiveWrite && current.Status == store.MemberDead {
			s.memberStats.delayedDeadCAS++
		}
		s.mu.Unlock()
		return 0, store.ErrConflict
	}
	if fault.kind == memberCASAppliedError {
		s.memberFault = memberFaultSpec{}
	}

	member.ETag = current.ETag + 1
	s.members[key] = member
	s.memberWrites = append(s.memberWrites, memberWriteEvent{key: key, status: member.Status})
	if member.Status == store.MemberDead {
		s.memberStats.deadWrites++
	}
	if fault.kind == memberCASAppliedError {
		s.memberStats.casAppliedErrors++
		s.mu.Unlock()
		return member.ETag, errMemberAppliedFailure
	}
	s.mu.Unlock()
	return member.ETag, nil
}

func (s *fakeStore) ListMembers(_ context.Context) (store.MemberSnapshot, error) {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	s.memberStats.listCalls++
	fault := s.memberFault
	if fault.kind == memberListError {
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

func (s *fakeStore) snapshotDue(now time.Time) ([]store.Schedule, scheduleFaultKind) {
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
		if result[i].Identity.Type != result[j].Identity.Type {
			return result[i].Identity.Type < result[j].Identity.Type
		}
		if result[i].Identity.Key != result[j].Identity.Key {
			return result[i].Identity.Key < result[j].Identity.Key
		}
		return result[i].Name < result[j].Name
	})
	fault := s.scheduleListFault
	if fault == scheduleListDelay && len(result) == 0 {
		return result, scheduleFaultNone
	}
	s.scheduleListFault = scheduleFaultNone
	switch fault {
	case scheduleListError:
		s.stats.listErrors++
	case scheduleListDelay:
		s.stats.listDelays++
	}
	return result, fault
}

func (s *fakeStore) ListDue(_ context.Context, now time.Time) ([]store.Schedule, error) {
	defer s.endOperation(s.beginOperation())
	result, fault := s.snapshotDue(now)
	if fault == scheduleListError {
		return nil, errScheduleListFailure
	}
	if fault == scheduleListDelay {
		s.recordDelay()
		time.Sleep(scheduleListDelayDuration)
	}
	return result, nil
}

func (s *fakeStore) Claim(_ context.Context, schedule store.Schedule, nextDueAt time.Time) (bool, error) {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	current, ok := s.schedules[scheduleKey{identity: schedule.Identity, name: schedule.Name}]
	if !ok || current.ETag != schedule.ETag {
		s.stats.claimLost++
		s.mu.Unlock()
		return false, nil
	}
	fault := s.scheduleClaimFaults[schedule.Identity]
	delete(s.scheduleClaimFaults, schedule.Identity)
	if fault == scheduleClaimError {
		s.stats.claimErrors++
		s.mu.Unlock()
		return false, errScheduleClaimFailure
	}
	if nextDueAt.IsZero() {
		delete(s.schedules, scheduleKey{identity: schedule.Identity, name: schedule.Name})
	} else {
		current.DueAt = nextDueAt
		current.ETag++
		s.schedules[scheduleKey{identity: schedule.Identity, name: schedule.Name}] = current
	}
	s.stats.claimWon++
	if fault == scheduleClaimAppliedError {
		s.stats.claimAppliedErrors++
	}
	s.mu.Unlock()
	s.timerTracker.claim(schedule.Identity, fault != scheduleClaimAppliedError)
	if fault == scheduleClaimAppliedError {
		return false, errScheduleClaimAppliedFailure
	}
	return true, nil
}

func (s *fakeStore) Put(_ context.Context, schedule store.Schedule) error {
	defer s.endOperation(s.beginOperation())
	s.mu.Lock()
	key := scheduleKey{identity: schedule.Identity, name: schedule.Name}
	current, ok := s.schedules[key]
	schedule.ETag = 1
	if ok {
		schedule.ETag = current.ETag + 1
	}
	s.schedules[key] = schedule
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) Delete(_ context.Context, id store.Identity, name string) error {
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

func (s *fakeStore) snapshot(ids []store.Identity) map[store.Identity]store.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[store.Identity]store.Record, len(ids))
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
