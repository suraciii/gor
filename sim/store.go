//go:build sim

package sim

import (
	"context"
	"errors"
	"sync"
	"time"

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
	read  faultSpec
	write faultSpec
}

var (
	errReadFailure         = errors.New("sim store read failure")
	errWriteFailure        = errors.New("sim store write failure")
	errAppliedWriteFailure = errors.New("sim store write applied before failure")
)

type writeEvent struct {
	id   store.Identity
	data []byte
}

type fakeStore struct {
	mu      sync.Mutex
	records map[store.Identity]store.Record
	plans   map[store.Identity]faultPlan
	writes  []writeEvent
	delays  int
	active  int
	idle    chan struct{}
}

var _ store.Store = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	idle := make(chan struct{})
	close(idle)
	return &fakeStore{
		records: make(map[store.Identity]store.Record),
		plans:   make(map[store.Identity]faultPlan),
		idle:    idle,
	}
}

func (s *fakeStore) setFaultPlans(plans map[store.Identity]faultPlan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans = make(map[store.Identity]faultPlan, len(plans))
	for id, plan := range plans {
		s.plans[id] = plan
	}
}

func (s *fakeStore) faultPlan(id store.Identity) faultPlan {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.plans[id]
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
	defer s.mu.Unlock()
	record := s.records[id]
	record.Data = cloneBytes(record.Data)
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
