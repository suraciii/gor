package store

import (
	"context"
	"sort"
	"time"
)

type Schedule struct {
	Identity Identity
	Name     string
	Method   string
	DueAt    time.Time
	Interval time.Duration
	ETag     ETag
}

type ScheduleStore interface {
	ListDue(context.Context, time.Time) ([]Schedule, error)
	Claim(context.Context, Schedule, time.Time) (bool, error)
	Put(context.Context, Schedule) error
	Delete(context.Context, Identity, string) error
}

type scheduleKey struct {
	identity Identity
	name     string
}

func keyForSchedule(schedule Schedule) scheduleKey {
	return scheduleKey{identity: schedule.Identity, name: schedule.Name}
}

func sortSchedules(schedules []Schedule) {
	sort.Slice(schedules, func(i, j int) bool {
		if schedules[i].DueAt != schedules[j].DueAt {
			return schedules[i].DueAt.Before(schedules[j].DueAt)
		}
		if schedules[i].Identity.Type != schedules[j].Identity.Type {
			return schedules[i].Identity.Type < schedules[j].Identity.Type
		}
		if schedules[i].Identity.Key != schedules[j].Identity.Key {
			return schedules[i].Identity.Key < schedules[j].Identity.Key
		}
		return schedules[i].Name < schedules[j].Name
	})
}

func (m *Memory) ListDue(_ context.Context, now time.Time) ([]Schedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Schedule, 0)
	for _, schedule := range m.schedules {
		if !schedule.DueAt.After(now) {
			result = append(result, schedule)
		}
	}
	sortSchedules(result)
	return result, nil
}

func (m *Memory) Claim(_ context.Context, schedule Schedule, nextDueAt time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keyForSchedule(schedule)
	current, ok := m.schedules[key]
	if !ok || current.ETag != schedule.ETag {
		return false, nil
	}
	if nextDueAt.IsZero() {
		delete(m.schedules, key)
		return true, nil
	}
	current.DueAt = nextDueAt
	current.ETag++
	m.schedules[key] = current
	return true, nil
}

func (m *Memory) Put(_ context.Context, schedule Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keyForSchedule(schedule)
	current, ok := m.schedules[key]
	schedule.ETag = 1
	if ok {
		schedule.ETag = current.ETag + 1
	}
	m.schedules[key] = schedule
	return nil
}

func (m *Memory) Delete(_ context.Context, id Identity, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.schedules, scheduleKey{identity: id, name: name})
	return nil
}
