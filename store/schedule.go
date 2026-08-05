package store

import (
	"context"
	"sort"
	"time"
)

// Schedule describes one persisted invocation deadline.
//
// Identity and Name identify the row. DueAt is inclusive when queried by
// ListDue. Interval is retained for the scheduler, and ETag is the version
// used by Claim.
type Schedule struct {
	Identity Identity
	Name     string
	Method   string
	DueAt    time.Time
	Interval time.Duration
	ETag     ETag
}

// ScheduleStore persists schedules and atomically claims due rows.
//
// Implementations must support concurrent calls. Claim must compare the row's
// identity, name, and ETag atomically so concurrent claimers using one
// snapshot produce at most one winner. Put and Delete are unconditional
// changes by design.
type ScheduleStore interface {
	// ListDue returns every schedule whose DueAt is no later than now.
	ListDue(context.Context, time.Time) ([]Schedule, error)
	// Claim compares the schedule's identity, name, and ETag atomically. When
	// they match, a non-zero nextDueAt replaces DueAt and increments the stored
	// ETag; a zero nextDueAt deletes the schedule. It returns true only when the
	// update or deletion succeeds, and returns false with a nil error when the
	// row is absent or its ETag is stale.
	Claim(context.Context, Schedule, time.Time) (bool, error)
	// Put inserts or replaces a schedule without an ETag precondition. A new
	// row receives ETag 1; replacing an existing row increments its current
	// ETag. The input ETag is ignored.
	Put(context.Context, Schedule) error
	// Delete unconditionally removes the named schedule, if it exists.
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

// ListDue returns due schedules in deterministic DueAt, identity, and name
// order.
func (m *Memory) ListDue(ctx context.Context, now time.Time) ([]Schedule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// Claim atomically checks schedule's identity, name, and ETag. It returns true
// and advances the row when nextDueAt is non-zero, or deletes the row when
// nextDueAt is zero. It returns false and nil when the row is absent or stale.
func (m *Memory) Claim(ctx context.Context, schedule Schedule, nextDueAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
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

// Put unconditionally inserts or replaces a schedule and assigns a new ETag.
func (m *Memory) Put(ctx context.Context, schedule Schedule) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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

// Delete unconditionally removes the schedule identified by id and name.
func (m *Memory) Delete(ctx context.Context, id Identity, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.schedules, scheduleKey{identity: id, name: name})
	return nil
}
