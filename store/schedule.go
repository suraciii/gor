package store

import (
	"context"
	"sort"
	"time"
)

// Reminder describes one persisted Reminder deadline.
//
// GrainId and Name identify the row. DueAt is inclusive when queried by
// ListDue. FirstTickTime is the first due time for the current setting.
// Interval is retained for the poller, and ETag is the version used by Claim.
type Reminder struct {
	GrainId       GrainId
	Name          string
	Method        string
	FirstTickTime time.Time
	DueAt         time.Time
	Interval      time.Duration
	ETag          ETag
}

// ReminderStore persists Reminders and atomically claims due rows.
//
// Implementations must support concurrent calls. Claim must compare the row's
// identity, name, and ETag atomically so concurrent claimers using one
// snapshot produce at most one winner. Put and Delete are unconditional
// changes by design.
type ReminderStore interface {
	// ListDue returns every Reminder whose DueAt is no later than now.
	ListDue(context.Context, time.Time) ([]Reminder, error)
	// Claim compares the Reminder identity, name, and ETag atomically. When
	// they match, a non-zero nextDueAt replaces DueAt and increments the stored
	// ETag; a zero nextDueAt deletes the Reminder. It returns true only when the
	// update or deletion succeeds, and returns false with a nil error when the
	// row is absent or its ETag is stale.
	Claim(context.Context, Reminder, time.Time) (bool, error)
	// Put inserts or replaces a Reminder without an ETag precondition. A new
	// row receives ETag 1; replacing an existing row increments its current
	// ETag. The input ETag is ignored.
	Put(context.Context, Reminder) error
	// Delete unconditionally removes the named Reminder, if it exists.
	Delete(context.Context, GrainId, string) error
}

type reminderKey struct {
	identity GrainId
	name     string
}

func keyForReminder(reminder Reminder) reminderKey {
	return reminderKey{identity: reminder.GrainId, name: reminder.Name}
}

func sortReminders(reminders []Reminder) {
	sort.Slice(reminders, func(i, j int) bool {
		if reminders[i].DueAt != reminders[j].DueAt {
			return reminders[i].DueAt.Before(reminders[j].DueAt)
		}
		if reminders[i].GrainId.GrainType != reminders[j].GrainId.GrainType {
			return reminders[i].GrainId.GrainType < reminders[j].GrainId.GrainType
		}
		if reminders[i].GrainId.GrainKey != reminders[j].GrainId.GrainKey {
			return reminders[i].GrainId.GrainKey < reminders[j].GrainId.GrainKey
		}
		return reminders[i].Name < reminders[j].Name
	})
}

// ListDue returns due Reminders in deterministic DueAt, identity, and name
// order.
func (m *Memory) ListDue(ctx context.Context, now time.Time) ([]Reminder, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Reminder, 0)
	for _, reminder := range m.reminders {
		if !reminder.DueAt.After(now) {
			result = append(result, reminder)
		}
	}
	sortReminders(result)
	return result, nil
}

// Claim atomically checks the Reminder identity, name, and ETag. It returns
// true and advances the row when nextDueAt is non-zero, or deletes the row
// when nextDueAt is zero. It returns false and nil when the row is absent or
// stale.
func (m *Memory) Claim(ctx context.Context, reminder Reminder, nextDueAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keyForReminder(reminder)
	current, ok := m.reminders[key]
	if !ok || current.ETag != reminder.ETag {
		return false, nil
	}
	if nextDueAt.IsZero() {
		delete(m.reminders, key)
		return true, nil
	}
	current.DueAt = nextDueAt
	current.ETag++
	m.reminders[key] = current
	return true, nil
}

// Put unconditionally inserts or replaces a Reminder and assigns a new ETag.
func (m *Memory) Put(ctx context.Context, reminder Reminder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keyForReminder(reminder)
	current, ok := m.reminders[key]
	reminder.ETag = 1
	if ok {
		reminder.ETag = current.ETag + 1
	}
	m.reminders[key] = reminder
	return nil
}

// Delete unconditionally removes the Reminder identified by id and name.
func (m *Memory) Delete(ctx context.Context, id GrainId, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.reminders, reminderKey{identity: id, name: name})
	return nil
}
