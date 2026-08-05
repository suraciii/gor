package gor

import (
	"context"
	"errors"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

// ErrScheduleStoreUnavailable is returned by Schedule.Set and Schedule.Cancel
// when no schedule store is configured. It is a sentinel suitable for
// errors.Is.
var ErrScheduleStoreUnavailable = errors.New("schedule store is not configured")

// ScheduleTime describes when a schedule first runs and whether it repeats.
// Use After for a one-shot schedule or Every for a recurring schedule.
type ScheduleTime struct {
	delay    time.Duration
	interval time.Duration
}

// After returns a one-shot schedule time due delay after it is set.
func After(delay time.Duration) ScheduleTime {
	return ScheduleTime{delay: delay}
}

// Every returns a recurring schedule time whose first and subsequent runs are
// separated by interval.
func Every(interval time.Duration) ScheduleTime {
	return ScheduleTime{delay: interval, interval: interval}
}

// Schedule manages schedules for the entity bound to a Binder. Obtain one
// with NewSchedule; the zero value has no schedule store and its operations
// return ErrScheduleStoreUnavailable.
type Schedule struct {
	identity store.Identity
	store    store.ScheduleStore
	clock    clock.Clock
}

// NewSchedule returns a schedule manager bound to the entity represented by
// b.
func NewSchedule(b *Binder) Schedule {
	return Schedule{
		identity: b.identity,
		store:    b.runtime.scheduleStore,
		clock:    b.runtime.clock,
	}
}

// Set creates or replaces the named schedule for the bound entity. The
// schedule invokes method after the time described by when; setting the same
// name again replaces its method and timing. It returns
// ErrScheduleStoreUnavailable when no schedule store is configured, or the
// error returned by the store.
func (s Schedule) Set(ctx context.Context, name string, when ScheduleTime, method string) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Put(ctx, store.Schedule{
		Identity: s.identity,
		Name:     name,
		Method:   method,
		DueAt:    s.clock.Now().Add(when.delay),
		Interval: when.interval,
	})
}

// Cancel asks the schedule store to delete the named schedule for the bound
// entity. It returns ErrScheduleStoreUnavailable when no schedule store is
// configured, or the error returned by the store.
func (s Schedule) Cancel(ctx context.Context, name string) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Delete(ctx, s.identity, name)
}
