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
// Its zero value is valid and is equivalent to After(0): a one-shot schedule
// due at the clock time used by Set.
type ScheduleTime struct {
	delay    time.Duration
	interval time.Duration
}

// After returns a one-shot schedule due delay after Set uses its clock. A zero
// delay is due immediately; a negative delay is due in the past and is
// eligible on the next scheduler poll.
func After(delay time.Duration) ScheduleTime {
	return ScheduleTime{delay: delay}
}

// Every returns a schedule whose first due time is interval after Set uses its
// clock and whose subsequent due times use the same interval when interval is
// positive. Every(0) is accepted and produces a one-shot schedule, just like
// After(0). A negative interval is also accepted and stored; it places the
// schedule in the past so it remains due instead of producing a future
// recurring deadline.
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

// Set creates or replaces the named schedule for the bound entity. method must
// name a generated entity method callable as func(context.Context) error; Set
// does not validate the name or signature, so an invalid method is stored and
// fails when the scheduler invokes it. A successful Set persists the schedule.
// Each due occurrence is delivered at most once, and an invocation that returns
// an error is not automatically retried. Setting the same name again replaces
// its method and timing.
// Set returns ErrScheduleStoreUnavailable when no schedule store is configured,
// or the error returned by the store.
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
// entity. Canceling a name that does not exist succeeds as a no-op. It returns
// ErrScheduleStoreUnavailable when no schedule store is configured, or the
// error returned by the store.
func (s Schedule) Cancel(ctx context.Context, name string) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Delete(ctx, s.identity, name)
}
