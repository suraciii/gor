package gor

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
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

// MethodHandle names one method of T for a schedule. Build one with Handle
// from a method expression on T's interface; the type parameter ties the
// handle to the entity interface, so a handle built from another interface
// does not assign and cannot reach this schedule.
type MethodHandle[T any] struct {
	method string
}

// Handle builds a MethodHandle from a method expression on T's interface,
// such as gor.Handle(Account.ApplyInterest). The method name is read off the
// expression once, at this call: reflect and runtime.FuncForPC yield the
// full function name, and its trailing segment is the method name. The
// expression must be a method expression on the interface — a hand-written
// closure of the same function type also compiles, but the name read off it
// is not a method name and delivery fails with "unknown method".
func Handle[T any](m func(T, context.Context) error) MethodHandle[T] {
	full := runtime.FuncForPC(reflect.ValueOf(m).Pointer()).Name()
	name := full[strings.LastIndexByte(full, '.')+1:]
	return MethodHandle[T]{method: strings.TrimSuffix(name, "-fm")}
}

// Schedule manages schedules for the entity bound to a Binder, typed to the
// entity's interface T. Obtain one with NewSchedule[T]; the zero value has no
// schedule store and its operations return ErrScheduleStoreUnavailable.
type Schedule[T any] struct {
	identity store.Identity
	store    store.ScheduleStore
	clock    clock.Clock
}

// NewSchedule returns a schedule manager bound to the entity represented by b
// and typed to its interface T.
func NewSchedule[T any](b *Binder) Schedule[T] {
	return Schedule[T]{
		identity: b.identity,
		store:    b.runtime.scheduleStore,
		clock:    b.runtime.clock,
	}
}

// Set creates or replaces the named schedule for the bound entity. m must be
// a Handle built from a method expression on the entity's interface; the
// method name is read off the handle once, here, and stored. Set does not
// validate that the name has a dispatch case, so a handle built from a
// closure rather than a method expression is stored and fails with "unknown
// method" when the scheduler invokes it. A successful Set persists the
// schedule. Each due occurrence is delivered at most once, and an invocation
// that returns an error is not automatically retried. Setting the same name
// again replaces its method and timing.
// Set returns ErrScheduleStoreUnavailable when no schedule store is
// configured, or the error returned by the store.
func (s Schedule[T]) Set(ctx context.Context, name string, when ScheduleTime, m MethodHandle[T]) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Put(ctx, store.Schedule{
		Identity: s.identity,
		Name:     name,
		Method:   m.method,
		DueAt:    s.clock.Now().Add(when.delay),
		Interval: when.interval,
	})
}

// Cancel asks the schedule store to delete the named schedule for the bound
// entity. Canceling a name that does not exist succeeds as a no-op. It returns
// ErrScheduleStoreUnavailable when no schedule store is configured, or the
// error returned by the store.
func (s Schedule[T]) Cancel(ctx context.Context, name string) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Delete(ctx, s.identity, name)
}
