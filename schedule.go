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

// ErrReminderStoreUnavailable is returned by Reminder.Set and Reminder.Cancel
// when no reminder store is configured. It is a sentinel suitable for
// errors.Is.
var ErrReminderStoreUnavailable = errors.New("reminder store is not configured")

// ReminderTime describes when a Reminder first runs and whether it repeats.
// Its zero value is valid and is equivalent to After(0): a one-shot Reminder
// due at the clock time used by Set.
type ReminderTime struct {
	delay    time.Duration
	interval time.Duration
}

// After returns a one-shot Reminder due delay after Set uses its clock. A zero
// delay is due immediately; a negative delay is due in the past and is
// eligible on the next poller poll.
func After(delay time.Duration) ReminderTime {
	return ReminderTime{delay: delay}
}

// Every returns a Reminder whose first due time is interval after Set uses its
// clock and whose subsequent due times use the same interval when interval is
// positive. Every(0) is accepted and produces a one-shot Reminder, just like
// After(0). A negative interval is accepted and stored.
func Every(interval time.Duration) ReminderTime {
	return ReminderTime{delay: interval, interval: interval}
}

// TickStatus describes the time represented by one Reminder delivery.
type TickStatus struct {
	FirstTickTime   time.Time
	Period          time.Duration
	CurrentTickTime time.Time
}

// MethodHandle names one method of T for a Reminder. Build one with Handle
// from a method expression on T's interface; the type parameter ties the
// handle to the Grain interface.
type MethodHandle[T any] struct {
	method string
}

// Handle builds a MethodHandle from a method expression on T's interface,
// such as gor.Handle(Account.ApplyInterest). The method name is read off the
// expression once, at this call. A closure of the same function type compiles,
// but its name is not a method name and delivery fails with "unknown method".
func Handle[T any](m func(T, context.Context, TickStatus) error) MethodHandle[T] {
	full := runtime.FuncForPC(reflect.ValueOf(m).Pointer()).Name()
	name := full[strings.LastIndexByte(full, '.')+1:]
	return MethodHandle[T]{method: name}
}

// Reminder manages Reminders for the Grain bound to a Binder, typed to the
// Grain's interface T. Obtain one with NewReminder[T]; the zero value has no
// reminder store and its operations return ErrReminderStoreUnavailable.
type Reminder[T any] struct {
	identity store.GrainId
	store    store.ReminderStore
	clock    clock.Clock
}

// NewReminder returns a Reminder manager bound to the Grain represented by b
// and typed to its interface T.
func NewReminder[T any](b *Binder) Reminder[T] {
	return Reminder[T]{
		identity: b.identity,
		store:    b.runtime.reminderStore,
		clock:    b.runtime.clock,
	}
}

// Set creates or replaces the named Reminder for the bound Grain. The new
// first due time is used as FirstTickTime and DueAt. A successful Set persists
// the Reminder. Each due occurrence is delivered at most once, and an
// invocation that returns an error is not automatically retried.
func (s Reminder[T]) Set(ctx context.Context, name string, when ReminderTime, m MethodHandle[T]) error {
	if s.store == nil {
		return ErrReminderStoreUnavailable
	}
	firstTickTime := s.clock.Now().Add(when.delay)
	return s.store.Put(ctx, store.Reminder{
		GrainId:       s.identity,
		Name:          name,
		Method:        m.method,
		FirstTickTime: firstTickTime,
		DueAt:         firstTickTime,
		Interval:      when.interval,
	})
}

// Cancel asks the reminder store to delete the named Reminder for the bound
// Grain. Canceling a name that does not exist succeeds as a no-op.
func (s Reminder[T]) Cancel(ctx context.Context, name string) error {
	if s.store == nil {
		return ErrReminderStoreUnavailable
	}
	return s.store.Delete(ctx, s.identity, name)
}
