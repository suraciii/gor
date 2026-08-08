// Package timer polls persisted Reminders and delivers due Grain Calls for
// gor.
//
// It is an implementation package, not an application dependency. Create and
// manage Reminders through the root gor package's Reminder APIs instead of
// importing timer directly.
package timer

import (
	"context"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type Table interface {
	ListDue(context.Context, time.Time) ([]store.Reminder, error)
	Claim(context.Context, store.Reminder, time.Time) (bool, error)
}

type Invoker interface {
	Owns(store.GrainId) bool
	Invoke(context.Context, store.GrainId, string, any, any) error
}

// ReminderCallFactory creates the normal typed request and reply values for a
// claimed Reminder. The root package converts these time values into its
// public TickStatus before calling generated code.
type ReminderCallFactory func(store.GrainId, string, time.Time, time.Duration, time.Time) (any, any)

type Poller struct {
	table    Table
	clock    clock.Clock
	interval time.Duration
	invoker  Invoker
	newCall  ReminderCallFactory

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func New(table Table, clock clock.Clock, interval time.Duration, invoker Invoker, newCall ReminderCallFactory) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &Poller{
		table:    table,
		clock:    clock,
		interval: interval,
		invoker:  invoker,
		newCall:  newCall,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go poller.run(poller.clock.NewTicker(poller.interval))
	return poller
}

func (p *Poller) Close() {
	p.cancel()
	<-p.done
}

func (p *Poller) run(ticker clock.Ticker) {
	defer func() {
		ticker.Stop()
		close(p.done)
	}()

	for {
		select {
		case <-ticker.C():
			p.poll()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Poller) poll() {
	now := p.clock.Now()
	reminders, err := p.table.ListDue(p.ctx, now)
	if err != nil {
		return
	}
	for _, reminder := range reminders {
		if p.ctx.Err() != nil {
			return
		}
		if !p.invoker.Owns(reminder.GrainId) {
			continue
		}
		nextDueAt := nextDueAt(reminder, now)
		claimed, err := p.table.Claim(p.ctx, reminder, nextDueAt)
		if err != nil || !claimed {
			continue
		}
		if p.newCall == nil {
			continue
		}
		args, reply := p.newCall(reminder.GrainId, reminder.Method, reminder.FirstTickTime, reminder.Interval, reminder.DueAt)
		_ = p.invoker.Invoke(p.ctx, reminder.GrainId, reminder.Method, args, reply)
	}
}

const maxDuration = time.Duration(1<<63 - 1)

func nextDueAt(reminder store.Reminder, now time.Time) time.Time {
	period := reminder.Interval
	if period <= 0 {
		return time.Time{}
	}
	if reminder.DueAt.After(now) {
		return reminder.DueAt
	}

	elapsed := now.Sub(reminder.DueAt)
	missed := elapsed / period
	if missed == maxDuration {
		return futureDueAt(now, period)
	}
	missed++
	if missed > maxDuration/period {
		return futureDueAt(now, period)
	}

	candidate := reminder.DueAt.Add(period * missed)
	if !candidate.After(now) {
		return futureDueAt(now, period)
	}
	return candidate
}

func futureDueAt(now time.Time, period time.Duration) time.Time {
	fallback := now.Add(period)
	if fallback.After(now) {
		return fallback
	}
	return now.Add(time.Nanosecond)
}
