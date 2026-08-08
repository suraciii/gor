// Package timer polls persisted schedules and delivers due entity calls for
// gor.
//
// It is an implementation package, not an application dependency. Create and
// manage schedules through the root gor package's Schedule APIs instead of
// importing timer directly.
package timer

import (
	"context"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

type Table interface {
	ListDue(context.Context, time.Time) ([]store.Schedule, error)
	Claim(context.Context, store.Schedule, time.Time) (bool, error)
}

type Invoker interface {
	Owns(store.GrainId) bool
	Invoke(context.Context, store.GrainId, string) error
}

type Poller struct {
	table    Table
	clock    clock.Clock
	interval time.Duration
	invoker  Invoker

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func New(table Table, clock clock.Clock, interval time.Duration, invoker Invoker) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	poller := &Poller{
		table:    table,
		clock:    clock,
		interval: interval,
		invoker:  invoker,
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
	schedules, err := p.table.ListDue(p.ctx, now)
	if err != nil {
		return
	}
	for _, schedule := range schedules {
		if p.ctx.Err() != nil {
			return
		}
		if !p.invoker.Owns(schedule.GrainId) {
			continue
		}
		nextDueAt := nextDueAt(schedule, now)
		claimed, err := p.table.Claim(p.ctx, schedule, nextDueAt)
		if err != nil || !claimed {
			continue
		}
		_ = p.invoker.Invoke(p.ctx, schedule.GrainId, schedule.Method)
	}
}

func nextDueAt(schedule store.Schedule, now time.Time) time.Time {
	if schedule.Interval == 0 {
		return time.Time{}
	}
	elapsed := now.Sub(schedule.DueAt)
	steps := elapsed/schedule.Interval + 1
	return schedule.DueAt.Add(steps * schedule.Interval)
}
