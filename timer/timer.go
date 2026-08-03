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
	Invoke(context.Context, store.Identity, string) error
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
	go poller.run()
	return poller
}

func (p *Poller) Close() {
	p.cancel()
	<-p.done
}

func (p *Poller) run() {
	ticker := p.clock.NewTicker(p.interval)
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
		nextDueAt := nextDueAt(schedule, now)
		claimed, err := p.table.Claim(p.ctx, schedule, nextDueAt)
		if err != nil || !claimed {
			continue
		}
		_ = p.invoker.Invoke(p.ctx, schedule.Identity, schedule.Method)
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
