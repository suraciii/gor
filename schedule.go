package gor

import (
	"context"
	"errors"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

var ErrScheduleStoreUnavailable = errors.New("schedule store is not configured")

type ScheduleTime struct {
	delay    time.Duration
	interval time.Duration
}

func After(delay time.Duration) ScheduleTime {
	return ScheduleTime{delay: delay}
}

func Every(interval time.Duration) ScheduleTime {
	return ScheduleTime{delay: interval, interval: interval}
}

type Schedule struct {
	identity store.Identity
	store    store.ScheduleStore
	clock    clock.Clock
}

func NewSchedule(b *Binder) Schedule {
	return Schedule{
		identity: b.identity,
		store:    b.runtime.scheduleStore,
		clock:    b.runtime.clock,
	}
}

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

func (s Schedule) Cancel(ctx context.Context, name string) error {
	if s.store == nil {
		return ErrScheduleStoreUnavailable
	}
	return s.store.Delete(ctx, s.identity, name)
}
