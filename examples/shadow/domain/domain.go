package domain

import (
	"context"
	"errors"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
)

const OfflineAfter = 30 * time.Second

var ErrWorkshopIDRequired = errors.New("workshop id is required")

type Shadow struct {
	ReportedState string
	ReportedAt    time.Time
	Online        bool
	WorkshopID    string
	Configuration string
}

//gor:entity
type Device interface {
	Report(ctx context.Context, workshopID string, state string) error
	Configure(ctx context.Context, configuration string) error
	Shadow(ctx context.Context) (Shadow, error)
	MarkOffline(ctx context.Context) error
}

//gor:entity
type Workshop interface {
	DeviceOnline(ctx context.Context, deviceID string) error
	DeviceOffline(ctx context.Context, deviceID string) error
	OnlineCount(ctx context.Context) (int, error)
}

type device struct {
	runtime  *gor.Runtime
	id       gor.Identity
	clock    clock.Clock
	shadow   gor.State[Shadow]
	schedule gor.Schedule
}

func NewDevice(runtime *gor.Runtime, b *gor.Binder, sourceClock clock.Clock) Device {
	return &device{
		runtime:  runtime,
		id:       gor.Self(b),
		clock:    sourceClock,
		shadow:   gor.NewState[Shadow](b, "shadow"),
		schedule: gor.NewSchedule(b),
	}
}

func (d *device) Report(ctx context.Context, workshopID string, state string) error {
	if workshopID == "" {
		return ErrWorkshopIDRequired
	}
	previous := d.shadow.Get()
	next := previous
	next.ReportedState = state
	next.ReportedAt = d.clock.Now()
	next.Online = true
	next.WorkshopID = workshopID
	if err := d.shadow.Set(ctx, next); err != nil {
		return err
	}
	if err := d.schedule.Set(ctx, "offline", gor.After(OfflineAfter), "MarkOffline"); err != nil {
		return err
	}

	if previous.Online && previous.WorkshopID != workshopID {
		if err := gor.Ref[Workshop](d.runtime, previous.WorkshopID).DeviceOffline(ctx, d.id.Key); err != nil {
			return err
		}
	}
	if !previous.Online || previous.WorkshopID != workshopID {
		if err := gor.Ref[Workshop](d.runtime, workshopID).DeviceOnline(ctx, d.id.Key); err != nil {
			return err
		}
	}
	return nil
}

func (d *device) Configure(ctx context.Context, configuration string) error {
	shadow := d.shadow.Get()
	shadow.Configuration = configuration
	return d.shadow.Set(ctx, shadow)
}

func (d *device) Shadow(context.Context) (Shadow, error) {
	return d.shadow.Get(), nil
}

func (d *device) MarkOffline(ctx context.Context) error {
	shadow := d.shadow.Get()
	shadow.Online = false
	if err := d.shadow.Set(ctx, shadow); err != nil {
		return err
	}
	return gor.Ref[Workshop](d.runtime, shadow.WorkshopID).DeviceOffline(ctx, d.id.Key)
}

type workshop struct {
	online gor.State[map[string]struct{}]
}

func NewWorkshop(b *gor.Binder) Workshop {
	return &workshop{online: gor.NewState[map[string]struct{}](b, "online")}
}

func (w *workshop) DeviceOnline(ctx context.Context, deviceID string) error {
	online := w.online.Get()
	if online == nil {
		online = make(map[string]struct{})
	}
	online[deviceID] = struct{}{}
	return w.online.Set(ctx, online)
}

func (w *workshop) DeviceOffline(ctx context.Context, deviceID string) error {
	online := w.online.Get()
	delete(online, deviceID)
	return w.online.Set(ctx, online)
}

func (w *workshop) OnlineCount(context.Context) (int, error) {
	return len(w.online.Get()), nil
}
