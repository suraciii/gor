package domain

import (
	"context"
	"log"
	"time"

	"github.com/suraciii/gor"
)

const OfflineAfter = 30 * time.Second

// ErrWorkshopIDRequired reports that a device report has no workshop ID.
const ErrWorkshopIDRequired gor.Code = "shadow.workshop_id_required"

const (
	LifecycleActivated   = "activated"
	LifecycleDeactivated = "deactivated"
)

type LifecycleEvent struct {
	Identity gor.Identity
	Kind     string
}

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
	binder          *gor.Binder
	id              gor.Identity
	shadow          gor.State[Shadow]
	schedule        gor.Schedule[Device]
	lifecycleEvents chan<- LifecycleEvent
}

func NewDevice(b *gor.Binder) Device {
	return newDevice(b, nil)
}

func NewDeviceWithLifecycle(b *gor.Binder, events chan<- LifecycleEvent) Device {
	return newDevice(b, events)
}

func newDevice(b *gor.Binder, events chan<- LifecycleEvent) Device {
	return &device{
		binder:          b,
		id:              gor.Self(b),
		shadow:          gor.NewState[Shadow](b, "shadow"),
		schedule:        gor.NewSchedule[Device](b),
		lifecycleEvents: events,
	}
}

func (d *device) Report(ctx context.Context, workshopID string, state string) error {
	if workshopID == "" {
		return ErrWorkshopIDRequired
	}
	previous := d.shadow.Get()
	next := previous
	next.ReportedState = state
	next.ReportedAt = gor.Now(d.binder)
	next.Online = true
	next.WorkshopID = workshopID
	if err := d.shadow.Set(ctx, next); err != nil {
		return err
	}
	if err := d.schedule.Set(ctx, "offline", gor.After(OfflineAfter), gor.Handle(Device.MarkOffline)); err != nil {
		return err
	}

	if previous.Online && previous.WorkshopID != workshopID {
		if err := gor.Ref[Workshop](d.binder, previous.WorkshopID).DeviceOffline(ctx, d.id.Key); err != nil {
			return err
		}
	}
	if !previous.Online || previous.WorkshopID != workshopID {
		if err := gor.Ref[Workshop](d.binder, workshopID).DeviceOnline(ctx, d.id.Key); err != nil {
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

func (d *device) OnActivate(context.Context) error {
	log.Printf("%s activated", d.id.Key)
	d.emitLifecycle(LifecycleActivated)
	return nil
}

func (d *device) OnDeactivate(_ context.Context, reason gor.DeactivationReason) error {
	log.Printf("%s deactivated: %v", d.id.Key, reason)
	d.emitLifecycle(LifecycleDeactivated)
	return nil
}

func (d *device) emitLifecycle(kind string) {
	if d.lifecycleEvents != nil {
		d.lifecycleEvents <- LifecycleEvent{Identity: d.id, Kind: kind}
	}
}

func (d *device) MarkOffline(ctx context.Context) error {
	shadow := d.shadow.Get()
	shadow.Online = false
	if err := d.shadow.Set(ctx, shadow); err != nil {
		return err
	}
	return gor.Ref[Workshop](d.binder, shadow.WorkshopID).DeviceOffline(ctx, d.id.Key)
}

type workshop struct {
	id              gor.Identity
	online          gor.State[map[string]struct{}]
	lifecycleEvents chan<- LifecycleEvent
}

func NewWorkshop(b *gor.Binder) Workshop {
	return newWorkshop(b, nil)
}

func NewWorkshopWithLifecycle(b *gor.Binder, events chan<- LifecycleEvent) Workshop {
	return newWorkshop(b, events)
}

func newWorkshop(b *gor.Binder, events chan<- LifecycleEvent) Workshop {
	return &workshop{
		id:              gor.Self(b),
		online:          gor.NewState[map[string]struct{}](b, "online"),
		lifecycleEvents: events,
	}
}

func (w *workshop) OnActivate(context.Context) error {
	w.emitLifecycle(LifecycleActivated)
	return nil
}

func (w *workshop) OnDeactivate(_ context.Context, _ gor.DeactivationReason) error {
	w.emitLifecycle(LifecycleDeactivated)
	return nil
}

func (w *workshop) emitLifecycle(kind string) {
	if w.lifecycleEvents != nil {
		w.lifecycleEvents <- LifecycleEvent{Identity: w.id, Kind: kind}
	}
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
