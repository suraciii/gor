package domain

import (
	"context"
	"errors"
	"fmt"
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
	GrainId gor.GrainId
	Kind    string
}

type Shadow struct {
	ReportedState string
	ReportedAt    time.Time
	Online        bool
	WorkshopID    string
	Configuration string
}

//gor:grain
type Device interface {
	Report(ctx context.Context, workshopID string, state string) error
	ReportAction(ctx context.Context, actionID string, state string) error
	Configure(ctx context.Context, configuration string) error
	Shadow(ctx context.Context) (Shadow, error)
	ShadowExists(ctx context.Context) (bool, error)
	ClearShadow(ctx context.Context) error
	ApplyPending(ctx context.Context, actionID string) error
	MarkOffline(ctx context.Context, tick gor.TickStatus) error
}

//gor:grain
type RecoveryCoordinator interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Recover(ctx context.Context, tick gor.TickStatus) error
}

const (
	RecoveryCoordinatorKey = "recovery"
	RecoveryReminderName   = "recovery"
	RecoveryInterval       = time.Second
)

type CoordinatorState struct {
	Running bool
}

type RecoveryObservation struct {
	Tick         gor.TickStatus
	TraceID      any
	TracePresent bool
}

//gor:grain
type Workshop interface {
	DeviceOnline(ctx context.Context, deviceID string) error
	DeviceOffline(ctx context.Context, deviceID string) error
	OnlineCount(ctx context.Context) (int, error)
}

type device struct {
	binder          *gor.Binder
	id              gor.GrainId
	shadow          gor.State[Shadow]
	schedule        gor.Reminder[Device]
	application     ApplicationStore
	lifecycleEvents chan<- LifecycleEvent
}

func NewDevice(b *gor.Binder) Device {
	return newDevice(b, nil, nil)
}

func NewDeviceWithLifecycle(b *gor.Binder, events chan<- LifecycleEvent) Device {
	return newDevice(b, events, nil)
}

func NewDeviceWithApplication(b *gor.Binder, application ApplicationStore) Device {
	return newDevice(b, nil, application)
}

func NewDeviceWithApplicationAndLifecycle(b *gor.Binder, application ApplicationStore, events chan<- LifecycleEvent) Device {
	return newDevice(b, events, application)
}

func newDevice(b *gor.Binder, events chan<- LifecycleEvent, application ApplicationStore) Device {
	return &device{
		binder:          b,
		id:              gor.Self(b),
		shadow:          gor.NewState[Shadow](b, "shadow"),
		schedule:        gor.NewReminder[Device](b),
		application:     application,
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
		if err := gor.Ref[Workshop](d.binder, previous.WorkshopID).DeviceOffline(ctx, d.id.GrainKey); err != nil {
			return err
		}
	}
	if !previous.Online || previous.WorkshopID != workshopID {
		if err := gor.Ref[Workshop](d.binder, workshopID).DeviceOnline(ctx, d.id.GrainKey); err != nil {
			return err
		}
	}
	return nil
}

func (d *device) ReportAction(ctx context.Context, actionID string, state string) error {
	if d.application == nil {
		return errors.New("application store is not configured")
	}
	if actionID == "" {
		return errors.New("application action ID is empty")
	}
	shadow := d.shadow.Get()
	shadow.ReportedState = state
	shadow.ReportedAt = gor.Now(d.binder)
	shadow.Online = true
	if err := d.shadow.Set(ctx, shadow); err != nil {
		return err
	}
	traceID, err := traceIDFromContext(ctx)
	if err != nil {
		return err
	}
	if err := d.application.SavePending(ctx, PendingAction{
		ActionID:  actionID,
		DeviceKey: d.id.GrainKey,
		State:     state,
		TraceID:   traceID,
	}); err != nil {
		return err
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

func (d *device) ShadowExists(context.Context) (bool, error) {
	return d.shadow.Exists(), nil
}

func (d *device) ClearShadow(ctx context.Context) error {
	return d.shadow.Clear(ctx)
}

func (d *device) ApplyPending(ctx context.Context, actionID string) error {
	if d.application == nil {
		return errors.New("application store is not configured")
	}
	return d.application.ApplyPending(ctx, actionID)
}

func (d *device) OnActivate(context.Context) error {
	log.Printf("%s activated", d.id.GrainKey)
	d.emitLifecycle(LifecycleActivated)
	return nil
}

func (d *device) OnDeactivate(_ context.Context, reason gor.DeactivationReason) error {
	log.Printf("%s deactivated: %v", d.id.GrainKey, reason)
	d.emitLifecycle(LifecycleDeactivated)
	return nil
}

func (d *device) emitLifecycle(kind string) {
	if d.lifecycleEvents != nil {
		d.lifecycleEvents <- LifecycleEvent{GrainId: d.id, Kind: kind}
	}
}

func traceIDFromContext(ctx context.Context) (string, error) {
	value, ok := gor.RequestContextValue(ctx, "trace_id")
	if !ok {
		return "", nil
	}
	traceID, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("trace_id has type %T, want string", value)
	}
	return traceID, nil
}

func (d *device) MarkOffline(ctx context.Context, _ gor.TickStatus) error {
	shadow := d.shadow.Get()
	shadow.Online = false
	if err := d.shadow.Set(ctx, shadow); err != nil {
		return err
	}
	return gor.Ref[Workshop](d.binder, shadow.WorkshopID).DeviceOffline(ctx, d.id.GrainKey)
}

type workshop struct {
	id              gor.GrainId
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
		w.lifecycleEvents <- LifecycleEvent{GrainId: w.id, Kind: kind}
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
