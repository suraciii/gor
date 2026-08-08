package domain

import (
	"context"
	"errors"

	"github.com/suraciii/gor"
)

type recoveryCoordinator struct {
	binder      *gor.Binder
	status      gor.State[CoordinatorState]
	reminder    gor.Reminder[RecoveryCoordinator]
	application ApplicationStore
	observed    chan<- RecoveryObservation
}

// NewRecoveryCoordinator creates the fixed-key recovery Grain.
func NewRecoveryCoordinator(b *gor.Binder, application ApplicationStore) RecoveryCoordinator {
	return newRecoveryCoordinator(b, application, nil)
}

// NewRecoveryCoordinatorWithObservation creates the recovery Grain and sends
// each Recover context and tick to observed. The channel is for example tests.
func NewRecoveryCoordinatorWithObservation(b *gor.Binder, application ApplicationStore, observed chan<- RecoveryObservation) RecoveryCoordinator {
	return newRecoveryCoordinator(b, application, observed)
}

func newRecoveryCoordinator(b *gor.Binder, application ApplicationStore, observed chan<- RecoveryObservation) RecoveryCoordinator {
	return &recoveryCoordinator{
		binder:      b,
		status:      gor.NewState[CoordinatorState](b, "running"),
		reminder:    gor.NewReminder[RecoveryCoordinator](b),
		application: application,
		observed:    observed,
	}
}

func (c *recoveryCoordinator) Start(ctx context.Context) error {
	if c.status.Exists() && c.status.Get().Running {
		return nil
	}
	if err := c.reminder.Set(ctx, RecoveryReminderName, gor.Every(RecoveryInterval), gor.Handle(RecoveryCoordinator.Recover)); err != nil {
		return err
	}
	return c.status.Set(ctx, CoordinatorState{Running: true})
}

func (c *recoveryCoordinator) Stop(ctx context.Context) error {
	if err := c.reminder.Cancel(ctx, RecoveryReminderName); err != nil {
		return err
	}
	return c.status.Clear(ctx)
}

func (c *recoveryCoordinator) Recover(ctx context.Context, tick gor.TickStatus) error {
	if c.observed != nil {
		traceID, present := gor.RequestContextValue(ctx, "trace_id")
		c.observed <- RecoveryObservation{Tick: tick, TraceID: traceID, TracePresent: present}
	}
	if c.application == nil {
		return errors.New("application store is not configured")
	}
	actions, err := c.application.ListPending(ctx)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if err := gor.Ref[Device](c.binder, action.DeviceKey).ApplyPending(ctx, action.ActionID); err != nil {
			return err
		}
	}
	return nil
}
