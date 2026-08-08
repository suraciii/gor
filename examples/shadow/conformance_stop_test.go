package shadow_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

func TestConformance_StopInterruptionRestartsCoordinator(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1800, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		baseReminders := store.NewMemory()
		blockingReminders := &blockingDeleteReminderStore{
			ReminderStore: baseReminders,
			deleted:       make(chan struct{}),
			release:       make(chan struct{}),
		}
		application := domain.NewMemoryApplicationStore()
		observed := make(chan domain.RecoveryObservation, 4)
		rt := newConformanceRuntime(t, sourceClock, stateStore, blockingReminders, application, observed, nil)
		coordinator := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey)
		if err := coordinator.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		before, err := baseReminders.ListDue(context.Background(), start.Add(2*domain.RecoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
		beforeReminder := findRecoveryReminder(t, before)
		if err := coordinator.Start(context.Background()); err != nil {
			t.Fatalf("idempotent Start: %v", err)
		}
		after, err := baseReminders.ListDue(context.Background(), start.Add(2*domain.RecoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
		afterReminder := findRecoveryReminder(t, after)
		if !afterReminder.FirstTickTime.Equal(beforeReminder.FirstTickTime) {
			t.Fatalf("idempotent Start changed FirstTickTime from %s to %s", beforeReminder.FirstTickTime, afterReminder.FirstTickTime)
		}
		if err := gor.Ref[domain.Device](rt, "device-1").ReportAction(context.Background(), "action-stop", "temperature=25"); err != nil {
			t.Fatalf("ReportAction: %v", err)
		}

		stopDone := make(chan error, 1)
		go func() {
			stopDone <- coordinator.Stop(context.Background())
		}()
		<-blockingReminders.deleted

		coordinatorRecord, err := stateStore.Read(context.Background(), store.GrainId{
			GrainType: gor.TypeName[domain.RecoveryCoordinator](),
			GrainKey:  domain.RecoveryCoordinatorKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(coordinatorRecord.Data) != `{}` {
			t.Fatalf("Coordinator State while Cancel is blocked = %s, want cleared state", coordinatorRecord.Data)
		}
		remaining, err := baseReminders.ListDue(context.Background(), start.Add(2*domain.RecoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
		if hasReminder(remaining, gor.TypeName[domain.RecoveryCoordinator](), domain.RecoveryCoordinatorKey, domain.RecoveryReminderName) {
			t.Fatalf("recovery Reminder remains after Delete completed: %#v", remaining)
		}

		killDone := make(chan struct{})
		go func() {
			rt.Kill()
			close(killDone)
		}()
		<-rt.Done()
		close(blockingReminders.release)
		<-killDone
		if err := <-stopDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop after interrupted process: %v", err)
		}

		rt = newConformanceRuntime(t, sourceClock, stateStore, baseReminders, application, observed, nil)
		defer rt.Kill()
		if err := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("restart Start: %v", err)
		}
		rows, err := baseReminders.ListDue(context.Background(), start.Add(2*domain.RecoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
		findRecoveryReminder(t, rows)
		sourceClock.Advance(domain.RecoveryInterval)
		synctest.Wait()
		if _, applied, err := application.ReadApplied(context.Background(), "action-stop"); err != nil || !applied {
			t.Fatalf("action after restart recovery = (%v, %v), want (nil, true)", err, applied)
		}
	})
}

type blockingDeleteReminderStore struct {
	store.ReminderStore
	deleted chan struct{}
	release chan struct{}
}

func (s *blockingDeleteReminderStore) Delete(ctx context.Context, id store.GrainId, name string) error {
	if err := s.ReminderStore.Delete(ctx, id, name); err != nil {
		return err
	}
	close(s.deleted)
	<-s.release
	return nil
}

func findRecoveryReminder(t *testing.T, rows []store.Reminder) store.Reminder {
	t.Helper()
	for _, row := range rows {
		if row.GrainId.GrainType == gor.TypeName[domain.RecoveryCoordinator]() && row.GrainId.GrainKey == domain.RecoveryCoordinatorKey && row.Name == domain.RecoveryReminderName {
			return row
		}
	}
	t.Fatalf("recovery Reminder missing from %#v", rows)
	return store.Reminder{}
}

var _ store.ReminderStore = (*blockingDeleteReminderStore)(nil)
