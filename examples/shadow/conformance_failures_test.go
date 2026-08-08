package shadow_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadowdomain "github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

func TestConformance_ReminderClaimHasOneWinner(t *testing.T) {
	start := time.Unix(1300, 0).UTC()
	reminderStore := store.NewMemory()
	row := store.Reminder{
		GrainId:       store.GrainId{GrainType: gor.TypeName[shadowdomain.RecoveryCoordinator](), GrainKey: shadowdomain.RecoveryCoordinatorKey},
		Name:          shadowdomain.RecoveryReminderName,
		Method:        "Recover",
		FirstTickTime: start,
		DueAt:         start,
		Interval:      shadowdomain.RecoveryInterval,
	}
	if err := reminderStore.Put(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	due, err := reminderStore.ListDue(context.Background(), start)
	if err != nil || len(due) != 1 {
		t.Fatalf("ListDue = (%#v, %v), want one row", due, err)
	}
	startClaims := make(chan struct{})
	results := make(chan claimResult, 2)
	for range 2 {
		go func() {
			<-startClaims
			won, err := reminderStore.Claim(context.Background(), due[0], start.Add(shadowdomain.RecoveryInterval))
			results <- claimResult{won: won, err: err}
		}()
	}
	close(startClaims)
	wins := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.won {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("Reminder claim winners = %d, want one", wins)
	}
}

func TestConformance_StopAfterClaimLeavesPendingForNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1400, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		baseReminders := store.NewMemory()
		blocker := &blockingReminderStore{
			ReminderStore: baseReminders,
			claimed:       make(chan struct{}),
			release:       make(chan struct{}),
		}
		application := shadowdomain.NewMemoryApplicationStore()
		rt := newConformanceRuntime(t, sourceClock, stateStore, blocker, application, nil, nil)
		if err := gor.Ref[shadowdomain.RecoveryCoordinator](rt, shadowdomain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := gor.Ref[shadowdomain.Device](rt, "device-1").ReportAction(context.Background(), "action-claim", "temperature=22"); err != nil {
			t.Fatalf("ReportAction: %v", err)
		}
		sourceClock.Advance(shadowdomain.RecoveryInterval)
		synctest.Wait()
		<-blocker.claimed

		killDone := make(chan struct{})
		go func() {
			rt.Kill()
			close(killDone)
		}()
		<-rt.Done()
		close(blocker.release)
		<-killDone
		if _, applied, err := application.ReadApplied(context.Background(), "action-claim"); err != nil || applied {
			t.Fatalf("applied action after stop between claim and delivery = (%v, %v), want (nil, false)", err, applied)
		}

		rt = newConformanceRuntime(t, sourceClock, stateStore, baseReminders, application, nil, nil)
		defer rt.Kill()
		if err := gor.Ref[shadowdomain.RecoveryCoordinator](rt, shadowdomain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("restart Start: %v", err)
		}
		sourceClock.Advance(shadowdomain.RecoveryInterval)
		synctest.Wait()
		if _, applied, err := application.ReadApplied(context.Background(), "action-claim"); err != nil || !applied {
			t.Fatalf("applied action after next periodic tick = (%v, %v), want (nil, true)", err, applied)
		}
	})
}

func TestConformance_SafeRepeatAndUnknownResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1500, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		baseApplication := shadowdomain.NewMemoryApplicationStore()
		unknownErr := errors.New("application result is unknown")
		application := &faultApplicationStore{ApplicationStore: baseApplication, afterCommit: unknownErr}
		errorsSeen := make(chan gor.BackgroundError, 4)
		calls := make(chan gor.CallObservation, 16)
		rt := newConformanceRuntimeWithErrors(t, sourceClock, stateStore, reminderStore, application, nil, calls, errorsSeen)
		defer rt.Kill()
		if err := gor.Ref[shadowdomain.RecoveryCoordinator](rt, shadowdomain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := gor.Ref[shadowdomain.Device](rt, "device-1").ReportAction(context.Background(), "action-unknown", "temperature=23"); err != nil {
			t.Fatalf("ReportAction: %v", err)
		}
		sourceClock.Advance(shadowdomain.RecoveryInterval)
		synctest.Wait()
		background := <-errorsSeen
		if !errors.Is(background.Err, unknownErr) {
			t.Fatalf("OnError = %#v, want unknown result", background)
		}
		source, ok := background.Source.(gor.ReminderInvocation)
		if !ok || source.Method != "Recover" {
			t.Fatalf("OnError source = %#v, want ReminderInvocation{Recover}", background.Source)
		}
		if !hasCall(calls, "Recover", unknownErr) {
			t.Fatalf("OnCall observations = %v, want Recover error", drainCalls(calls))
		}
		record, applied, err := baseApplication.ReadApplied(context.Background(), "action-unknown")
		if err != nil || !applied {
			t.Fatalf("ReadApplied after unknown result = (%#v, %v, %v), want one receipt", record, applied, err)
		}
		if pending, err := baseApplication.ListPending(context.Background()); err != nil || len(pending) != 0 {
			t.Fatalf("pending after unknown result = (%#v, %v), want none", pending, err)
		}
		if err := gor.Ref[shadowdomain.Device](rt, "device-1").ApplyPending(context.Background(), "action-unknown"); err != nil {
			t.Fatalf("Safe Repeat ApplyPending: %v", err)
		}
		repeated, applied, err := baseApplication.ReadApplied(context.Background(), "action-unknown")
		if err != nil || !applied || repeated != record {
			t.Fatalf("receipt after Safe Repeat = (%#v, %v, %v), want unchanged receipt", repeated, applied, err)
		}
	})
}

func TestConformance_ReminderFailureRetriesPendingAndReportsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1600, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		baseApplication := shadowdomain.NewMemoryApplicationStore()
		beforeErr := errors.New("application commit failed before commit")
		application := &faultApplicationStore{ApplicationStore: baseApplication, before: beforeErr}
		errorsSeen := make(chan gor.BackgroundError, 4)
		rt := newConformanceRuntimeWithErrors(t, sourceClock, stateStore, reminderStore, application, nil, nil, errorsSeen)
		defer rt.Kill()
		if err := gor.Ref[shadowdomain.RecoveryCoordinator](rt, shadowdomain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := gor.Ref[shadowdomain.Device](rt, "device-1").ReportAction(context.Background(), "action-before", "temperature=24"); err != nil {
			t.Fatalf("ReportAction: %v", err)
		}
		sourceClock.Advance(shadowdomain.RecoveryInterval)
		synctest.Wait()
		background := <-errorsSeen
		if !errors.Is(background.Err, beforeErr) {
			t.Fatalf("OnError = %#v, want pre-commit error", background)
		}
		if pending, err := baseApplication.ListPending(context.Background()); err != nil || len(pending) != 1 {
			t.Fatalf("pending after failed Reminder = (%#v, %v), want one action", pending, err)
		}
		sourceClock.Advance(shadowdomain.RecoveryInterval)
		synctest.Wait()
		if _, applied, err := baseApplication.ReadApplied(context.Background(), "action-before"); err != nil || !applied {
			t.Fatalf("applied after later Reminder = (%v, %v), want true", err, applied)
		}
	})
}

func TestConformance_CancelRecoveryReminderClearsScheduleAndState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1700, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		application := shadowdomain.NewMemoryApplicationStore()
		observed := make(chan shadowdomain.RecoveryObservation, 4)
		rt := newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, observed, nil)
		defer rt.Kill()
		coordinator := gor.Ref[shadowdomain.RecoveryCoordinator](rt, shadowdomain.RecoveryCoordinatorKey)
		if err := coordinator.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		rows, err := reminderStore.ListDue(context.Background(), start.Add(2*shadowdomain.RecoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
		if hasReminder(rows, gor.TypeName[shadowdomain.RecoveryCoordinator](), shadowdomain.RecoveryCoordinatorKey, shadowdomain.RecoveryReminderName) {
			t.Fatalf("recovery Reminder remains after Stop: %#v", rows)
		}
		record, err := stateStore.Read(context.Background(), store.GrainId{GrainType: gor.TypeName[shadowdomain.RecoveryCoordinator](), GrainKey: shadowdomain.RecoveryCoordinatorKey})
		if err != nil {
			t.Fatal(err)
		}
		if string(record.Data) != `{}` {
			t.Fatalf("coordinator State after Stop = %s, want cleared record", record.Data)
		}
		sourceClock.Advance(2 * shadowdomain.RecoveryInterval)
		synctest.Wait()
		select {
		case observation := <-observed:
			t.Fatalf("recovery Call after cancellation: %#v", observation)
		default:
		}
	})
}
