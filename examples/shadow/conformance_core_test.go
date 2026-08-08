package shadow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

func TestConformance_TypedGrainsStatePresenceAndClear(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1000, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		application := domain.NewMemoryApplicationStore()
		rt := newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, nil, nil)
		ctx := context.Background()

		device := gor.Ref[domain.Device](rt, "device-1")
		workshop := gor.Ref[domain.Workshop](rt, "assembly")
		if err := device.Report(ctx, "assembly", "temperature=20"); err != nil {
			t.Fatalf("Report: %v", err)
		}
		if count, err := workshop.OnlineCount(ctx); err != nil || count != 1 {
			t.Fatalf("typed cross-Grain OnlineCount = (%d, %v), want (1, nil)", count, err)
		}
		if exists, err := device.ShadowExists(ctx); err != nil || !exists {
			t.Fatalf("ShadowExists after Report = (%v, %v), want (true, nil)", exists, err)
		}

		if err := device.ClearShadow(ctx); err != nil {
			t.Fatalf("ClearShadow: %v", err)
		}
		if exists, err := device.ShadowExists(ctx); err != nil || exists {
			t.Fatalf("ShadowExists after ClearShadow = (%v, %v), want (false, nil)", exists, err)
		}
		if err := device.Configure(ctx, ""); err != nil {
			t.Fatalf("Configure empty shadow: %v", err)
		}
		zero, err := device.Shadow(ctx)
		if err != nil {
			t.Fatalf("Shadow present zero: %v", err)
		}
		if zero != (domain.Shadow{}) {
			t.Fatalf("present zero Shadow = %#v, want zero value", zero)
		}
		if exists, err := device.ShadowExists(ctx); err != nil || !exists {
			t.Fatalf("ShadowExists for present zero = (%v, %v), want (true, nil)", exists, err)
		}
		if err := device.ClearShadow(ctx); err != nil {
			t.Fatalf("second ClearShadow: %v", err)
		}

		rt.Kill()
		rt = newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, nil, nil)
		defer rt.Kill()
		if exists, err := gor.Ref[domain.Device](rt, "device-1").ShadowExists(ctx); err != nil || exists {
			t.Fatalf("ShadowExists after reactivation = (%v, %v), want (false, nil)", exists, err)
		}
	})
}

func TestConformance_RequestContextIsCopiedAndNotPersisted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1100, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		application := domain.NewMemoryApplicationStore()
		observed := make(chan domain.RecoveryObservation, 4)
		rt := newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, observed, nil)
		defer rt.Kill()
		ctx, err := gor.WithRequestContext(context.Background(), "trace_id", "trace-1")
		if err != nil {
			t.Fatal(err)
		}

		coordinator := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey)
		if err := coordinator.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := gor.Ref[domain.Device](rt, "device-1").ReportAction(ctx, "action-1", "temperature=20"); err != nil {
			t.Fatalf("ReportAction with Request Context: %v", err)
		}
		pending, err := application.ListPending(context.Background())
		if err != nil {
			t.Fatalf("ListPending: %v", err)
		}
		if len(pending) != 1 || pending[0].TraceID != "trace-1" {
			t.Fatalf("pending actions = %#v, want one copied trace ID", pending)
		}

		stateRecord, err := stateStore.Read(context.Background(), store.GrainId{GrainType: gor.TypeName[domain.Device](), GrainKey: "device-1"})
		if err != nil {
			t.Fatalf("read Device State: %v", err)
		}
		if bytes.Contains(stateRecord.Data, []byte("trace_id")) || bytes.Contains(stateRecord.Data, []byte("trace-1")) {
			t.Fatalf("Device State contains Request Context: %s", stateRecord.Data)
		}
		rows, err := reminderStore.ListDue(context.Background(), start.Add(2*time.Second))
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		if !hasReminder(rows, gor.TypeName[domain.RecoveryCoordinator](), domain.RecoveryCoordinatorKey, domain.RecoveryReminderName) {
			t.Fatalf("Reminders = %#v, want fixed-key recovery Reminder", rows)
		}
		for _, row := range rows {
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte("request_context")) || bytes.Contains(encoded, []byte("trace-1")) {
				t.Fatalf("Reminder record contains Request Context: %s", encoded)
			}
		}

		sourceClock.Advance(domain.RecoveryInterval)
		synctest.Wait()
		observation := <-observed
		if observation.TracePresent || observation.TraceID != nil {
			t.Fatalf("Reminder Request Context = %#v, want absent", observation)
		}
		if !observation.Tick.FirstTickTime.Equal(start.Add(domain.RecoveryInterval)) || observation.Tick.Period != domain.RecoveryInterval {
			t.Fatalf("Reminder TickStatus = %#v, want fixed first tick and period", observation.Tick)
		}
	})
}

func TestConformance_RestartRecoversPendingAction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1200, 0).UTC()
		sourceClock := clock.NewFake(start)
		stateStore := store.NewMemory()
		reminderStore := store.NewMemory()
		application := domain.NewMemoryApplicationStore()
		observed := make(chan domain.RecoveryObservation, 4)
		rt := newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, observed, nil)
		coordinator := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey)
		if err := coordinator.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		ctx, err := gor.WithRequestContext(context.Background(), "trace_id", "trace-restart")
		if err != nil {
			t.Fatal(err)
		}
		if err := gor.Ref[domain.Device](rt, "device-1").ReportAction(ctx, "action-restart", "temperature=21"); err != nil {
			t.Fatalf("ReportAction: %v", err)
		}
		rt.Kill()

		calls := make(chan gor.CallObservation, 16)
		rt = newConformanceRuntime(t, sourceClock, stateStore, reminderStore, application, observed, calls)
		defer rt.Kill()
		if err := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey).Start(context.Background()); err != nil {
			t.Fatalf("restart Start: %v", err)
		}
		sourceClock.Advance(domain.RecoveryInterval)
		synctest.Wait()
		observation := <-observed
		if observation.TracePresent {
			t.Fatal("recovery Reminder inherited Request Context")
		}
		record, applied, err := application.ReadApplied(context.Background(), "action-restart")
		if err != nil {
			t.Fatalf("ReadApplied: %v", err)
		}
		if !applied || record.TraceID != "trace-restart" || record.State != "temperature=21" {
			t.Fatalf("applied record = (%#v, %v), want copied action receipt", record, applied)
		}
		pending, err := application.ListPending(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 0 {
			t.Fatalf("pending actions after recovery = %#v, want none", pending)
		}
		observations := drainCalls(calls)
		if !containsCall(observations, "Recover", nil) || !containsCall(observations, "ApplyPending", nil) {
			t.Fatalf("OnCall observations = %v, want Recover and typed ApplyPending", observations)
		}
	})
}
