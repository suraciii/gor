package shadow_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

func TestDeviceShadowTracksReportsAndWorkshopPresence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		start := time.Unix(0, 0).UTC()
		sourceClock := clock.NewFake(start)
		backend := store.NewMemory()
		rt, err := gor.New(
			gor.WithStore(backend),
			gor.WithClock(sourceClock),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
			gor.WithScheduleInterval(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := shadow.Register(rt); err != nil {
			rt.Close()
			t.Fatal(err)
		}
		defer rt.Close()

		device := gor.Ref[domain.Device](rt, "device-1")
		workshop := gor.Ref[domain.Workshop](rt, "assembly")
		if err := device.Report(ctx, "assembly", "temperature=20"); err != nil {
			t.Fatal(err)
		}
		if got, err := workshop.OnlineCount(ctx); err != nil || got != 1 {
			t.Fatalf("online count after report = (%d, %v), want (1, nil)", got, err)
		}
		if got, err := device.Shadow(ctx); err != nil || got.ReportedState != "temperature=20" || !got.ReportedAt.Equal(start) || !got.Online || got.WorkshopID != "assembly" {
			t.Fatalf("shadow after report = (%#v, %v), want reported state, start time, online, and assembly", got, err)
		}
		packaging := gor.Ref[domain.Workshop](rt, "packaging")
		if err := device.Report(ctx, "packaging", "temperature=20"); err != nil {
			t.Fatal(err)
		}
		if got, err := workshop.OnlineCount(ctx); err != nil || got != 0 {
			t.Fatalf("assembly count after move = (%d, %v), want (0, nil)", got, err)
		}
		if got, err := packaging.OnlineCount(ctx); err != nil || got != 1 {
			t.Fatalf("packaging count after move = (%d, %v), want (1, nil)", got, err)
		}

		if err := device.Configure(ctx, "sample-rate=10s"); err != nil {
			t.Fatal(err)
		}
		if got, err := device.Shadow(ctx); err != nil || got.Configuration != "sample-rate=10s" {
			t.Fatalf("shadow after configure = (%#v, %v), want configuration", got, err)
		}

		sourceClock.Advance(29 * time.Second)
		synctest.Wait()
		if err := device.Report(ctx, "assembly", "temperature=21"); err != nil {
			t.Fatal(err)
		}
		schedules, err := backend.ListDue(ctx, start.Add(59*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if len(schedules) != 1 || !schedules[0].DueAt.Equal(start.Add(59*time.Second)) {
			t.Fatalf("offline schedule after second report = %#v, want one schedule due at 59s", schedules)
		}

		sourceClock.Advance(2 * time.Second)
		synctest.Wait()
		if got, err := device.Shadow(ctx); err != nil || !got.Online {
			t.Fatalf("shadow before reset deadline = (%#v, %v), want online", got, err)
		}

		sourceClock.Advance(28 * time.Second)
		synctest.Wait()
		if got, err := device.Shadow(ctx); err != nil || got.Online {
			t.Fatalf("shadow after reset deadline = (%#v, %v), want offline", got, err)
		}
		if got, err := workshop.OnlineCount(ctx); err != nil || got != 0 {
			t.Fatalf("online count after timeout = (%d, %v), want (0, nil)", got, err)
		}
		schedules, err = backend.ListDue(ctx, sourceClock.Now().Add(domain.OfflineAfter))
		if err != nil {
			t.Fatal(err)
		}
		if len(schedules) != 0 {
			t.Fatalf("schedules after timeout = %#v, want none", schedules)
		}

		if err := device.Report(ctx, "assembly", "temperature=22"); err != nil {
			t.Fatal(err)
		}
		if got, err := workshop.OnlineCount(ctx); err != nil || got != 1 {
			t.Fatalf("online count after re-report = (%d, %v), want (1, nil)", got, err)
		}
	})
}
