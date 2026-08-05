package shadow_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
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

func TestDeviceLifecycleLogsActivationAndDeactivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := log.Default()
		previousWriter := logger.Writer()
		var output bytes.Buffer
		logger.SetOutput(&output)

		sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
		rt, err := gor.New(
			gor.WithStore(store.NewMemory()),
			gor.WithClock(sourceClock),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
			gor.WithScheduleInterval(0),
		)
		if err != nil {
			logger.SetOutput(previousWriter)
			t.Fatal(err)
		}
		if err := shadow.Register(rt); err != nil {
			rt.Close()
			logger.SetOutput(previousWriter)
			t.Fatal(err)
		}
		defer func() {
			rt.Close()
			logger.SetOutput(previousWriter)
		}()

		device := gor.Ref[domain.Device](rt, "device-1")
		if _, err := device.Shadow(context.Background()); err != nil {
			t.Fatal(err)
		}
		rt.Deactivate(gor.Identity{Type: gor.TypeName[domain.Device](), Key: "device-1"})
		synctest.Wait()
		if _, err := device.Shadow(context.Background()); err != nil {
			t.Fatal(err)
		}

		for _, event := range []string{"device-1 activated", "device-1 deactivated"} {
			if !strings.Contains(output.String(), event) {
				t.Fatalf("lifecycle log = %q, want %q", output.String(), event)
			}
		}
	})
}

func TestScheduledFailureReachesOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &failingWorkshopStore{Memory: store.NewMemory()}
		sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
		errorsSeen := make(chan backgroundError, 1)
		rt, err := gor.New(
			gor.WithStore(backend),
			gor.WithScheduleStore(backend),
			gor.WithClock(sourceClock),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
			gor.WithScheduleInterval(time.Second),
			gor.OnError(func(id gor.Identity, method string, err error) {
				errorsSeen <- backgroundError{id: id, method: method, err: err}
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := shadow.Register(rt); err != nil {
			rt.Close()
			t.Fatal(err)
		}
		defer rt.Close()

		if err := gor.Ref[domain.Device](rt, "device-1").Report(context.Background(), "assembly", "temperature=20"); err != nil {
			t.Fatal(err)
		}
		backend.failWorkshopWrites.Store(true)
		sourceClock.Advance(domain.OfflineAfter)
		synctest.Wait()

		select {
		case got := <-errorsSeen:
			wantID := gor.Identity{Type: gor.TypeName[domain.Device](), Key: "device-1"}
			if got.id != wantID || got.method != "MarkOffline" || !errors.Is(got.err, errWorkshopWrite) {
				t.Fatalf("OnError event = %#v, want %v.MarkOffline with %v", got, wantID, errWorkshopWrite)
			}
		default:
			t.Fatal("scheduled failure did not reach OnError")
		}
	})
}

type backgroundError struct {
	id     gor.Identity
	method string
	err    error
}

var errWorkshopWrite = errors.New("workshop write failed")

type failingWorkshopStore struct {
	*store.Memory
	failWorkshopWrites atomic.Bool
}

func (s *failingWorkshopStore) Write(ctx context.Context, id store.Identity, data []byte, expect store.ETag) (store.ETag, error) {
	if s.failWorkshopWrites.Load() && id.Type == gor.TypeName[domain.Workshop]() {
		return 0, errWorkshopWrite
	}
	return s.Memory.Write(ctx, id, data, expect)
}
