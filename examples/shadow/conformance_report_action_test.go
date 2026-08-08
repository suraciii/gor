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

func TestConformance_ReportActionConflictLeavesShadowUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sourceClock := clock.NewFake(time.Unix(1900, 0).UTC())
		application := domain.NewMemoryApplicationStore()
		rt := newConformanceRuntime(t, sourceClock, store.NewMemory(), store.NewMemory(), application, nil, nil)
		defer rt.Kill()

		device := gor.Ref[domain.Device](rt, "device-1")
		if err := device.ReportAction(context.Background(), "action-existing", "temperature=20"); err != nil {
			t.Fatalf("seed ReportAction: %v", err)
		}
		before, err := device.Shadow(context.Background())
		if err != nil {
			t.Fatalf("read shadow before conflict: %v", err)
		}
		if err := application.SavePending(context.Background(), domain.PendingAction{
			ActionID:  "action-conflict",
			DeviceKey: "device-1",
			State:     "temperature=21",
			TraceID:   "trace-existing",
		}); err != nil {
			t.Fatalf("seed conflicting action: %v", err)
		}

		err = device.ReportAction(context.Background(), "action-conflict", "temperature=22")
		if !errors.Is(err, domain.ErrPendingActionConflict) {
			t.Fatalf("conflicting ReportAction error = %v, want ErrPendingActionConflict", err)
		}
		after, err := device.Shadow(context.Background())
		if err != nil {
			t.Fatalf("read shadow after conflict: %v", err)
		}
		if after != before {
			t.Fatalf("shadow after deterministic conflict = %#v, want unchanged %#v", after, before)
		}
	})
}
