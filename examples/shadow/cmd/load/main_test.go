package main

import (
	"context"
	"testing"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/store"
)

func TestReportDevicesReturnsAfterSuccess(t *testing.T) {
	rt, err := gor.New(
		gor.WithStore(store.NewMemory()),
		gor.WithClock(clock.NewFake(time.Unix(0, 0).UTC())),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithScheduleInterval(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.Register(rt); err != nil {
		rt.Close()
		t.Fatal(err)
	}
	defer rt.Close()

	if err := reportDevices(context.Background(), rt, 1); err != nil {
		t.Fatalf("reportDevices returned error: %v", err)
	}
}
