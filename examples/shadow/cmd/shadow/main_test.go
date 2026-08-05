package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

func TestNewRuntimeReportsScheduledFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := store.NewMemory()
		start := time.Unix(0, 0).UTC()
		sourceClock := clock.NewFake(start)
		var output bytes.Buffer
		logger := log.Default()
		previousWriter := logger.Writer()
		logger.SetOutput(&output)

		rt, err := newRuntime(
			backend,
			gor.WithClock(sourceClock),
			gor.WithIdleTimeout(0),
			gor.WithEvictionInterval(0),
			gor.WithScheduleInterval(time.Second),
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

		if err := backend.Put(context.Background(), store.Schedule{
			Identity: store.Identity{Type: gor.TypeName[domain.Device](), Key: "device-1"},
			Name:     "broken",
			Method:   "NotAMethod",
			DueAt:    start,
		}); err != nil {
			t.Fatal(err)
		}
		sourceClock.Advance(time.Second)
		synctest.Wait()

		if !strings.Contains(output.String(), ".NotAMethod failed:") {
			t.Fatalf("runtime error log = %q, want scheduled failure from production OnError", output.String())
		}
	})
}
