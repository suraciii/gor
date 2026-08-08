package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

const (
	idleTimeout      = 2 * time.Second
	evictionInterval = time.Second
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) (runErr error) {
	flags := flag.NewFlagSet("shadow-load", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	deviceCount := flags.Int("devices", 10, "number of devices to start")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *deviceCount < 1 {
		return fmt.Errorf("-devices must be positive")
	}

	dataDir, err := os.MkdirTemp("", "gor-shadow-load-")
	if err != nil {
		return fmt.Errorf("create temporary data directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(dataDir); err != nil && runErr == nil {
			runErr = fmt.Errorf("remove temporary data directory: %w", err)
		}
	}()

	database, err := store.OpenSQLite(filepath.Join(dataDir, "shadow.db"))
	if err != nil {
		return fmt.Errorf("open SQLite database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close SQLite database: %w", err)
		}
	}()

	sourceClock := clock.Real{}
	lifecycleEvents := make(chan domain.LifecycleEvent, 2*(*deviceCount)+4)
	rt, err := gor.New(
		gor.WithStore(database),
		gor.WithClock(sourceClock),
		gor.WithIdleTimeout(idleTimeout),
		gor.WithEvictionInterval(evictionInterval),
		gor.WithReminderInterval(time.Second),
		gor.OnError(shadow.LogBackgroundError),
	)
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}
	defer rt.Close()
	if err := shadow.RegisterWithLifecycle(rt, lifecycleEvents); err != nil {
		return fmt.Errorf("register shadow entities: %w", err)
	}

	if err := reportDevices(ctx, rt, *deviceCount); err != nil {
		return err
	}
	device := gor.Ref[domain.Device](rt, "device-000")
	if err := device.Configure(ctx, "sample-rate=10s"); err != nil {
		return fmt.Errorf("configure device-000: %w", err)
	}
	log.Println("initial reports and configuration complete; waiting for idle eviction")
	if err := waitForDeactivations(ctx, lifecycleEvents, *deviceCount+1); err != nil {
		return err
	}

	if activations := rt.Activations(); len(activations) != 0 {
		return fmt.Errorf("active activations after idle eviction = %#v, want none", activations)
	}
	value, err := device.Shadow(ctx)
	if err != nil {
		return fmt.Errorf("read device-000 shadow after eviction: %w", err)
	}
	if value.Configuration != "sample-rate=10s" {
		return fmt.Errorf("device-000 configuration after eviction = %q, want %q", value.Configuration, "sample-rate=10s")
	}
	log.Printf("device-000 was evicted and reloaded from store; configuration: %s", value.Configuration)
	return nil
}

func waitForDeactivations(ctx context.Context, events <-chan domain.LifecycleEvent, expected int) error {
	deactivated := make(map[gor.GrainId]struct{}, expected)
	for len(deactivated) < expected {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for idle eviction: %w", ctx.Err())
		case event := <-events:
			if event.Kind == domain.LifecycleDeactivated {
				deactivated[event.GrainId] = struct{}{}
			}
		}
	}
	return nil
}

func reportDevices(ctx context.Context, rt *gor.Runtime, count int) error {
	failures := make(chan error, count)
	var waitGroup sync.WaitGroup
	for index := 0; index < count; index++ {
		key := fmt.Sprintf("device-%03d", index)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := gor.Ref[domain.Device](rt, key).Report(ctx, "assembly", "load"); err != nil {
				failures <- fmt.Errorf("report %s: %w", key, err)
			}
		}()
	}
	waitGroup.Wait()
	close(failures)
	for failure := range failures {
		return failure
	}
	return nil
}
