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
	rt := gor.New(
		gor.WithStore(database),
		gor.WithClock(sourceClock),
		gor.WithIdleTimeout(idleTimeout),
		gor.WithEvictionInterval(evictionInterval),
		gor.WithScheduleInterval(time.Second),
	)
	defer rt.Close()
	if err := shadow.Register(rt, sourceClock); err != nil {
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
	time.Sleep(idleTimeout + 2*evictionInterval)

	value, err := device.Shadow(ctx)
	if err != nil {
		return fmt.Errorf("read device-000 shadow after eviction: %w", err)
	}
	if value.Configuration != "sample-rate=10s" {
		return fmt.Errorf("device-000 configuration after eviction = %q, want %q", value.Configuration, "sample-rate=10s")
	}
	log.Printf("device-000 configuration survived idle eviction: %s", value.Configuration)
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
	return <-failures
}
