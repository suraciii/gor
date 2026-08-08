package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/suraciii/gor"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

const (
	phasePrepare = "prepare"
	phaseRecover = "recover"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) (runErr error) {
	flags := flag.NewFlagSet("conformance", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	phase := flags.String("phase", "", "process phase: prepare or recover")
	runtimePath := flags.String("db", "runtime.db", "Runtime SQLite database path")
	businessPath := flags.String("business-db", "business.db", "Application SQLite database path")
	deviceKey := flags.String("device", "device-1", "target Device GrainKey")
	actionID := flags.String("action-id", "action-1", "pending Business ActionID")
	state := flags.String("state", "temperature=20", "reported Device State")
	traceID := flags.String("trace-id", "trace-1", "Request Context trace_id for prepare")
	waitTimeout := flags.Duration("timeout", 10*time.Second, "maximum wait for the recovery Call")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *phase != phasePrepare && *phase != phaseRecover {
		return fmt.Errorf("-phase must be %q or %q", phasePrepare, phaseRecover)
	}
	if *waitTimeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if err := validateDatabasePaths(*runtimePath, *businessPath); err != nil {
		return err
	}
	if err := makeParent(*runtimePath); err != nil {
		return fmt.Errorf("create Runtime database directory: %w", err)
	}
	if err := makeParent(*businessPath); err != nil {
		return fmt.Errorf("create Application database directory: %w", err)
	}

	runtimeStore, err := store.OpenSQLite(*runtimePath)
	if err != nil {
		return fmt.Errorf("open Runtime database: %w", err)
	}
	application, err := domain.OpenSQLiteApplicationStore(*businessPath)
	if err != nil {
		runtimeStore.Close()
		return fmt.Errorf("open Application database: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close Application database: %w", err))
		}
		if err := runtimeStore.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close Runtime database: %w", err))
		}
	}()

	calls := make(chan gor.CallObservation, 32)
	options := []gor.Option{
		gor.WithStore(runtimeStore),
		gor.WithReminderStore(runtimeStore),
		gor.WithReminderInterval(0),
		gor.OnError(shadow.LogBackgroundError),
		gor.OnCall(func(observation gor.CallObservation) { calls <- observation }),
	}
	if *phase == phaseRecover {
		options = append(options, gor.WithReminderInterval(domain.RecoveryInterval))
	}
	rt, err := gor.New(options...)
	if err != nil {
		return fmt.Errorf("create Single Silo Runtime: %w", err)
	}
	defer rt.Close()
	if err := shadow.RegisterConformance(rt, application); err != nil {
		return fmt.Errorf("register conformance Grains: %w", err)
	}

	coordinator := gor.Ref[domain.RecoveryCoordinator](rt, domain.RecoveryCoordinatorKey)
	switch *phase {
	case phasePrepare:
		if err := coordinator.Start(ctx); err != nil {
			return fmt.Errorf("start recovery coordinator: %w", err)
		}
		requestContext, err := gor.WithRequestContext(ctx, "trace_id", *traceID)
		if err != nil {
			return fmt.Errorf("add Request Context: %w", err)
		}
		if err := gor.Ref[domain.Device](rt, *deviceKey).ReportAction(requestContext, *actionID, *state); err != nil {
			return fmt.Errorf("save pending action: %w", err)
		}
		log.Printf("prepared ActionID %q for Device %q; stop the process before Reminder delivery", *actionID, *deviceKey)
		return nil
	case phaseRecover:
		if err := coordinator.Start(ctx); err != nil {
			return fmt.Errorf("start recovery coordinator: %w", err)
		}
		if err := waitForRecovery(ctx, calls, *waitTimeout); err != nil {
			return err
		}
		record, applied, err := application.ReadApplied(ctx, *actionID)
		if err != nil {
			return fmt.Errorf("read applied record: %w", err)
		}
		if !applied {
			return fmt.Errorf("ActionID %q has no applied record", *actionID)
		}
		pending, err := application.ListPending(ctx)
		if err != nil {
			return fmt.Errorf("list pending actions: %w", err)
		}
		if len(pending) != 0 {
			return fmt.Errorf("pending actions remain after recovery: %#v", pending)
		}
		log.Printf("recovered ActionID %q for Device %q with receipt %#v", record.ActionID, record.DeviceKey, record)
		return nil
	}
	return nil
}

func waitForRecovery(ctx context.Context, calls <-chan gor.CallObservation, timeout time.Duration) error {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case observation := <-calls:
			if observation.Method != "Recover" {
				continue
			}
			if observation.Err != nil {
				return fmt.Errorf("recovery Call failed: %w", observation.Err)
			}
			return nil
		case <-waitContext.Done():
			return fmt.Errorf("wait for recovery Call: %w", waitContext.Err())
		}
	}
}

func validateDatabasePaths(runtimePath, businessPath string) error {
	runtimeAbsolute, err := filepath.Abs(filepath.Clean(runtimePath))
	if err != nil {
		return fmt.Errorf("resolve Runtime database path: %w", err)
	}
	businessAbsolute, err := filepath.Abs(filepath.Clean(businessPath))
	if err != nil {
		return fmt.Errorf("resolve Application database path: %w", err)
	}
	if runtimeAbsolute == businessAbsolute {
		return fmt.Errorf("runtime and application database paths must be different: both resolve to %q", runtimeAbsolute)
	}
	return nil
}

func makeParent(path string) error {
	parent := filepath.Dir(path)
	if parent == "." {
		return nil
	}
	return os.MkdirAll(parent, 0o755)
}
