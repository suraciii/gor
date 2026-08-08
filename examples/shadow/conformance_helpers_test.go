package shadow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/store"
)

type claimResult struct {
	won bool
	err error
}

type blockingReminderStore struct {
	store.ReminderStore
	claimed chan struct{}
	release chan struct{}
}

func (s *blockingReminderStore) Claim(ctx context.Context, reminder store.Reminder, nextDueAt time.Time) (bool, error) {
	won, err := s.ReminderStore.Claim(ctx, reminder, nextDueAt)
	if err != nil || !won {
		return won, err
	}
	close(s.claimed)
	<-s.release
	return won, nil
}

type faultApplicationStore struct {
	domain.ApplicationStore
	before      error
	afterCommit error
	beforeUsed  atomic.Bool
	afterUsed   atomic.Bool
}

func (s *faultApplicationStore) ApplyPending(ctx context.Context, actionID string) error {
	if s.before != nil && s.beforeUsed.CompareAndSwap(false, true) {
		return s.before
	}
	if err := s.ApplicationStore.ApplyPending(ctx, actionID); err != nil {
		return err
	}
	if s.afterCommit != nil && s.afterUsed.CompareAndSwap(false, true) {
		return s.afterCommit
	}
	return nil
}

func newConformanceRuntime(t *testing.T, sourceClock *clock.Fake, stateStore store.Store, reminderStore store.ReminderStore, application domain.ApplicationStore, observed chan<- domain.RecoveryObservation, calls chan gor.CallObservation) *gor.Runtime {
	t.Helper()
	return newConformanceRuntimeWithErrors(t, sourceClock, stateStore, reminderStore, application, observed, calls, nil)
}

func newConformanceRuntimeWithErrors(t *testing.T, sourceClock *clock.Fake, stateStore store.Store, reminderStore store.ReminderStore, application domain.ApplicationStore, observed chan<- domain.RecoveryObservation, calls chan gor.CallObservation, errorsSeen chan gor.BackgroundError) *gor.Runtime {
	t.Helper()
	if errorsSeen == nil {
		errorsSeen = make(chan gor.BackgroundError, 16)
	}
	options := []gor.Option{
		gor.WithStore(stateStore),
		gor.WithReminderStore(reminderStore),
		gor.WithClock(sourceClock),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithReminderInterval(domain.RecoveryInterval),
		gor.OnError(func(event gor.BackgroundError) { errorsSeen <- event }),
	}
	if calls != nil {
		options = append(options, gor.OnCall(func(observation gor.CallObservation) { calls <- observation }))
	}
	rt, err := gor.New(options...)
	if err != nil {
		t.Fatal(err)
	}
	if observed == nil {
		if err := shadow.RegisterConformance(rt, application); err != nil {
			rt.Kill()
			t.Fatal(err)
		}
	} else if err := shadow.RegisterConformanceWithObservation(rt, application, observed); err != nil {
		rt.Kill()
		t.Fatal(err)
	}
	return rt
}

func hasReminder(rows []store.Reminder, grainType, grainKey, name string) bool {
	for _, row := range rows {
		if row.GrainId.GrainType == grainType && row.GrainId.GrainKey == grainKey && row.Name == name {
			return true
		}
	}
	return false
}

func hasCall(calls chan gor.CallObservation, method string, wantErr error) bool {
	found := false
	for len(calls) > 0 {
		observation := <-calls
		if observation.Method == method && (wantErr == nil || errors.Is(observation.Err, wantErr)) {
			found = true
		}
	}
	return found
}

func drainCalls(calls chan gor.CallObservation) []gor.CallObservation {
	var result []gor.CallObservation
	for len(calls) > 0 {
		result = append(result, <-calls)
	}
	return result
}

func containsCall(observations []gor.CallObservation, method string, wantErr error) bool {
	for _, observation := range observations {
		if observation.Method == method && (wantErr == nil || errors.Is(observation.Err, wantErr)) {
			return true
		}
	}
	return false
}

var _ domain.ApplicationStore = (*faultApplicationStore)(nil)
var _ store.ReminderStore = (*blockingReminderStore)(nil)
