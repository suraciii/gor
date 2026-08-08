package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestMemoryReminderStore(t *testing.T) {
	runReminderStoreTests(t, NewMemory())
}

func TestSQLiteReminderStore(t *testing.T) {
	runReminderStoreTests(t, newSQLiteTestStore(t))
}

func TestSQLiteReminderStore_PreservesFirstTickAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reminders.db")
	first, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite first: %v", err)
	}
	row := Reminder{
		GrainId:       GrainId{GrainType: "account", GrainKey: "alice"},
		Name:          "wake",
		Method:        "Wake",
		FirstTickTime: time.Unix(10, 0).UTC(),
		DueAt:         time.Unix(20, 0).UTC(),
		Interval:      time.Hour,
	}
	if err := first.Put(context.Background(), row); err != nil {
		first.Close()
		t.Fatalf("Put: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite second: %v", err)
	}
	defer second.Close()
	got, err := second.ListDue(context.Background(), row.DueAt)
	if err != nil {
		t.Fatalf("ListDue after reopen: %v", err)
	}
	if len(got) != 1 || !got[0].FirstTickTime.Equal(row.FirstTickTime) || !got[0].DueAt.Equal(row.DueAt) {
		t.Fatalf("row after reopen = %#v, want FirstTickTime %s and DueAt %s", got, row.FirstTickTime, row.DueAt)
	}
}

func runReminderStoreTests(t *testing.T, backend ReminderStore) {
	t.Helper()
	t.Run("WriteAndListDue", func(t *testing.T) {
		ctx := context.Background()
		now := time.Unix(100, 0).UTC()
		due := Reminder{
			GrainId:       GrainId{GrainType: "account", GrainKey: "alice"},
			Name:          "wake",
			Method:        "Wake",
			FirstTickTime: now.Add(-2 * time.Second),
			DueAt:         now.Add(-time.Second),
			Interval:      time.Hour,
		}
		if err := backend.Put(ctx, due); err != nil {
			t.Fatalf("Put due: %v", err)
		}
		future := due
		future.Name = "later"
		future.DueAt = now.Add(2 * time.Hour)
		if err := backend.Put(ctx, future); err != nil {
			t.Fatalf("Put future: %v", err)
		}

		got, err := backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ListDue returned %d rows, want 1", len(got))
		}
		if got[0].GrainId != due.GrainId || got[0].Name != due.Name || got[0].Method != due.Method || !got[0].FirstTickTime.Equal(due.FirstTickTime) || !got[0].DueAt.Equal(due.DueAt) || got[0].Interval != due.Interval || got[0].ETag != 1 {
			t.Fatalf("due row = %#v, want %#v with ETag 1", got[0], due)
		}

		replacement := due
		replacement.Method = "WakeAgain"
		replacement.FirstTickTime = now.Add(30 * time.Minute)
		replacement.DueAt = now.Add(time.Hour)
		if err := backend.Put(ctx, replacement); err != nil {
			t.Fatalf("Put replacement: %v", err)
		}
		won, err := backend.Claim(ctx, due, now.Add(2*time.Hour))
		if err != nil {
			t.Fatalf("Claim with stale ETag: %v", err)
		}
		if won {
			t.Fatal("Claim with stale ETag won, want false")
		}
		got, err = backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue after replacement: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListDue after replacement returned %d rows, want 0", len(got))
		}
		got, err = backend.ListDue(ctx, now.Add(time.Hour))
		if err != nil {
			t.Fatalf("ListDue at replacement: %v", err)
		}
		if len(got) != 1 || got[0].Method != replacement.Method || !got[0].FirstTickTime.Equal(replacement.FirstTickTime) || got[0].ETag != 2 {
			t.Fatalf("replacement rows = %#v, want method %q and ETag 2", got, replacement.Method)
		}
	})

	t.Run("ClaimCASAllowsExactlyOneWinner", func(t *testing.T) {
		ctx := context.Background()
		now := time.Unix(200, 0).UTC()
		task := Reminder{
			GrainId:  GrainId{GrainType: "account", GrainKey: "bob"},
			Name:     "wake",
			Method:   "Wake",
			DueAt:    now.Add(-time.Second),
			Interval: time.Hour,
		}
		if err := backend.Put(ctx, task); err != nil {
			t.Fatalf("Put: %v", err)
		}
		due, err := backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		if len(due) != 1 {
			t.Fatalf("ListDue returned %d rows, want 1", len(due))
		}
		task = due[0]
		nextDueAt := now.Add(time.Hour)
		start := make(chan struct{})
		results := make(chan bool, 2)
		errors := make(chan error, 2)
		var wait sync.WaitGroup
		for range 2 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				won, err := backend.Claim(ctx, task, nextDueAt)
				results <- won
				errors <- err
			}()
		}
		close(start)
		wait.Wait()
		close(results)
		close(errors)

		wins := 0
		for won := range results {
			if won {
				wins++
			}
		}
		for err := range errors {
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
		}
		if wins != 1 {
			t.Fatalf("Claim winners = %d, want 1", wins)
		}

		got, err := backend.ListDue(ctx, nextDueAt)
		if err != nil {
			t.Fatalf("ListDue after Claim: %v", err)
		}
		var claimed Reminder
		found := false
		for _, candidate := range got {
			if candidate.GrainId == task.GrainId && candidate.Name == task.Name {
				claimed = candidate
				found = true
				break
			}
		}
		if !found || claimed.ETag != task.ETag+1 || !claimed.FirstTickTime.Equal(task.FirstTickTime) || !claimed.DueAt.Equal(nextDueAt) {
			t.Fatalf("claimed row = %#v, want %s/%s at next due time and ETag %d", got, task.GrainId.GrainType, task.Name, task.ETag+1)
		}
	})

	t.Run("OneShotClaimDeletes", func(t *testing.T) {
		ctx := context.Background()
		now := time.Unix(300, 0).UTC()
		task := Reminder{
			GrainId: GrainId{GrainType: "account", GrainKey: "carol"},
			Name:    "once",
			Method:  "Wake",
			DueAt:   now.Add(-time.Second),
		}
		if err := backend.Put(ctx, task); err != nil {
			t.Fatalf("Put: %v", err)
		}
		due, err := backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		won, err := backend.Claim(ctx, due[0], time.Time{})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if !won {
			t.Fatal("Claim won = false, want true")
		}
		remaining, err := backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue after one-shot Claim: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("remaining rows = %#v, want none", remaining)
		}
	})

	t.Run("DeleteIsUnconditional", func(t *testing.T) {
		ctx := context.Background()
		now := time.Unix(400, 0).UTC()
		task := Reminder{
			GrainId: GrainId{GrainType: "account", GrainKey: "dave"},
			Name:    "cancel",
			Method:  "Wake",
			DueAt:   now.Add(time.Hour),
		}
		if err := backend.Put(ctx, task); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := backend.Delete(ctx, task.GrainId, task.Name); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		remaining, err := backend.ListDue(ctx, now)
		if err != nil {
			t.Fatalf("ListDue after Delete: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("remaining rows = %#v, want none", remaining)
		}
	})
}
