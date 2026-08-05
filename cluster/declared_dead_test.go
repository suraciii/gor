package cluster

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

// DeclaredDead is the explicit reason channel the root coordinator reads
// instead of guessing from Done closing. These two tests pin its contract:
// external death closes it, voluntary Close does not.

func TestNode_DeclaredDeadClosesOnExternalDeath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1500, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		config.ViewInterval = time.Hour
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()

		self := findTestMember(t, backend, "node-a", "generation-a")
		self.Status = store.MemberDead
		if _, err := backend.WriteMember(context.Background(), self); err != nil {
			t.Fatalf("mark node dead: %v", err)
		}

		fakeClock.Advance(testHeartbeat)
		synctest.Wait()
		select {
		case <-node.Done():
		default:
			t.Fatal("node is still running after heartbeat found a dead self")
		}
		select {
		case <-node.DeclaredDead():
		default:
			t.Fatal("DeclaredDead is still open after external death")
		}
		node.Close()
	})
}

func TestNode_DeclaredDeadStaysOpenOnVoluntaryClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(1600, 0).UTC()
		fakeClock := clock.NewFake(start)
		backend := store.NewMemory()
		config := testNodeConfig(backend, fakeClock, "node-a", "generation-a")
		node, err := New(config)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		<-node.ViewChanges()
		synctest.Wait()

		node.Close()
		select {
		case <-node.Done():
		default:
			t.Fatal("node is still running after Close")
		}
		select {
		case <-node.DeclaredDead():
			t.Fatal("DeclaredDead closed after voluntary Close, want it to stay open")
		default:
		}
	})
}
