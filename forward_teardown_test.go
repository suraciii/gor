package gor

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/store"
)

// blockingEntity is a scenario entity whose only method blocks until released.
// It records every entry so a test can tell whether a method body ran. Its
// request and reply are empty, so it carries no state and no observable result
// beyond the side effect of having run.
type blockingEntity interface {
	Hold(context.Context) error
}

type blockingHoldRequest struct{}
type blockingHoldReply struct{}

type blockingEntityProxy struct {
	invoker Invoker
	id      GrainId
}

func (p *blockingEntityProxy) Hold(ctx context.Context) error {
	return p.invoker.Invoke(ctx, p.id, "Hold", &blockingHoldRequest{}, &blockingHoldReply{})
}

type blockingEntityImpl struct {
	entries chan struct{}
	release chan struct{}
}

func (e *blockingEntityImpl) Hold(context.Context) error {
	e.entries <- struct{}{}
	<-e.release
	return nil
}

func dispatchBlockingEntity(ctx context.Context, instance blockingEntity, method string, _ any, _ any) error {
	if method != "Hold" {
		return fmt.Errorf("unknown method %q", method)
	}
	return instance.Hold(ctx)
}

func newBlockingEntityCall(method string) (args any, reply any) {
	if method != "Hold" {
		return nil, nil
	}
	return &blockingHoldRequest{}, &blockingHoldReply{}
}

func installBlockingEntity(t *testing.T, rt *Runtime, entries, release chan struct{}) {
	t.Helper()
	if err := InstallType[blockingEntity](rt, dispatchBlockingEntity, func(invoker Invoker, id GrainId) blockingEntity {
		return &blockingEntityProxy{invoker: invoker, id: id}
	}, newBlockingEntityCall); err != nil {
		t.Fatal(err)
	}
	if err := Register[blockingEntity](rt, func(b *Binder) blockingEntity {
		return &blockingEntityImpl{entries: entries, release: release}
	}); err != nil {
		t.Fatal(err)
	}
}

// TestScenario_ForwardedCallSurvivesOwnerClose is the forwarded-call scenario:
// the owner begins closing while a forwarded call it admitted is still running.
// The transport must stay open until that call's round trip finishes, so the
// in-flight forward returns the business result; a call forwarded after the
// stop began is rejected at the owner's admission gate.
func TestScenario_ForwardedCallSurvivesOwnerClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(2000, 0).UTC()
		fakeClock := clock.NewFake(start)
		members := store.NewMemory()
		backend := store.NewMemory()
		network := newTestTransportNetwork()
		source := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-a", "generation-a", network.add("node-a"))...)
		target := mustNew(t, clusterRuntimeOptions(backend, members, fakeClock, "node-b", "generation-b", network.add("node-b"))...)
		entries := make(chan struct{}, 8)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			// A failed assertion must not leave the bubble with live runtimes:
			// synctest would then panic over the goroutines and bury the real
			// failure. Release the blocked method first so both runtimes can
			// drain, then stop them.
			releaseOnce.Do(func() { close(release) })
			source.Close()
			target.Close()
		})
		installBlockingEntity(t, source, entries, release)
		installBlockingEntity(t, target, entries, release)
		synctest.Wait()
		fakeClock.Advance(time.Second)
		synctest.Wait()

		targetView := target.clusterView.Load()
		var owned cluster.View
		var id GrainId
		for index := 0; index < 4096; index++ {
			candidate := GrainId{GrainType: TypeName[blockingEntity](), GrainKey: strconv.Itoa(index)}
			owner, ok := cluster.Owner(*targetView, store.GrainId(candidate))
			if ok && owner == "node-b" {
				id = candidate
				owned = *targetView
				break
			}
		}
		if id == (GrainId{}) {
			t.Fatal("no identity owned by the target node")
		}
		// Sanity: the source routes this identity to the target.
		if owner, _ := cluster.Owner(owned, store.GrainId(id)); owner != "node-b" {
			t.Fatalf("source routes %v to %q, want node-b", id, owner)
		}

		// A forwarded call admitted before the stop: it crosses the network and
		// the owner's method starts running.
		forwardDone := make(chan error, 1)
		go func() {
			forwardDone <- source.Invoke(context.Background(), id, "Hold", &blockingHoldRequest{}, &blockingHoldReply{})
		}()
		synctest.Wait()
		<-entries

		closeDone := make(chan struct{})
		go func() {
			target.Close()
			close(closeDone)
		}()
		synctest.Wait()
		// Close has begun but cannot finish: the owner is still running the
		// admitted forward, and the transport is still open for its reply.
		select {
		case <-closeDone:
			t.Fatal("target Close returned before the in-flight forward finished")
		default:
		}

		// A call forwarded after the stop began is rejected at the owner's
		// admission gate, not queued behind the running method. The rejection is
		// ErrRuntimeClosed in the correct case; the assertion only requires that it
		// did not execute, so an early transport close (a different mutation) does
		// not also trip this check.
		retryDone := make(chan error, 1)
		go func() {
			retryDone <- source.Invoke(context.Background(), id, "Hold", &blockingHoldRequest{}, &blockingHoldReply{})
		}()
		synctest.Wait()
		select {
		case err := <-retryDone:
			if err == nil {
				t.Fatal("forward after owner Close executed, want rejection")
			}
		default:
			t.Fatal("forward after owner Close was queued instead of rejected at the admission gate")
		}

		// Releasing the admitted method lets its round trip finish with the
		// business result; only then does the owner close the transport. The
		// order is now enforced by the transport itself: the owner's graceful
		// close waits for the in-flight reply to reach the caller before it
		// completes.
		releaseOnce.Do(func() { close(release) })
		synctest.Wait()
		if err := <-forwardDone; err != nil {
			t.Fatalf("in-flight forward error = %v, want nil (business success)", err)
		}
		<-closeDone
	})
}
