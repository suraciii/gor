package mail

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

type callResult struct {
	value any
	err   error
}

func TestMailbox_SerializesCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		box := New(2)
		defer box.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		secondStarted := make(chan struct{})
		firstDone := make(chan callResult, 1)
		secondDone := make(chan callResult, 1)

		go func() {
			value, err := box.Call(context.Background(), func(context.Context) (any, error) {
				close(started)
				<-release
				return "first", nil
			})
			firstDone <- callResult{value: value, err: err}
		}()
		synctest.Wait()
		<-started

		go func() {
			value, err := box.Call(context.Background(), func(context.Context) (any, error) {
				close(secondStarted)
				return "second", nil
			})
			secondDone <- callResult{value: value, err: err}
		}()
		synctest.Wait()
		select {
		case <-secondStarted:
			t.Fatal("second call ran before first call completed")
		default:
		}

		close(release)
		synctest.Wait()
		if result := <-firstDone; result.value != "first" || result.err != nil {
			t.Fatalf("first result = %#v, want first result", result)
		}
		if result := <-secondDone; result.value != "second" || result.err != nil {
			t.Fatalf("second result = %#v, want second result", result)
		}
	})
}

func TestMailbox_RejectsCallsWhenQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		box := New(1)
		defer box.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)

		go func() {
			_, err := box.Call(context.Background(), func(context.Context) (any, error) {
				close(started)
				<-release
				return nil, nil
			})
			firstDone <- err
		}()
		synctest.Wait()
		<-started

		go func() {
			_, err := box.Call(context.Background(), func(context.Context) (any, error) { return nil, nil })
			secondDone <- err
		}()
		synctest.Wait()

		if _, err := box.Call(context.Background(), func(context.Context) (any, error) { return nil, nil }); !errors.Is(err, ErrOverloaded) {
			t.Fatalf("third call error = %v, want ErrOverloaded", err)
		}

		close(release)
		synctest.Wait()
		if err := <-firstDone; err != nil {
			t.Fatalf("first call error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second call error = %v", err)
		}
	})
}

func TestMailbox_ContinuesAfterCallerTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		box := New(1)
		defer box.Close()

		started := make(chan struct{})
		release := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		callDone := make(chan error, 1)
		go func() {
			_, err := box.Call(ctx, func(context.Context) (any, error) {
				close(started)
				<-release
				return "timed out", nil
			})
			callDone <- err
		}()

		synctest.Wait()
		<-started
		cancel()
		synctest.Wait()
		if err := <-callDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("timed out call error = %v, want context.Canceled", err)
		}

		close(release)
		synctest.Wait()
		value, err := box.Call(context.Background(), func(context.Context) (any, error) {
			return "after timeout", nil
		})
		if err != nil || value != "after timeout" {
			t.Fatalf("follow-up call = %#v, %v", value, err)
		}
	})
}

func TestMailbox_RejectsQueuedCallsOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		box := New(1)

		started := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan callResult, 1)
		queuedDone := make(chan callResult, 1)
		go func() {
			value, err := box.Call(context.Background(), func(context.Context) (any, error) {
				close(started)
				<-release
				return "first", nil
			})
			firstDone <- callResult{value: value, err: err}
		}()
		synctest.Wait()
		<-started

		go func() {
			value, err := box.Call(context.Background(), func(context.Context) (any, error) {
				return "queued", nil
			})
			queuedDone <- callResult{value: value, err: err}
		}()
		synctest.Wait()
		box.Close()
		close(release)
		synctest.Wait()

		if result := <-firstDone; result.value != "first" || result.err != nil {
			t.Fatalf("first result = %#v, want first result", result)
		}
		if result := <-queuedDone; !errors.Is(result.err, ErrClosed) {
			t.Fatalf("queued result error = %v, want ErrClosed", result.err)
		}
	})
}
