package clock

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestRealClock_NowReturnsTime(t *testing.T) {
	if (Real{}).Now().IsZero() {
		t.Fatal("real clock returned zero time")
	}
}

func TestRealClock_TickerCanStop(t *testing.T) {
	ticker := (Real{}).NewTicker(time.Hour)
	ticker.Stop()
	ticker.Stop()
}

func TestRealClock_TickerUsesBubbleTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := Real{}
		start := clock.Now()
		ticker := clock.NewTicker(time.Second)
		defer ticker.Stop()

		<-ticker.C()

		if elapsed := clock.Now().Sub(start); elapsed != time.Second {
			t.Fatalf("ticker advanced time by %s, want %s", elapsed, time.Second)
		}
	})
}

func TestFakeClock_TickersAdvanceIndependently(t *testing.T) {
	fake := NewFake(time.Unix(0, 0).UTC())
	fast := fake.NewTicker(time.Second)
	slow := fake.NewTicker(2 * time.Second)

	fake.Advance(1500 * time.Millisecond)
	if got := <-fast.C(); !got.Equal(time.Unix(1, 0).UTC()) {
		t.Fatalf("fast tick = %s, want 1970-01-01 00:00:01 UTC", got)
	}
	select {
	case got := <-slow.C():
		t.Fatalf("slow ticker fired early at %s", got)
	default:
	}

	fake.Advance(500 * time.Millisecond)
	if got := <-fast.C(); !got.Equal(time.Unix(2, 0).UTC()) {
		t.Fatalf("second fast tick = %s, want 1970-01-01 00:00:02 UTC", got)
	}
	if got := <-slow.C(); !got.Equal(time.Unix(2, 0).UTC()) {
		t.Fatalf("slow tick = %s, want 1970-01-01 00:00:02 UTC", got)
	}
}

func TestFakeClock_StopRemovesTicker(t *testing.T) {
	fake := NewFake(time.Unix(0, 0).UTC())
	ticker := fake.NewTicker(time.Second)
	ticker.Stop()
	fake.Advance(time.Second)
	select {
	case <-ticker.C():
		t.Fatal("stopped ticker received a tick")
	default:
	}
}

func TestFakeClock_DropsBackloggedTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := NewFake(time.Unix(0, 0).UTC())
		ticker := fake.NewTicker(time.Second)
		defer ticker.Stop()

		done := make(chan struct{})
		go func() {
			for range 5 {
				fake.Advance(time.Second)
			}
			close(done)
		}()
		<-done

		<-ticker.C()
		select {
		case <-ticker.C():
			t.Fatal("fake ticker retained more than one unread tick")
		default:
		}
	})
}
