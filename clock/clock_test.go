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
