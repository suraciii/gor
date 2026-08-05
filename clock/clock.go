// Package clock provides injectable time sources for gor.
//
// Runtime code obtains the current time and periodic notifications through
// Clock, so production uses Real while deterministic tests can use Fake.
package clock

import "time"

// Clock supplies the time source used by a gor runtime.
//
// Implementations must be safe for concurrent calls. Now reports the current
// time on the implementation's timeline. NewTicker requires a positive
// duration and must panic when given a non-positive duration.
type Clock interface {
	// Now returns the current time on this clock's timeline.
	Now() time.Time
	// NewTicker returns a ticker that reports ticks at the given interval.
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers periodic times from a Clock.
//
// C returns the notification channel. Stop prevents future ticks, is safe to
// call more than once, and does not close the channel.
type Ticker interface {
	// C returns the channel on which ticks are delivered.
	C() <-chan time.Time
	// Stop prevents future ticks from being delivered.
	Stop()
}

// Real uses the process's real time source.
type Real struct{}

// Now returns the current real time.
func (Real) Now() time.Time {
	return time.Now()
}

// NewTicker returns a real-time ticker. It panics when d is not positive.
func (Real) NewTicker(d time.Duration) Ticker {
	return realTicker{ticker: time.NewTicker(d)}
}

type realTicker struct {
	ticker *time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t realTicker) Stop() {
	t.ticker.Stop()
}
