package clock

import (
	"sync"
	"time"
)

// Fake is a deterministic Clock for tests.
//
// Create it with NewFake; its zero value is not ready for use. Advance moves
// the clock and notifies each active ticker. A ticker retains at most one
// unread notification, so advancing across several intervals does not queue
// one notification for every missed interval.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	tickers map[*fakeTicker]struct{}
}

// NewFake returns a Fake whose current time is now.
func NewFake(now time.Time) *Fake {
	return &Fake{
		now:     now,
		tickers: make(map[*fakeTicker]struct{}),
	}
}

var _ Clock = (*Fake)(nil)

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTicker returns a ticker driven by Advance. It panics when interval is
// not positive.
func (f *Fake) NewTicker(interval time.Duration) Ticker {
	if interval <= 0 {
		panic("clock: non-positive fake ticker interval")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ticker := &fakeTicker{
		clock:    f,
		interval: interval,
		next:     f.now.Add(interval),
		channel:  make(chan time.Time, 1),
		stopped:  make(chan struct{}),
	}
	f.tickers[ticker] = struct{}{}
	return ticker
}

// Advance moves the fake clock by d. A negative d moves the clock backward and
// produces no new tick. When a positive advance crosses multiple deadlines for
// a ticker, Advance attempts to deliver only the latest crossed deadline; the
// earlier deadlines are discarded. If the ticker already has an unread tick,
// the new tick is discarded as well.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	end := f.now
	deliveries := make([]fakeDelivery, 0)
	for ticker := range f.tickers {
		var at time.Time
		due := false
		for !ticker.next.After(end) {
			at = ticker.next
			due = true
			ticker.next = ticker.next.Add(ticker.interval)
		}
		if due {
			deliveries = append(deliveries, fakeDelivery{ticker: ticker, at: at})
		}
	}
	f.mu.Unlock()

	for _, delivery := range deliveries {
		select {
		case delivery.ticker.channel <- delivery.at:
		default:
		}
	}
}

type fakeDelivery struct {
	ticker *fakeTicker
	at     time.Time
}

type fakeTicker struct {
	clock    *Fake
	interval time.Duration
	next     time.Time
	channel  chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.channel
}

func (t *fakeTicker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopped)
		t.clock.mu.Lock()
		delete(t.clock.tickers, t)
		t.clock.mu.Unlock()
	})
}
