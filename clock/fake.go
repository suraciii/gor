package clock

import (
	"sync"
	"time"
)

type Fake struct {
	mu      sync.Mutex
	now     time.Time
	tickers map[*fakeTicker]struct{}
}

func NewFake(now time.Time) *Fake {
	return &Fake{
		now:     now,
		tickers: make(map[*fakeTicker]struct{}),
	}
}

var _ Clock = (*Fake)(nil)

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

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

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	end := f.now
	deliveries := make([]fakeDelivery, 0)
	for ticker := range f.tickers {
		for !ticker.next.After(end) {
			deliveries = append(deliveries, fakeDelivery{ticker: ticker, at: ticker.next})
			ticker.next = ticker.next.Add(ticker.interval)
		}
	}
	f.mu.Unlock()

	for _, delivery := range deliveries {
		select {
		case delivery.ticker.channel <- delivery.at:
		case <-delivery.ticker.stopped:
			continue
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
