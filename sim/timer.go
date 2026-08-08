//go:build sim

package sim

import (
	"fmt"
	"sync"

	"github.com/suraciii/gor/store"
)

type timerTracker struct {
	mu          sync.Mutex
	outstanding map[store.GrainId]int
	failure     error
	deliveries  int
}

func newTimerTracker() *timerTracker {
	return &timerTracker{outstanding: make(map[store.GrainId]int)}
}

func (t *timerTracker) claim(id store.GrainId, willDeliver bool) {
	if !willDeliver {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outstanding[id]++
}

func (t *timerTracker) deliver(id store.GrainId) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outstanding[id] == 0 {
		t.failure = fmt.Errorf("delivery without an active claim for %s/%s", id.GrainType, id.GrainKey)
		return
	}
	t.outstanding[id]--
	t.deliveries++
}

func (t *timerTracker) check() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failure
}

func (t *timerTracker) deliveryCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.deliveries
}
