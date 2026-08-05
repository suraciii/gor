// Package runtime is gor's local actor engine: it manages entity activation,
// lifecycle, mailboxes, and dispatch within one process.
//
// Application code should use the root package's gor.New, gor.Register,
// gor.Ref, and *gor.Runtime APIs instead of importing runtime directly.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/mail"
)

var (
	ErrTypeNotRegistered = errors.New("entity type is not registered")
	ErrRuntimeClosed     = errors.New("runtime closed")
)

type Identity struct {
	Type string
	Key  string
}

type Activation struct {
	Identity Identity
	Queued   int
}

type Dispatch func(context.Context, any, string, any, any) error

type Registration struct {
	Factory      func(context.Context, Identity) (any, error)
	Dispatch     Dispatch
	OnDeactivate func(context.Context, Identity, any)
}

type Discard struct {
	Err error
}

func (d Discard) Error() string {
	if d.Err == nil {
		return "activation discarded"
	}
	return d.Err.Error()
}

func (d Discard) Unwrap() error {
	return d.Err
}

type Config struct {
	Clock            clock.Clock
	MailboxCapacity  int
	IdleTimeout      time.Duration
	EvictionInterval time.Duration
}

type Runtime struct {
	clock           clock.Clock
	mailboxCapacity int
	idleTimeout     time.Duration

	mu            sync.Mutex
	closed        bool
	registrations map[string]Registration
	activations   map[Identity]*activation
	pending       map[Identity]*entry

	stop                chan struct{}
	ticker              clock.Ticker
	evictionDone        chan struct{}
	deactivationWaiters sync.WaitGroup
	killCtx             context.Context
	killCancel          context.CancelFunc
}

type ActivationState uint8

const (
	ActivationActivating ActivationState = iota
	ActivationActive
	ActivationDeactivating
	ActivationStopped
)

type activation struct {
	id               Identity
	instance         any
	onDeactivate     func(context.Context, Identity, any)
	skipOnDeactivate bool
	mailbox          *mail.Box
	lastUsed         time.Time
	calls            int
	state            ActivationState
	done             chan struct{}
}

type entry struct {
	ready chan struct{}
	act   *activation
	err   error
}

func New(config Config) *Runtime {
	killCtx, killCancel := context.WithCancel(context.Background())
	r := &Runtime{
		clock:           config.Clock,
		mailboxCapacity: config.MailboxCapacity,
		idleTimeout:     config.IdleTimeout,
		registrations:   make(map[string]Registration),
		activations:     make(map[Identity]*activation),
		pending:         make(map[Identity]*entry),
		stop:            make(chan struct{}),
		evictionDone:    make(chan struct{}),
		killCtx:         killCtx,
		killCancel:      killCancel,
	}
	if config.IdleTimeout > 0 && config.EvictionInterval > 0 {
		r.ticker = config.Clock.NewTicker(config.EvictionInterval)
		go r.evictLoop()
	} else {
		close(r.evictionDone)
	}
	return r
}

func (r *Runtime) Register(name string, registration Registration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.registrations[name]; exists {
		return fmt.Errorf("entity type %q is already registered", name)
	}
	r.registrations[name] = registration
	return nil
}

func (r *Runtime) Invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(r.killCtx, cancel)()

	registration, err := r.registration(id.Type)
	if err != nil {
		return err
	}
	for {
		act, err := r.activationFor(callCtx, id, registration)
		if err != nil {
			return err
		}
		if !r.admit(act) {
			continue
		}
		_, err = act.mailbox.Call(callCtx, func(callCtx context.Context) (any, error) {
			defer r.callFinished(act)
			return nil, r.dispatch(registration, act, callCtx, method, args, reply)
		})
		if errors.Is(err, mail.ErrOverloaded) || errors.Is(err, mail.ErrClosed) {
			r.callFinished(act)
		}
		return err
	}
}

func (r *Runtime) Deactivate(id Identity) {
	r.mu.Lock()
	act, ok := r.activations[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	started := r.deactivateLocked(act)
	r.mu.Unlock()
	if started {
		act.mailbox.Close()
	}
}

func (r *Runtime) Identities() []Identity {
	r.mu.Lock()
	defer r.mu.Unlock()
	identities := make([]Identity, 0, len(r.activations))
	for id := range r.activations {
		identities = append(identities, id)
	}
	return identities
}

func (r *Runtime) Activations() []Activation {
	r.mu.Lock()
	activations := make([]Activation, 0, len(r.activations))
	for _, act := range r.activations {
		if act.state != ActivationActive {
			continue
		}
		activations = append(activations, Activation{
			Identity: act.id,
			Queued:   act.mailbox.Len(),
		})
	}
	r.mu.Unlock()

	sort.Slice(activations, func(i, j int) bool {
		if activations[i].Identity.Type != activations[j].Identity.Type {
			return activations[i].Identity.Type < activations[j].Identity.Type
		}
		return activations[i].Identity.Key < activations[j].Identity.Key
	})
	return activations
}

func (r *Runtime) Close() {
	down, ok := r.beginShutdown(false)
	if !ok {
		return
	}
	r.mu.Lock()
	for _, act := range down.newlyDeactivating {
		r.startDeactivationWaiterLocked(act)
	}
	r.mu.Unlock()
	for _, act := range down.activations {
		act.mailbox.Close()
	}
	<-r.evictionDone
	r.deactivationWaiters.Wait()
}

func (r *Runtime) Kill() {
	down, ok := r.beginShutdown(true)
	if !ok {
		return
	}
	r.killCancel()
	r.mu.Lock()
	for _, act := range down.newlyDeactivating {
		r.startDeactivationWaiterLocked(act)
	}
	r.mu.Unlock()
	for _, act := range down.activations {
		act.mailbox.Close()
	}
	<-r.evictionDone
}

type shutdown struct {
	activations       []*activation
	newlyDeactivating []*activation
}

func (r *Runtime) beginShutdown(skipOnDeactivate bool) (shutdown, bool) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return shutdown{}, false
	}
	r.closed = true
	close(r.stop)
	result := shutdown{
		activations:       make([]*activation, 0, len(r.activations)),
		newlyDeactivating: make([]*activation, 0, len(r.activations)),
	}
	for _, act := range r.activations {
		result.activations = append(result.activations, act)
		if skipOnDeactivate {
			act.skipOnDeactivate = true
		}
		if beginDeactivation(act) {
			result.newlyDeactivating = append(result.newlyDeactivating, act)
		}
	}
	ticker := r.ticker
	r.mu.Unlock()
	if ticker != nil {
		ticker.Stop()
	}
	return result, true
}

func (r *Runtime) registration(name string) (Registration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Registration{}, ErrRuntimeClosed
	}
	registration, ok := r.registrations[name]
	if !ok {
		return Registration{}, fmt.Errorf("%w: %s", ErrTypeNotRegistered, name)
	}
	return registration, nil
}

func (r *Runtime) activationFor(ctx context.Context, id Identity, registration Registration) (*activation, error) {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrRuntimeClosed
		}
		if act, ok := r.activations[id]; ok {
			switch act.state {
			case ActivationActive:
				r.mu.Unlock()
				return act, nil
			case ActivationDeactivating:
				done := act.done
				r.mu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			case ActivationStopped:
				delete(r.activations, id)
			}
		}
		if pending, ok := r.pending[id]; ok {
			ready := pending.ready
			r.mu.Unlock()
			select {
			case <-ready:
				if pending.err != nil {
					return nil, pending.err
				}
				return pending.act, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		pending := &entry{ready: make(chan struct{})}
		r.pending[id] = pending
		r.mu.Unlock()
		return r.createActivation(ctx, id, registration, pending)
	}
}

func (r *Runtime) createActivation(ctx context.Context, id Identity, registration Registration, pending *entry) (act *activation, err error) {
	defer func() {
		if value := recover(); value != nil {
			act = nil
			err = fmt.Errorf("activation factory panicked: %v", value)
		}
		r.mu.Lock()
		delete(r.pending, id)
		if err == nil && r.closed {
			err = ErrRuntimeClosed
		}
		pending.act = act
		pending.err = err
		if err == nil {
			r.activations[id] = act
		}
		close(pending.ready)
		r.mu.Unlock()
		if err != nil && act != nil {
			act.mailbox.Close()
		}
	}()

	act, err = r.activate(ctx, id, registration)
	return act, err
}

func (r *Runtime) activate(ctx context.Context, id Identity, registration Registration) (*activation, error) {
	act := &activation{
		id:           id,
		onDeactivate: registration.OnDeactivate,
		state:        ActivationActivating,
		done:         make(chan struct{}),
	}
	instance, err := registration.Factory(ctx, id)
	if err != nil {
		return nil, err
	}
	act.instance = instance
	act.mailbox = mail.New(r.mailboxCapacity)
	act.lastUsed = r.clock.Now()
	act.state = ActivationActive
	return act, nil
}

func (r *Runtime) admit(act *activation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if act.state != ActivationActive {
		return false
	}
	act.lastUsed = r.clock.Now()
	act.calls++
	return true
}

func (r *Runtime) callFinished(act *activation) {
	r.mu.Lock()
	act.calls--
	r.mu.Unlock()
}

func (r *Runtime) dispatch(registration Registration, act *activation, ctx context.Context, method string, args any, reply any) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("entity method panicked: %v", value)
			r.stopActivation(act)
		}
	}()
	err = registration.Dispatch(ctx, act.instance, method, args, reply)
	var discard Discard
	if errors.As(err, &discard) {
		r.stopActivation(act)
		return discard.Err
	}
	return err
}

func (r *Runtime) evictLoop() {
	defer close(r.evictionDone)
	for {
		select {
		case now := <-r.ticker.C():
			r.evict(now)
		case <-r.stop:
			return
		}
	}
}

func (r *Runtime) evict(now time.Time) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	victims := make([]*activation, 0)
	for _, act := range r.activations {
		if act.state == ActivationActive && act.calls == 0 && !now.Before(act.lastUsed.Add(r.idleTimeout)) && r.deactivateLocked(act) {
			victims = append(victims, act)
		}
	}
	r.mu.Unlock()

	for _, act := range victims {
		act.mailbox.Close()
	}
}

func (r *Runtime) stopActivation(act *activation) {
	r.mu.Lock()
	started := r.deactivateLocked(act)
	r.mu.Unlock()
	if !started {
		return
	}
	act.mailbox.Close()
}

func (r *Runtime) deactivateLocked(act *activation) bool {
	if !beginDeactivation(act) {
		return false
	}
	r.startDeactivationWaiterLocked(act)
	return true
}

func (r *Runtime) startDeactivationWaiterLocked(act *activation) {
	r.deactivationWaiters.Add(1)
	go func() {
		defer r.deactivationWaiters.Done()
		r.waitForDeactivation(act)
	}()
}

func (r *Runtime) waitForDeactivation(act *activation) {
	<-act.mailbox.Done()
	r.mu.Lock()
	var onDeactivate func(context.Context, Identity, any)
	if !act.skipOnDeactivate {
		onDeactivate = act.onDeactivate
	}
	r.mu.Unlock()
	if onDeactivate != nil {
		onDeactivate(context.Background(), act.id, act.instance)
	}
	r.finishDeactivation(act)
}

func (r *Runtime) finishDeactivation(act *activation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !finishDeactivation(act) {
		return
	}
	if current, ok := r.activations[act.id]; ok && current == act {
		delete(r.activations, act.id)
	}
}

func beginDeactivation(act *activation) bool {
	if act.state != ActivationActive {
		return false
	}
	act.state = ActivationDeactivating
	return true
}

func finishDeactivation(act *activation) bool {
	if act.state != ActivationDeactivating {
		return false
	}
	act.state = ActivationStopped
	close(act.done)
	return true
}
