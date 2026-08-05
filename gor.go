// Package gor provides the application-facing API for defining, registering,
// invoking, and persisting virtual actors.
package gor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/cluster"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/timer"
	"github.com/suraciii/gor/transport"
)

// Identity identifies an entity by its registered type name and key.
type Identity = runtimepkg.Identity
type Activation = runtimepkg.Activation

// CallObservation describes one invocation observed by an OnCall callback.
// EntityType and Method identify the call, Duration is measured with the
// observing Runtime's configured clock, and Err is the error returned to the
// caller.
//
// For a forwarded call, the initiating Runtime reports one observation whose
// duration includes forwarding; the owning Runtime does not report a second
// observation. A remote coded error is reconstructed so errors.Is matches its
// Code; an opaque remote error retains only its diagnostic text.
type CallObservation struct {
	EntityType string
	Method     string
	Duration   time.Duration
	Err        error
}

// Scope is the generated-reference scope accepted by Ref. Runtime and Binder
// implement it; application code should pass those values rather than
// implement Scope.
type Scope interface {
	scopeRuntime() *Runtime
}

// Activatable is implemented by an entity that needs a hook after its state is
// loaded and before its first method call. An OnActivate error prevents that
// activation from being established and is returned by the triggering call.
type Activatable interface {
	OnActivate(context.Context) error
}

// Deactivatable is implemented by an entity that needs a hook when its
// activation is normally stopped. Its error is reported through OnError when
// configured; Kill skips this hook.
type Deactivatable interface {
	OnDeactivate(context.Context) error
}

// Runtime coordinates entity registration, activation, invocation, state, and
// schedules. Invocations for the same identity are serialized, and a runtime
// configured for a cluster can route an invocation to its current owner.
// Create a Runtime with New and stop it with Close or Kill.
type Runtime struct {
	engine           *runtimepkg.Runtime
	store            store.Store
	scheduleStore    store.ScheduleStore
	clock            clock.Clock
	onError          func(Identity, string, error)
	onCall           func(CallObservation)
	poller           *timer.Poller
	transport        transport.Transport
	transportDone    chan struct{}
	transportStop    context.CancelFunc
	transportClosing atomic.Bool
	clusterNode      *cluster.Node
	clusterView      atomic.Pointer[cluster.View]
	clusterDone      chan struct{}
	done             chan struct{}

	lifecycleMu sync.Mutex
	state       rootState
	stopCode    Code
	inflight    int
	drained     chan struct{}

	nodeAddr string
	typesMu  sync.Mutex
	types    map[string]typeRegistration
}

// Config contains the settings assembled by New from Option values. Use the
// provided option functions for normal configuration; custom options may
// inspect or modify Config directly. Omitted fields receive the defaults
// described by their corresponding option.
type Config struct {
	runtimepkg.Config
	Store             store.Store
	ScheduleStore     store.ScheduleStore
	ScheduleInterval  time.Duration
	Transport         transport.Transport
	OnError           func(Identity, string, error)
	OnCall            func(CallObservation)
	MemberStore       store.MemberStore
	NodeAddr          string
	Generation        string
	HeartbeatInterval time.Duration
	ViewInterval      time.Duration
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	ProbeFailures     int
	VoteTTL           time.Duration
	MaxTickGap        time.Duration
	MaxTableLatency   time.Duration
}

// Invoker is the generated proxy call boundary. Generated code receives an
// Invoker from the runtime; application code normally consumes generated
// proxies instead of implementing Invoker.
type Invoker interface {
	// Invoke invokes method for id, using the generated request in args and
	// writing the generated response to reply. For a local call, ctx bounds
	// activation and delivery; for a forwarded call, it bounds forwarding at
	// the initiating Runtime. A forwarded call may continue on the owning
	// Runtime after ctx is canceled, and errors returned from that Runtime do
	// not preserve the original errors.Is or errors.As identity.
	Invoke(context.Context, Identity, string, any, any) error
}

var _ Invoker = (*Runtime)(nil)

type typeRegistration struct {
	dispatch runtimepkg.Dispatch
	newProxy func(Invoker, Identity) any
	newCall  func(string) (any, any)
}

// Option configures a Runtime created by New. New applies options in argument
// order, then derives a schedule store when none was supplied.
type Option func(*Config)

func (rt *Runtime) scopeRuntime() *Runtime {
	return rt
}

func (b *Binder) scopeRuntime() *Runtime {
	return b.runtime
}

// New creates and starts a Runtime.
//
// By default, New uses clock.Real{}, store.NewMemory for entity state and
// schedules, a mailbox capacity of 16, a one-minute idle timeout, one-second
// eviction and schedule intervals, and one-second heartbeat and view
// intervals. A MemberStore and Transport must be configured together. In
// clustered mode, ProbeInterval, ProbeTimeout, ProbeFailures, VoteTTL,
// MaxTickGap, and MaxTableLatency must also be positive; leaving them at their
// zero values returns an error matching cluster.ErrInvalidConfig.
//
// New returns an error if cluster initialization fails. The returned Runtime
// is ready for entity installation and registration.
func New(options ...Option) (*Runtime, error) {
	config := Config{
		Config: runtimepkg.Config{
			Clock:            clock.Real{},
			MailboxCapacity:  16,
			IdleTimeout:      time.Minute,
			EvictionInterval: time.Second,
		},
		Store:             store.NewMemory(),
		ScheduleInterval:  time.Second,
		HeartbeatInterval: time.Second,
		ViewInterval:      time.Second,
	}
	for _, option := range options {
		option(&config)
	}
	if (config.MemberStore == nil) != (config.Transport == nil) {
		return nil, errors.New("member store and transport must be configured together")
	}
	if config.ScheduleStore == nil {
		if schedules, ok := config.Store.(store.ScheduleStore); ok {
			config.ScheduleStore = schedules
		}
	}
	var (
		clusterNode *cluster.Node
		initialView cluster.View
	)
	if config.MemberStore != nil {
		var err error
		clusterNode, err = cluster.New(cluster.Config{
			Table:             config.MemberStore,
			Clock:             config.Clock,
			Prober:            transportProber{transport: config.Transport},
			NodeAddr:          config.NodeAddr,
			Generation:        config.Generation,
			HeartbeatInterval: config.HeartbeatInterval,
			ViewInterval:      config.ViewInterval,
			ProbeInterval:     config.ProbeInterval,
			ProbeTimeout:      config.ProbeTimeout,
			ProbeFailures:     config.ProbeFailures,
			VoteTTL:           config.VoteTTL,
			MaxTickGap:        config.MaxTickGap,
			MaxTableLatency:   config.MaxTableLatency,
		})
		if err != nil {
			return nil, fmt.Errorf("start cluster node: %w", err)
		}
		initialView = <-clusterNode.ViewChanges()
	}
	rt := &Runtime{
		engine:        runtimepkg.New(config.Config),
		store:         config.Store,
		scheduleStore: config.ScheduleStore,
		clock:         config.Clock,
		onError:       config.OnError,
		onCall:        config.OnCall,
		transport:     config.Transport,
		done:          make(chan struct{}),
		types:         make(map[string]typeRegistration),
	}
	if clusterNode != nil {
		rt.clusterNode = clusterNode
		rt.clusterDone = make(chan struct{})
		rt.nodeAddr = config.NodeAddr
		rt.clusterView.Store(&initialView)
		go rt.watchCluster()
	}
	if config.ScheduleStore != nil && config.ScheduleInterval > 0 {
		rt.poller = timer.New(config.ScheduleStore, config.Clock, config.ScheduleInterval, scheduleInvoker{runtime: rt})
	}
	if rt.clusterNode != nil && rt.transport != nil {
		rt.startTransport()
	}
	return rt, nil
}

// WithClock sets the clock used by runtime and cluster timers. If omitted, New
// uses clock.Real{}.
func WithClock(value clock.Clock) Option {
	return func(config *Config) {
		config.Clock = value
	}
}

// WithMailboxCapacity sets the number of calls that may wait in one entity's
// mailbox. If omitted, New allows 16 queued calls per entity; calls that cannot
// be queued are rejected. The value must not be negative; New panics when it
// creates a mailbox with a negative capacity.
func WithMailboxCapacity(value int) Option {
	return func(config *Config) {
		config.MailboxCapacity = value
	}
}

// WithIdleTimeout sets how long an unused activation may remain before idle
// eviction. If omitted, New uses one minute. A non-positive value disables
// idle eviction.
func WithIdleTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.IdleTimeout = value
	}
}

// WithEvictionInterval sets how often idle activations are checked. If omitted,
// New checks once per second. A non-positive value disables idle eviction.
func WithEvictionInterval(value time.Duration) Option {
	return func(config *Config) {
		config.EvictionInterval = value
	}
}

// WithStore sets the store used for entity state. If omitted, New uses an
// in-memory store; when the selected store also implements ScheduleStore and
// no schedule store is supplied, New uses it for schedules too.
func WithStore(value store.Store) Option {
	return func(config *Config) {
		config.Store = value
	}
}

// WithScheduleStore sets the store used for entity schedules. If omitted, New
// derives it from Store when Store implements ScheduleStore; otherwise schedule
// operations return ErrScheduleStoreUnavailable.
func WithScheduleStore(value store.ScheduleStore) Option {
	return func(config *Config) {
		config.ScheduleStore = value
	}
}

// WithScheduleInterval sets the interval for background schedule polling. If
// omitted, New polls once per second. A non-positive value keeps schedules
// persisted but disables automatic polling.
func WithScheduleInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ScheduleInterval = value
	}
}

// WithTransport sets the transport used by a clustered runtime for serving and
// forwarding calls. If omitted, no transport is started; a transport must be
// configured together with a MemberStore.
func WithTransport(value transport.Transport) Option {
	return func(config *Config) {
		config.Transport = value
	}
}

// OnError sets the callback for errors from background scheduled invocations
// and normal OnDeactivate hooks. If omitted, those errors are not reported.
// The callback may run asynchronously and concurrently with application code;
// it is not called for ordinary foreground Invoke errors. Cancellation errors
// from scheduled invocations during shutdown are not reported.
func OnError(f func(id Identity, method string, err error)) Option {
	return func(config *Config) {
		config.OnError = f
	}
}

// OnCall sets the callback invoked after each call initiated through this
// Runtime, including calls that return an error and calls started by the
// scheduler. If omitted, no observations are produced.
//
// The callback runs synchronously on the invoking goroutine and may be called
// concurrently for different calls. A forwarded call produces one observation
// on the initiating Runtime; the target Runtime does not produce a second
// observation, and the duration includes forwarding.
func OnCall(f func(CallObservation)) Option {
	return func(config *Config) {
		config.OnCall = f
	}
}

// WithMemberStore sets the membership store used to enable clustering. If
// omitted, the runtime operates without cluster membership; a MemberStore must
// be configured together with a Transport.
func WithMemberStore(value store.MemberStore) Option {
	return func(config *Config) {
		config.MemberStore = value
	}
}

// WithNodeAddr sets this node's address in cluster membership and ownership
// decisions. If omitted, the address is the empty string; the option has no
// effect when clustering is disabled.
func WithNodeAddr(value string) Option {
	return func(config *Config) {
		config.NodeAddr = value
	}
}

// WithGeneration sets this node's membership generation. If omitted, the
// generation is the empty string; the option has no effect when clustering is
// disabled.
func WithGeneration(value string) Option {
	return func(config *Config) {
		config.Generation = value
	}
}

// WithHeartbeatInterval sets the cluster heartbeat interval. If omitted, New
// uses one second; the option has no effect when clustering is disabled. When
// clustering is enabled, the value must be positive; a non-positive value makes
// New panic while creating the cluster ticker.
func WithHeartbeatInterval(value time.Duration) Option {
	return func(config *Config) {
		config.HeartbeatInterval = value
	}
}

// WithViewInterval sets how often the cluster membership view is refreshed. If
// omitted, New uses one second; the option has no effect when clustering is
// disabled. When clustering is enabled, the value must be positive; a
// non-positive value makes New panic while creating the cluster ticker.
func WithViewInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ViewInterval = value
	}
}

// WithProbeInterval sets the cluster probe interval. If omitted, the value is
// zero; a clustered New then returns an error because the interval must be
// positive. It has no effect when clustering is disabled.
func WithProbeInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ProbeInterval = value
	}
}

// WithProbeTimeout sets the deadline for a cluster probe. If omitted, the
// value is zero; a clustered New then returns an error because the timeout must
// be positive. It has no effect when clustering is disabled.
func WithProbeTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.ProbeTimeout = value
	}
}

// WithProbeFailures sets the number of failed probes required before a member
// is considered for a death vote. If omitted, the value is zero; a clustered
// New then returns an error because the value must be positive. It has no effect
// when clustering is disabled.
func WithProbeFailures(value int) Option {
	return func(config *Config) {
		config.ProbeFailures = value
	}
}

// WithVoteTTL sets how long a cluster suspect vote remains valid. If omitted,
// the value is zero; a clustered New then returns an error because the duration
// must be positive. It has no effect when clustering is disabled.
func WithVoteTTL(value time.Duration) Option {
	return func(config *Config) {
		config.VoteTTL = value
	}
}

// WithMaxTickGap sets the maximum allowed gap between healthy cluster ticks. If
// omitted, the value is zero; a clustered New then returns an error because the
// duration must be positive. It has no effect when clustering is disabled.
func WithMaxTickGap(value time.Duration) Option {
	return func(config *Config) {
		config.MaxTickGap = value
	}
}

// WithMaxTableLatency sets the maximum acceptable membership-store latency. If
// omitted, the value is zero; a clustered New then returns an error because the
// duration must be positive. It has no effect when clustering is disabled.
func WithMaxTableLatency(value time.Duration) Option {
	return func(config *Config) {
		config.MaxTableLatency = value
	}
}

// Invoke calls method for id, passing args and reply to the registered entity
// dispatch. Calls for the same identity are serialized; calls for different
// identities may run concurrently.
//
// For a local call, ctx limits waiting for activation and delivery and is
// passed to the entity method. For a remote owner, ctx limits the forwarding
// operation at the initiating Runtime; canceling it does not cancel the
// already forwarded entity call, which may continue on the remote Runtime.
// An identity with no current owner returns an error matching ErrNoOwner
// without being forwarded. A forwarded error with a Code is reconstructed so
// errors.Is can match that Code; errors from an opaque error retain only text.
// Caller cancellation and deadline errors are returned unchanged.
// Once the Runtime has begun stopping, new calls are rejected at the root
// admission gate with a stable stop error before ownership is decided or a
// call is forwarded: gor.runtime_closed after Close or Kill, and gor.node_dead
// once the cluster has declared this node dead.
func (rt *Runtime) Invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	if rt.onCall == nil {
		return publicError(rt.invoke(ctx, id, method, args, reply))
	}
	started := rt.clock.Now()
	err := publicError(rt.invoke(ctx, id, method, args, reply))
	rt.onCall(CallObservation{
		EntityType: id.Type,
		Method:     method,
		Duration:   rt.clock.Now().Sub(started),
		Err:        err,
	})
	return err
}

func (rt *Runtime) invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	release, err := rt.admit()
	if err != nil {
		return err
	}
	defer release()
	if rt.clusterNode == nil {
		return rt.invokeLocal(ctx, id, method, args, reply)
	}
	view := rt.clusterView.Load()
	owner, ok := cluster.Owner(*view, store.Identity(id))
	if !ok {
		return fmt.Errorf("%w: identity currently has no active owner", ErrNoOwner)
	}
	if owner != rt.nodeAddr {
		return rt.forward(ctx, owner, id, method, args, reply)
	}
	return rt.invokeLocal(ctx, id, method, args, reply)
}

func (rt *Runtime) invokeLocal(ctx context.Context, id Identity, method string, args any, reply any) error {
	return rt.engine.Invoke(ctx, id, method, args, reply)
}

// Owns reports whether this runtime currently owns id. It is an integration
// seam for scheduling and cluster plumbing; application code should normally
// invoke a reference or Runtime.Invoke and let routing choose the owner.
func (rt *Runtime) Owns(id store.Identity) bool {
	if rt.clusterNode == nil {
		return true
	}
	view := rt.clusterView.Load()
	owner, ok := cluster.Owner(*view, id)
	return ok && owner == rt.nodeAddr
}

type boundInstance struct {
	entity any
	binder *Binder
}

// InstallType installs the dispatch and proxy factories required for T in rt.
// Generated Install code calls it; application code should use the generated
// installer rather than hand-writing this integration seam. It returns an
// error when T is already installed in rt.
func InstallType[T any](rt *Runtime, dispatch func(context.Context, T, string, any, any) error, newProxy func(Invoker, Identity) T, newCall func(string) (any, any)) error {
	name := TypeName[T]()
	rt.typesMu.Lock()
	defer rt.typesMu.Unlock()
	if _, exists := rt.types[name]; exists {
		return fmt.Errorf("entity type %q is already installed", name)
	}
	rt.types[name] = typeRegistration{
		dispatch: func(ctx context.Context, instance any, method string, args any, reply any) error {
			return dispatch(ctx, instance.(T), method, args, reply)
		},
		newProxy: func(invoker Invoker, id Identity) any {
			return newProxy(invoker, id)
		},
		newCall: newCall,
	}
	return nil
}

// Register associates T with factory in rt. T must already be installed by
// generated code or InstallType; otherwise the returned error wraps
// ErrTypeNotInstalled. Register rejects a second registration of the same
// type in one runtime.
//
// factory is called to create each activation and receives a Binder for that
// activation's identity. Register itself does not create an activation.
func Register[T any](rt *Runtime, factory func(*Binder) T) error {
	name := TypeName[T]()
	registration, ok := rt.typeRegistration(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTypeNotInstalled, name)
	}
	return rt.engine.Register(name, runtimepkg.Registration{
		Factory: func(ctx context.Context, id runtimepkg.Identity) (any, error) {
			binder := newBinder(rt, id)
			entity := factory(binder)
			if err := binder.load(ctx); err != nil {
				return nil, err
			}
			if activatable, ok := any(entity).(Activatable); ok {
				if err := activatable.OnActivate(ctx); err != nil {
					return nil, err
				}
			}
			return boundInstance{entity: entity, binder: binder}, nil
		},
		Dispatch: func(ctx context.Context, instance any, method string, args any, reply any) error {
			bound := instance.(boundInstance)
			err := registration.dispatch(ctx, bound.entity, method, args, reply)
			if discard := bound.binder.discardError(); discard != nil {
				return runtimepkg.Discard{Err: errors.Join(err, discard)}
			}
			return err
		},
		OnDeactivate: func(ctx context.Context, id runtimepkg.Identity, instance any) {
			bound := instance.(boundInstance)
			deactivatable, ok := bound.entity.(Deactivatable)
			if !ok {
				return
			}
			err := deactivatable.OnDeactivate(ctx)
			if err != nil && rt.onError != nil {
				rt.onError(Identity(id), "OnDeactivate", err)
			}
		},
	})
}

// Now returns the current time from the clock configured for the entity bound
// to b.
func Now(b *Binder) time.Time {
	return b.runtime.clock.Now()
}

// Ref returns a typed reference to entity T identified by key. The type must
// already be installed in the runtime represented by scope; otherwise Ref
// panics with an ErrTypeNotInstalled message. Creating a reference does not
// activate the entity; activation begins when a method is invoked on it.
func Ref[T any](scope Scope, key string) T {
	rt := scope.scopeRuntime()
	name := TypeName[T]()
	registration, ok := rt.typeRegistration(name)
	if !ok {
		panic(fmt.Sprintf("%v: %s", ErrTypeNotInstalled, name))
	}
	return registration.newProxy(rt, Identity{Type: name, Key: key}).(T)
}

func (rt *Runtime) typeRegistration(name string) (typeRegistration, bool) {
	rt.typesMu.Lock()
	defer rt.typesMu.Unlock()
	registration, ok := rt.types[name]
	return registration, ok
}

// Close begins an orderly shutdown. It stops admitting new entity calls,
// lets calls already admitted finish, rejects queued calls without entering
// their method bodies, and then waits for in-flight methods, normal
// deactivation callbacks, and the runtime's own infrastructure goroutines
// before closing configured cluster and transport resources.
//
// Scheduled delivery, direct Invoke, and inbound forwarded invokes all pass
// through the same admission gate, so all three are rejected once Close has
// begun. Repeated Close or Kill calls are safe and do not start another
// shutdown; a Kill during Close escalates to immediate shutdown semantics.
func (rt *Runtime) Close() {
	rt.beginClose()
	rt.closeGracefully()
}

// Kill begins an immediate shutdown. It stops admitting new entity calls,
// cancels the contexts of calls already running, rejects queued work, and
// skips deactivation callbacks. Unlike Close, Kill does not wait for user
// methods to return: Go cannot forcibly stop code that ignores cancellation.
// It is safe to call repeatedly; a Kill during Close escalates immediately.
func (rt *Runtime) Kill() {
	rt.beginKill()
	rt.closeImmediately()
}

func (rt *Runtime) closeGracefully() {
	if rt.poller != nil {
		rt.poller.Close()
	}
	if rt.clusterNode != nil {
		rt.clusterNode.Close()
		<-rt.clusterDone
	}
	rt.engine.Close()
	rt.closeTransport()
	rt.waitDrained()
	rt.finishStop()
}

func (rt *Runtime) closeImmediately() {
	if rt.poller != nil {
		rt.poller.Close()
	}
	if rt.clusterNode != nil {
		rt.clusterNode.Kill()
		<-rt.clusterDone
	}
	rt.engine.Kill()
	rt.closeTransport()
	rt.finishStop()
}

// Activations returns a sorted snapshot of this runtime's active entities.
func (rt *Runtime) Activations() []Activation {
	return rt.engine.Activations()
}

// Done returns a channel that is closed when shutdown begins and the runtime
// stops accepting forwarded requests. It may close before Close or Kill has
// finished waiting for invocations, deactivation callbacks, or resources.
// It also closes when a clustered runtime's node is declared dead.
func (rt *Runtime) Done() <-chan struct{} {
	return rt.done
}

// rootState is the root runtime lifecycle. The root owns call admission and
// the stop reason; the inner execution runtime owns activations and mailboxes,
// the cluster node owns membership, and the poller and transport are not stop
// switches of their own.
type rootState uint8

const (
	rootRunning rootState = iota
	rootClosing
	rootKilling
	rootDead
	rootStopped
)

// beginClose transitions running to closing. It is the linearization point for
// call admission: admit starts rejecting with ErrRuntimeClosed and the public
// stop signal closes. A runtime that already left running is left unchanged.
func (rt *Runtime) beginClose() bool {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.state != rootRunning {
		return false
	}
	rt.leaveRunning(rootClosing, ErrRuntimeClosed)
	return true
}

// beginKill transitions running or closing to killing. From running it is the
// admission linearization point; from closing it is an escalation, not a no-op.
func (rt *Runtime) beginKill() bool {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	switch rt.state {
	case rootRunning:
		rt.leaveRunning(rootKilling, ErrRuntimeClosed)
		return true
	case rootClosing:
		rt.state = rootKilling
		return true
	default:
		return false
	}
}

// becomeDead transitions a still-running root to dead after the cluster
// declared this node dead. It is the admission linearization point with stop
// code ErrNodeDead. A root already in closing, killing, or dead is unchanged.
func (rt *Runtime) becomeDead() bool {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.state != rootRunning {
		return false
	}
	rt.leaveRunning(rootDead, ErrNodeDead)
	return true
}

// finishStop transitions closing, killing, or dead to the terminal stopped
// state after the runtime's infrastructure has exited.
func (rt *Runtime) finishStop() {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	switch rt.state {
	case rootClosing, rootKilling, rootDead:
		rt.state = rootStopped
	}
}

// leaveRunning performs the atomic admission transition out of running. It
// records the stop code, arms the drain channel that release closes once the
// last admitted call finishes, and closes the public stop signal. The caller
// must hold lifecycleMu and state must be running.
func (rt *Runtime) leaveRunning(next rootState, code Code) {
	rt.state = next
	rt.stopCode = code
	rt.drained = make(chan struct{})
	if rt.inflight == 0 {
		close(rt.drained)
	}
	close(rt.done)
}

// admit registers one entity call against the root lifecycle. It returns a
// release function when the call may proceed, or a stop error otherwise. It is
// the single admission gate shared by public Invoke, the inbound invoke
// handler, and scheduled delivery; the ownership decision and forwarding happen
// only after a successful admit.
func (rt *Runtime) admit() (func(), error) {
	rt.lifecycleMu.Lock()
	if rt.state == rootRunning {
		rt.inflight++
		rt.lifecycleMu.Unlock()
		return rt.release, nil
	}
	code := rt.stopCode
	rt.lifecycleMu.Unlock()
	return nil, stopRejection(code)
}

func (rt *Runtime) release() {
	rt.lifecycleMu.Lock()
	rt.inflight--
	if rt.inflight == 0 && rt.state != rootRunning && rt.drained != nil {
		close(rt.drained)
		rt.drained = nil
	}
	rt.lifecycleMu.Unlock()
}

// waitDrained blocks until every call admitted before the stop transition has
// released. Calls admitted after the transition are impossible.
func (rt *Runtime) waitDrained() {
	rt.lifecycleMu.Lock()
	drained := rt.drained
	rt.lifecycleMu.Unlock()
	if drained != nil {
		<-drained
	}
}

func (rt *Runtime) stopCodeSnapshot() Code {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	return rt.stopCode
}

func stopRejection(code Code) error {
	if code == ErrNodeDead {
		return withCode(ErrNodeDead, errors.New("node declared dead"))
	}
	return withCode(ErrRuntimeClosed, errors.New("runtime is not accepting calls"))
}

func (rt *Runtime) startTransport() {
	serveContext, stopServe := context.WithCancel(context.Background())
	rt.transportStop = stopServe
	rt.transportDone = make(chan struct{})
	go func() {
		defer close(rt.transportDone)
		_ = rt.transport.Serve(serveContext, rt.handle)
	}()
}

func (rt *Runtime) closeTransport() {
	if rt.transport == nil {
		return
	}
	if rt.transportClosing.CompareAndSwap(false, true) {
		if rt.transportStop != nil {
			rt.transportStop()
		}
		_ = rt.transport.Close()
	}
	if rt.transportDone != nil {
		<-rt.transportDone
	}
}

func (rt *Runtime) watchCluster() {
	defer close(rt.clusterDone)
	for view := range rt.clusterNode.ViewChanges() {
		rt.clusterView.Store(&view)
		rt.deactivateMovedActivations(view)
	}
	// The cluster node finished. becomeDead is atomic: it returns true only when
	// this call moved a still-running root to dead. If Close or Kill already left
	// running, it returns false and the voluntary stop drives its own teardown —
	// a graceful close must not be upgraded to a sudden stop here.
	if !rt.becomeDead() {
		return
	}
	if rt.poller != nil {
		rt.poller.Close()
	}
	rt.engine.Kill()
}

func (rt *Runtime) deactivateMovedActivations(view cluster.View) {
	for _, id := range rt.engine.Identities() {
		owner, ok := cluster.Owner(view, store.Identity(id))
		if !ok || owner != rt.nodeAddr {
			rt.engine.Deactivate(runtimepkg.Identity(id))
		}
	}
}

type scheduleInvoker struct {
	runtime *Runtime
}

func (i scheduleInvoker) Invoke(ctx context.Context, id store.Identity, method string) error {
	err := i.runtime.Invoke(ctx, Identity(id), method, nil, nil)
	if err != nil && ctx.Err() == nil && i.runtime.onError != nil {
		i.runtime.onError(Identity(id), method, err)
	}
	return err
}

func (i scheduleInvoker) Owns(id store.Identity) bool {
	return i.runtime.Owns(id)
}

// TypeName returns the registered name used for T by the generated runtime
// glue. Application code should use this helper rather than constructing type
// names by hand.
func TypeName[T any]() string {
	return strings.TrimPrefix(fmt.Sprintf("%T", (*T)(nil)), "*")
}
