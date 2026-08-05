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

var ErrTypeNotInstalled = errors.New("entity type is not installed; call InstallType or run the generated Install")

// Identity identifies an entity by its registered type name and key.
type Identity = runtimepkg.Identity
type Activation = runtimepkg.Activation

type CallObservation struct {
	EntityType string
	Method     string
	Duration   time.Duration
	Err        error
}

type Scope interface {
	scopeRuntime() *Runtime
}

type Activatable interface {
	OnActivate(context.Context) error
}

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
	stopOnce         sync.Once
	shuttingDown     atomic.Bool
	nodeAddr         string
	typesMu          sync.Mutex
	types            map[string]typeRegistration
}

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

type Invoker interface {
	Invoke(context.Context, Identity, string, any, any) error
}

var _ Invoker = (*Runtime)(nil)

type typeRegistration struct {
	dispatch runtimepkg.Dispatch
	newProxy func(Invoker, Identity) any
	newCall  func(string) (any, any)
}

type Option func(*Config)

func (rt *Runtime) scopeRuntime() *Runtime {
	return rt
}

func (b *Binder) scopeRuntime() *Runtime {
	return b.runtime
}

// New creates and starts a Runtime.
//
// With no options, New uses a real clock, an in-memory state and schedule
// store, a mailbox capacity of 16, a one-minute idle timeout, one-second
// eviction and schedule intervals, and one-second heartbeat and view
// intervals. A MemberStore and Transport must be configured together;
// configuring only one returns an error.
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
		go func() {
			<-clusterNode.Done()
			rt.stopServing()
		}()
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

func WithClock(value clock.Clock) Option {
	return func(config *Config) {
		config.Clock = value
	}
}

func WithMailboxCapacity(value int) Option {
	return func(config *Config) {
		config.MailboxCapacity = value
	}
}

func WithIdleTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.IdleTimeout = value
	}
}

func WithEvictionInterval(value time.Duration) Option {
	return func(config *Config) {
		config.EvictionInterval = value
	}
}

func WithStore(value store.Store) Option {
	return func(config *Config) {
		config.Store = value
	}
}

func WithScheduleStore(value store.ScheduleStore) Option {
	return func(config *Config) {
		config.ScheduleStore = value
	}
}

func WithScheduleInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ScheduleInterval = value
	}
}

func WithTransport(value transport.Transport) Option {
	return func(config *Config) {
		config.Transport = value
	}
}

func OnError(f func(id Identity, method string, err error)) Option {
	return func(config *Config) {
		config.OnError = f
	}
}

func OnCall(f func(CallObservation)) Option {
	return func(config *Config) {
		config.OnCall = f
	}
}

func WithMemberStore(value store.MemberStore) Option {
	return func(config *Config) {
		config.MemberStore = value
	}
}

func WithNodeAddr(value string) Option {
	return func(config *Config) {
		config.NodeAddr = value
	}
}

func WithGeneration(value string) Option {
	return func(config *Config) {
		config.Generation = value
	}
}

func WithHeartbeatInterval(value time.Duration) Option {
	return func(config *Config) {
		config.HeartbeatInterval = value
	}
}

func WithViewInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ViewInterval = value
	}
}

func WithProbeInterval(value time.Duration) Option {
	return func(config *Config) {
		config.ProbeInterval = value
	}
}

func WithProbeTimeout(value time.Duration) Option {
	return func(config *Config) {
		config.ProbeTimeout = value
	}
}

func WithProbeFailures(value int) Option {
	return func(config *Config) {
		config.ProbeFailures = value
	}
}

func WithVoteTTL(value time.Duration) Option {
	return func(config *Config) {
		config.VoteTTL = value
	}
}

func WithMaxTickGap(value time.Duration) Option {
	return func(config *Config) {
		config.MaxTickGap = value
	}
}

func WithMaxTableLatency(value time.Duration) Option {
	return func(config *Config) {
		config.MaxTableLatency = value
	}
}

// WrongOwnerError reports that a clustered Runtime has no current owner for
// an identity. Invoke returns this error without forwarding a request when the
// current view has no active owner.
//
// Owner identifies the owner associated with the error. It is empty when no
// owner is available.
type WrongOwnerError struct {
	Owner string
}

// Error returns a description of the unavailable owner.
func (e WrongOwnerError) Error() string {
	return fmt.Sprintf("identity belongs to node %q", e.Owner)
}

// Invoke calls method for id, passing args and reply to the registered entity
// dispatch. Calls for the same identity are serialized; calls for different
// identities may run concurrently.
//
// ctx limits waiting for activation and delivery and is passed to the entity
// call. In a clustered runtime, an invocation for a remote owner is forwarded;
// an identity with no current owner returns WrongOwnerError without being
// forwarded. Errors from registration, activation, dispatch, context
// cancellation, or forwarding are returned to the caller.
//
// After the runtime has shut down, Invoke does not start a new entity call.
func (rt *Runtime) Invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	if rt.onCall == nil {
		return rt.invoke(ctx, id, method, args, reply)
	}
	started := rt.clock.Now()
	err := rt.invoke(ctx, id, method, args, reply)
	rt.onCall(CallObservation{
		EntityType: id.Type,
		Method:     method,
		Duration:   rt.clock.Now().Sub(started),
		Err:        err,
	})
	return err
}

func (rt *Runtime) invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	if rt.clusterNode == nil {
		return rt.engine.Invoke(ctx, id, method, args, reply)
	}
	view := rt.clusterView.Load()
	owner, ok := cluster.Owner(*view, store.Identity(id))
	if !ok {
		return WrongOwnerError{Owner: owner}
	}
	if owner != rt.nodeAddr {
		return rt.forward(ctx, owner, id, method, args, reply)
	}
	return rt.engine.Invoke(ctx, id, method, args, reply)
}

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

// Ref returns a typed reference to T with key as its entity key. The type must
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

// Close begins an orderly shutdown. It stops scheduling and serving new work,
// waits for in-flight invocations and normal deactivation callbacks to finish,
// and closes configured cluster and transport resources.
//
// Calls already in progress are allowed to finish. Unlike Kill, Close does
// not cancel their contexts or skip deactivation callbacks.
func (rt *Runtime) Close() {
	rt.shuttingDown.Store(true)
	rt.stopServing()
	if rt.poller != nil {
		rt.poller.Close()
	}
	if rt.clusterNode != nil {
		rt.clusterNode.Close()
		<-rt.clusterDone
	}
	rt.engine.Close()
	rt.closeTransport()
}

// Kill begins an immediate shutdown. It cancels the contexts of running and
// queued invocations, rejects queued work, and skips deactivation callbacks.
// Unlike Close, Kill does not wait for deactivation callbacks to finish.
func (rt *Runtime) Kill() {
	rt.shuttingDown.Store(true)
	rt.stopServing()
	if rt.poller != nil {
		rt.poller.Close()
	}
	if rt.clusterNode != nil {
		rt.clusterNode.Kill()
		<-rt.clusterDone
	}
	rt.engine.Kill()
	rt.closeTransport()
}

// Activations returns a sorted snapshot of this runtime's active entities.
func (rt *Runtime) Activations() []Activation {
    return rt.engine.Activations()
}

// Done returns a channel that is closed when the runtime stops serving
// invocations. It closes after Close or Kill and when a clustered runtime's
// node is declared dead.
func (rt *Runtime) Done() <-chan struct{} {
	return rt.done
}

func (rt *Runtime) stopServing() {
	rt.stopOnce.Do(func() {
		close(rt.done)
	})
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
	defer func() {
		close(rt.clusterDone)
	}()
	for view := range rt.clusterNode.ViewChanges() {
		rt.clusterView.Store(&view)
		rt.deactivateMovedActivations(view)
	}
	rt.stopServing()
	if rt.shuttingDown.Load() {
		return
	}
	if rt.poller != nil {
		rt.poller.Close()
	}
	rt.engine.Close()
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

func TypeName[T any]() string {
	return strings.TrimPrefix(fmt.Sprintf("%T", (*T)(nil)), "*")
}
