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

type WrongOwnerError struct {
	Owner string
}

func (e WrongOwnerError) Error() string {
	return fmt.Sprintf("identity belongs to node %q", e.Owner)
}

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

func Now(b *Binder) time.Time {
	return b.runtime.clock.Now()
}

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
