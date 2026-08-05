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
)

var ErrTypeNotInstalled = errors.New("entity type is not installed; call InstallType or run the generated Install")

type Identity = runtimepkg.Identity

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
	*runtimepkg.Runtime
	store         store.Store
	scheduleStore store.ScheduleStore
	clock         clock.Clock
	onError       func(Identity, string, error)
	poller        *timer.Poller
	clusterNode   *cluster.Node
	clusterView   atomic.Pointer[cluster.View]
	clusterDone   chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	shuttingDown  atomic.Bool
	nodeAddr      string
	typesMu       sync.Mutex
	types         map[string]typeRegistration
}

type Config struct {
	runtimepkg.Config
	Store             store.Store
	ScheduleStore     store.ScheduleStore
	ScheduleInterval  time.Duration
	OnError           func(Identity, string, error)
	MemberStore       store.MemberStore
	NodeAddr          string
	Generation        string
	HeartbeatInterval time.Duration
	ViewInterval      time.Duration
	DeadAfter         time.Duration
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
			Locator:          runtimepkg.LocalLocator{},
			IdleTimeout:      time.Minute,
			EvictionInterval: time.Second,
		},
		Store:             store.NewMemory(),
		ScheduleInterval:  time.Second,
		HeartbeatInterval: time.Second,
		ViewInterval:      time.Second,
		DeadAfter:         3 * time.Second,
	}
	for _, option := range options {
		option(&config)
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
			NodeAddr:          config.NodeAddr,
			Generation:        config.Generation,
			HeartbeatInterval: config.HeartbeatInterval,
			ViewInterval:      config.ViewInterval,
			DeadAfter:         config.DeadAfter,
		})
		if err != nil {
			return nil, fmt.Errorf("start cluster node: %w", err)
		}
		initialView = <-clusterNode.ViewChanges()
	}
	rt := &Runtime{
		Runtime:       runtimepkg.New(config.Config),
		store:         config.Store,
		scheduleStore: config.ScheduleStore,
		clock:         config.Clock,
		onError:       config.OnError,
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

func WithLocator(value runtimepkg.Locator) Option {
	return func(config *Config) {
		config.Locator = value
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

func OnError(f func(id Identity, method string, err error)) Option {
	return func(config *Config) {
		config.OnError = f
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

func WithDeadAfter(value time.Duration) Option {
	return func(config *Config) {
		config.DeadAfter = value
	}
}

type WrongOwnerError struct {
	Owner string
}

func (e WrongOwnerError) Error() string {
	return fmt.Sprintf("identity belongs to node %q", e.Owner)
}

func (rt *Runtime) Invoke(ctx context.Context, id Identity, method string, args any, reply any) error {
	if rt.clusterNode == nil {
		return rt.Runtime.Invoke(ctx, id, method, args, reply)
	}
	view := rt.clusterView.Load()
	owner, _ := cluster.Owner(*view, store.Identity(id))
	if owner != rt.nodeAddr {
		return WrongOwnerError{Owner: owner}
	}
	return rt.Runtime.Invoke(ctx, id, method, args, reply)
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
	return rt.Runtime.Register(name, runtimepkg.Registration{
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
	rt.Runtime.Close()
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
	rt.Runtime.Kill()
}

func (rt *Runtime) Done() <-chan struct{} {
	return rt.done
}

func (rt *Runtime) stopServing() {
	rt.stopOnce.Do(func() {
		close(rt.done)
	})
}

func (rt *Runtime) watchCluster() {
	defer func() {
		rt.stopServing()
		close(rt.clusterDone)
	}()
	for view := range rt.clusterNode.ViewChanges() {
		rt.clusterView.Store(&view)
		rt.deactivateMovedActivations(view)
	}
	if rt.shuttingDown.Load() {
		return
	}
	if rt.poller != nil {
		rt.poller.Close()
	}
	rt.Runtime.Close()
}

func (rt *Runtime) deactivateMovedActivations(view cluster.View) {
	for _, id := range rt.Runtime.Identities() {
		owner, ok := cluster.Owner(view, store.Identity(id))
		if !ok || owner != rt.nodeAddr {
			rt.Runtime.Deactivate(runtimepkg.Identity(id))
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
