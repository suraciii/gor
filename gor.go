package gor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/suraciii/gor/clock"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/timer"
)

var ErrTypeNotInstalled = errors.New("entity type is not installed; call InstallType or run the generated Install")

type Identity = runtimepkg.Identity
type Runtime struct {
	*runtimepkg.Runtime
	store         store.Store
	scheduleStore store.ScheduleStore
	clock         clock.Clock
	poller        *timer.Poller
	typesMu       sync.Mutex
	types         map[string]typeRegistration
}

type Config struct {
	runtimepkg.Config
	Store            store.Store
	ScheduleStore    store.ScheduleStore
	ScheduleInterval time.Duration
}

type Invoker interface {
	Invoke(context.Context, Identity, string, []any, any) error
}

var _ Invoker = (*Runtime)(nil)

type typeRegistration struct {
	dispatch runtimepkg.Dispatch
	newProxy func(Invoker, Identity) any
}

type Option func(*Config)

func New(options ...Option) *Runtime {
	config := Config{
		Config: runtimepkg.Config{
			Clock:            clock.Real{},
			MailboxCapacity:  16,
			Locator:          runtimepkg.LocalLocator{},
			IdleTimeout:      time.Minute,
			EvictionInterval: time.Second,
		},
		Store:            store.NewMemory(),
		ScheduleInterval: time.Second,
	}
	for _, option := range options {
		option(&config)
	}
	if config.ScheduleStore == nil {
		if schedules, ok := config.Store.(store.ScheduleStore); ok {
			config.ScheduleStore = schedules
		}
	}
	rt := &Runtime{
		Runtime:       runtimepkg.New(config.Config),
		store:         config.Store,
		scheduleStore: config.ScheduleStore,
		clock:         config.Clock,
		types:         make(map[string]typeRegistration),
	}
	if config.ScheduleStore != nil && config.ScheduleInterval > 0 {
		rt.poller = timer.New(config.ScheduleStore, config.Clock, config.ScheduleInterval, scheduleInvoker{runtime: rt})
	}
	return rt
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

type boundInstance struct {
	entity any
	binder *Binder
}

func InstallType[T any](rt *Runtime, dispatch func(context.Context, T, string, []any, any) error, newProxy func(Invoker, Identity) T) error {
	name := TypeName[T]()
	rt.typesMu.Lock()
	defer rt.typesMu.Unlock()
	if _, exists := rt.types[name]; exists {
		return fmt.Errorf("entity type %q is already installed", name)
	}
	rt.types[name] = typeRegistration{
		dispatch: func(ctx context.Context, instance any, method string, args []any, reply any) error {
			return dispatch(ctx, instance.(T), method, args, reply)
		},
		newProxy: func(invoker Invoker, id Identity) any {
			return newProxy(invoker, id)
		},
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
			binder := newBinder(id, rt.store, rt.scheduleStore, rt.clock)
			entity := factory(binder)
			if err := binder.load(ctx); err != nil {
				return nil, err
			}
			return boundInstance{entity: entity, binder: binder}, nil
		},
		Dispatch: func(ctx context.Context, instance any, method string, args []any, reply any) error {
			bound := instance.(boundInstance)
			err := registration.dispatch(ctx, bound.entity, method, args, reply)
			if discard := bound.binder.discardError(); discard != nil {
				return runtimepkg.Discard{Err: err}
			}
			return err
		},
	})
}

func Ref[T any](rt *Runtime, key string) T {
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
	if rt.poller != nil {
		rt.poller.Close()
	}
	rt.Runtime.Close()
}

func (rt *Runtime) Kill() {
	if rt.poller != nil {
		rt.poller.Close()
	}
	rt.Runtime.Kill()
}

type scheduleInvoker struct {
	runtime *Runtime
}

func (i scheduleInvoker) Invoke(ctx context.Context, id store.Identity, method string) error {
	return i.runtime.Invoke(ctx, Identity(id), method, nil, nil)
}

func TypeName[T any]() string {
	return strings.TrimPrefix(fmt.Sprintf("%T", (*T)(nil)), "*")
}
