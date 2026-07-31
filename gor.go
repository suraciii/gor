package gor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/suraciii/gor/clock"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
)

type Identity = runtimepkg.Identity
type Runtime struct {
	*runtimepkg.Runtime
	store store.Store
}

type Config struct {
	runtimepkg.Config
	Store store.Store
}

type Invoker interface {
	Invoke(context.Context, Identity, string, []any, any) error
}

var _ Invoker = (*Runtime)(nil)

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
		Store: store.NewMemory(),
	}
	for _, option := range options {
		option(&config)
	}
	return &Runtime{
		Runtime: runtimepkg.New(config.Config),
		store:   config.Store,
	}
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

type boundInstance struct {
	entity any
	binder *Binder
}

func Register[T any](rt *Runtime, factory func(*Binder) T, dispatch func(context.Context, T, string, []any, any) error) error {
	return rt.Runtime.Register(TypeName[T](), runtimepkg.Registration{
		Factory: func(ctx context.Context, id runtimepkg.Identity) (any, error) {
			binder := newBinder(id, rt.store)
			entity := factory(binder)
			if err := binder.load(ctx); err != nil {
				return nil, err
			}
			return boundInstance{entity: entity, binder: binder}, nil
		},
		Dispatch: func(ctx context.Context, instance any, method string, args []any, reply any) error {
			bound := instance.(boundInstance)
			err := dispatch(ctx, bound.entity.(T), method, args, reply)
			if discard := bound.binder.discardError(); discard != nil {
				return runtimepkg.Discard{Err: err}
			}
			return err
		},
	})
}

func TypeName[T any]() string {
	return strings.TrimPrefix(fmt.Sprintf("%T", (*T)(nil)), "*")
}
