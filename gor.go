package gor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/suraciii/gor/clock"
	runtimepkg "github.com/suraciii/gor/runtime"
)

type Identity = runtimepkg.Identity
type Runtime = runtimepkg.Runtime
type Config = runtimepkg.Config
type Invoker interface {
	Invoke(context.Context, Identity, string, []any, any) error
}

var _ Invoker = (*Runtime)(nil)

type Option func(*Config)

func New(options ...Option) *Runtime {
	config := Config{
		Clock:            clock.Real{},
		MailboxCapacity:  16,
		Locator:          runtimepkg.LocalLocator{},
		IdleTimeout:      time.Minute,
		EvictionInterval: time.Second,
	}
	for _, option := range options {
		option(&config)
	}
	return runtimepkg.New(config)
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

func Register[T any](rt *Runtime, factory func() T, dispatch func(context.Context, T, string, []any, any) error) error {
	return rt.Register(TypeName[T](), runtimepkg.Registration{
		Factory: func() any {
			return factory()
		},
		Dispatch: func(ctx context.Context, instance any, method string, args []any, reply any) error {
			return dispatch(ctx, instance.(T), method, args, reply)
		},
	})
}

func TypeName[T any]() string {
	return strings.TrimPrefix(fmt.Sprintf("%T", (*T)(nil)), "*")
}
