package gor

import (
	"context"
	"errors"

	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/mail"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
)

// Code is the stable identity of an application or gor framework error.
// A Code is also an error and can be matched with errors.Is.
type Code string

// Error returns the code's diagnostic text.
func (c Code) Error() string {
	return string(c)
}

// Code returns c.
func (c Code) Code() Code {
	return c
}

// Is reports whether target is the same Code as c.
func (c Code) Is(target error) bool {
	other, ok := target.(Code)
	return ok && c == other
}

// Coded exposes the stable Code carried by an error.
type Coded interface {
	Code() Code
}

// CodeOf reports the single Code reachable from err. It traverses err the way
// errors.Is does—the error itself, the single-value Unwrap chain, and every
// branch of a multi-value Unwrap—collecting every reachable Code. Exactly one
// reachable Code is the error's determined Code; none, or more than one, means
// the error has no determined Code and crosses the network as opaque text. As
// with errors.Is, a cyclic Unwrap is not handled.
func CodeOf(err error) (Code, bool) {
	var (
		codes map[Code]struct{}
		walk  func(error)
	)
	walk = func(e error) {
		if e == nil {
			return
		}
		if coded, ok := e.(Coded); ok {
			if codes == nil {
				codes = map[Code]struct{}{}
			}
			codes[coded.Code()] = struct{}{}
		}
		switch u := e.(type) {
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		case interface{ Unwrap() []error }:
			for _, sub := range u.Unwrap() {
				walk(sub)
			}
		}
	}
	walk(err)
	if len(codes) != 1 {
		return "", false
	}
	var sole Code
	for c := range codes {
		sole = c
	}
	return sole, true
}

// The framework Code values are a closed set. Applications must declare codes
// under an owner other than gor.
const (
	ErrNoOwner             Code = "gor.no_owner"
	ErrNodeDead            Code = "gor.node_dead"
	ErrRuntimeClosed       Code = "gor.runtime_closed"
	ErrOverloaded          Code = "gor.overloaded"
	ErrTypeNotInstalled    Code = "gor.type_not_installed"
	ErrUnknownMethod       Code = "gor.unknown_method"
	ErrInvalidRequest      Code = "gor.invalid_request"
	ErrPersistenceConflict Code = "gor.persistence_conflict"
	ErrPersistenceFailed   Code = "gor.persistence_failed"
	ErrPanic               Code = "gor.panic"
	ErrRequestEncodeFailed Code = "gor.request_encode_failed"
	ErrReplyEncodeFailed   Code = "gor.reply_encode_failed"
	ErrTransportFailed     Code = "gor.transport_failed"
	// ErrCallCycle reports that a call targeted an entity that the same call
	// chain already occupies, so the call could never start. The error text
	// names the entities in the cycle. The cycle is detected at delivery, not
	// inferred from elapsed time: a slow call that is not a cycle still times
	// out as a plain timeout.
	ErrCallCycle Code = "gor.call_cycle"
)

type codedError struct {
	code Code
	err  error
}

func (e codedError) Error() string {
	if e.err == nil {
		return e.code.Error()
	}
	return e.err.Error()
}

func (e codedError) Code() Code {
	return e.code
}

func (e codedError) Is(target error) bool {
	other, ok := target.(Code)
	return ok && e.code == other
}

func (e codedError) Unwrap() error {
	return e.err
}

func withCode(code Code, err error) error {
	return codedError{code: code, err: err}
}

func publicError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := CodeOf(err); ok {
		return err
	}
	switch {
	case errors.Is(err, cluster.ErrNodeDead):
		return withCode(ErrNodeDead, err)
	case errors.Is(err, runtimepkg.ErrRuntimeClosed), errors.Is(err, mail.ErrClosed):
		return withCode(ErrRuntimeClosed, err)
	case errors.Is(err, mail.ErrOverloaded):
		return withCode(ErrOverloaded, err)
	case errors.Is(err, runtimepkg.ErrTypeNotRegistered):
		return withCode(ErrTypeNotInstalled, err)
	case errors.Is(err, store.ErrConflict):
		return withCode(ErrPersistenceConflict, err)
	case errors.Is(err, runtimepkg.ErrPanic):
		return withCode(ErrPanic, err)
	case errors.Is(err, runtimepkg.ErrCallCycle):
		return withCode(ErrCallCycle, err)
	default:
		return err
	}
}
