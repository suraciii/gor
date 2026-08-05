package gor

import (
	"context"
	"errors"

	"github.com/suraciii/gor/cluster"
	"github.com/suraciii/gor/mail"
	runtimepkg "github.com/suraciii/gor/runtime"
	"github.com/suraciii/gor/store"
)

type Code string

func (c Code) Error() string {
	return string(c)
}

func (c Code) Code() Code {
	return c
}

func (c Code) Is(target error) bool {
	other, ok := target.(Code)
	return ok && c == other
}

type Coded interface {
	Code() Code
}

func CodeOf(err error) (Code, bool) {
	for err != nil {
		if coded, ok := err.(Coded); ok {
			return coded.Code(), true
		}
		err = errors.Unwrap(err)
	}
	return "", false
}

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
	default:
		return err
	}
}
