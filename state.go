package gor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/suraciii/gor/store"
)

// Binder is the runtime-bound context passed to an entity factory. Entity code
// uses the Binder to create state, schedules, and references; application code
// should use the supplied Binder rather than construct one.
type Binder struct {
	runtime  *Runtime
	identity store.GrainId
	etag     store.ETag
	states   map[string]stateCell
	discard  error
}

type stateCell interface {
	encode() ([]byte, error)
	decode([]byte) error
	isPresent() bool
}

func newBinder(runtime *Runtime, id GrainId) *Binder {
	return &Binder{
		runtime:  runtime,
		identity: store.GrainId{GrainType: id.GrainType, GrainKey: id.GrainKey},
		states:   make(map[string]stateCell),
	}
}

// Self returns the identity of the entity bound to b.
func Self(b *Binder) GrainId {
	return GrainId{GrainType: b.identity.GrainType, GrainKey: b.identity.GrainKey}
}

// NewState registers a named persistent value for the entity bound to b and
// returns its handle. The name must be unique within that Grain; registering
// the same name twice panics. A newly registered State is absent and has its
// type's zero value until activation data is loaded or Set succeeds.
func NewState[T any](b *Binder, name string) State[T] {
	if _, exists := b.states[name]; exists {
		panic(fmt.Sprintf("state %q is already registered", name))
	}
	cell := &stateCellValue[T]{binder: b}
	b.states[name] = cell
	return State[T]{cell: cell}
}

// State is a handle to one named JSON-encoded value in a Grain's persistent
// state record. All State handles for one Grain share one JSON object, one
// store record, and one ETag; setting or clearing one handle rewrites the
// complete record. Obtain a State with NewState; the zero value is not usable.
type State[T any] struct {
	cell *stateCellValue[T]
}

// Get returns the current in-memory value without reading from the store or
// making a copy. If T is a map or slice, the returned value aliases the state;
// mutating it changes in-memory state but does not persist the change. Call Set
// to encode and persist a mutated value.
func (s State[T]) Get() T {
	return s.cell.value
}

// Exists reports whether this named State has a confirmed value. It does not
// compare the value with T's zero value.
func (s State[T]) Exists() bool {
	return s.cell.isPresent()
}

// Clear removes this named State from the confirmed record and resets Get to
// T's zero value after the write succeeds. The write uses the current Grain
// ETag and has the same conflict and store failure behavior as Set.
func (s State[T]) Clear(ctx context.Context) error {
	if err := s.cell.binder.persist(ctx, s.cell, nil, false); err != nil {
		return err
	}
	var zero T
	s.cell.value = zero
	s.cell.present = false
	return nil
}

// Set JSON-encodes value and persists the Grain's complete state record using
// ctx. A JSON encoding error for value or another registered state leaves the
// current value unchanged and is returned without a store write. Store errors
// leave the current in-memory value unchanged, but do not establish whether the
// store wrote the record; callers must not assume the write failed or retry
// unconditionally. Store errors are returned as well; in particular,
// errors.Is(err, store.ErrConflict) and errors.Is(err, ErrPersistenceConflict)
// report an ETag conflict.
// A store write failure also discards the current entity activation after the
// containing call completes, so the next call creates a fresh activation.
// On success, subsequent Get calls return value.
func (s State[T]) Set(ctx context.Context, value T) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := s.cell.binder.persist(ctx, s.cell, encoded, true); err != nil {
		return err
	}
	s.cell.value = value
	s.cell.present = true
	return nil
}

type stateCellValue[T any] struct {
	binder  *Binder
	value   T
	present bool
}

func (s *stateCellValue[T]) encode() ([]byte, error) {
	return json.Marshal(s.value)
}

func (s *stateCellValue[T]) decode(data []byte) error {
	if err := json.Unmarshal(data, &s.value); err != nil {
		return err
	}
	s.present = true
	return nil
}

func (s *stateCellValue[T]) isPresent() bool {
	return s.present
}

func (b *Binder) load(ctx context.Context) error {
	record, err := b.runtime.store.Read(ctx, b.identity)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return withCode(ErrPersistenceFailed, err)
	}
	if len(record.Data) == 0 {
		b.etag = record.ETag
		return nil
	}

	var document map[string]json.RawMessage
	if err := json.Unmarshal(record.Data, &document); err != nil {
		return err
	}
	for name, cell := range b.states {
		data, ok := document[name]
		if !ok {
			continue
		}
		if err := cell.decode(data); err != nil {
			return err
		}
	}
	b.etag = record.ETag
	return nil
}

func (b *Binder) persist(ctx context.Context, changed stateCell, changedData []byte, changedPresent bool) error {
	document := make(map[string]json.RawMessage, len(b.states))
	for name, cell := range b.states {
		if cell == changed {
			if !changedPresent {
				continue
			}
			document[name] = json.RawMessage(changedData)
			continue
		}
		if !cell.isPresent() {
			continue
		}
		data, err := cell.encode()
		if err != nil {
			return err
		}
		document[name] = json.RawMessage(data)
	}

	data, err := json.Marshal(document)
	if err != nil {
		return err
	}
	etag, err := b.runtime.store.Write(ctx, b.identity, data, b.etag)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, store.ErrConflict) {
			b.discard = withCode(ErrPersistenceConflict, err)
		} else {
			b.discard = withCode(ErrPersistenceFailed, err)
		}
		return b.discard
	}
	b.etag = etag
	return nil
}

func (b *Binder) discardError() error {
	return b.discard
}
