package gor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/suraciii/gor/store"
)

// Binder is the runtime-bound context passed to an entity factory. Entity code
// uses the Binder to create state, schedules, and references; application code
// should use the supplied Binder rather than construct one.
type Binder struct {
	runtime  *Runtime
	identity store.Identity
	etag     store.ETag
	states   map[string]stateCell
	discard  error
}

type stateCell interface {
	encode() ([]byte, error)
	decode([]byte) error
}

func newBinder(runtime *Runtime, id Identity) *Binder {
	return &Binder{
		runtime:  runtime,
		identity: store.Identity{Type: id.Type, Key: id.Key},
		states:   make(map[string]stateCell),
	}
}

// Self returns the identity of the entity bound to b.
func Self(b *Binder) Identity {
	return Identity{Type: b.identity.Type, Key: b.identity.Key}
}

// NewState registers a named persistent value for the entity bound to b and
// returns its handle. The name must be unique within that entity; registering
// the same name twice panics. A newly registered state has its type's zero
// value until activation data is loaded or Set succeeds.
func NewState[T any](b *Binder, name string) State[T] {
	if _, exists := b.states[name]; exists {
		panic(fmt.Sprintf("state %q is already registered", name))
	}
	cell := &stateCellValue[T]{binder: b}
	b.states[name] = cell
	return State[T]{cell: cell}
}

// State is a handle to one named JSON-encoded value in an entity's persistent
// state record. All State handles for one entity share one JSON object, one
// store record, and one ETag; setting one handle rewrites the complete record.
// Obtain a State with NewState; the zero value is not usable.
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

// Set JSON-encodes value and persists the entity's complete state record using
// ctx. A JSON encoding error, including an error encoding another registered
// state while rebuilding the record, leaves the current value unchanged and is
// returned without a store write. Store errors leave the current value
// unchanged and are returned as well; in particular,
// errors.Is(err, store.ErrConflict) reports an ETag conflict.
// A store write failure also discards the current entity activation after the
// containing call completes, so the next call creates a fresh activation.
// On success, subsequent Get calls return value.
func (s State[T]) Set(ctx context.Context, value T) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := s.cell.binder.persist(ctx, s.cell, encoded); err != nil {
		return err
	}
	s.cell.value = value
	return nil
}

type stateCellValue[T any] struct {
	binder *Binder
	value  T
}

func (s *stateCellValue[T]) encode() ([]byte, error) {
	return json.Marshal(s.value)
}

func (s *stateCellValue[T]) decode(data []byte) error {
	return json.Unmarshal(data, &s.value)
}

func (b *Binder) load(ctx context.Context) error {
	record, err := b.runtime.store.Read(ctx, b.identity)
	if err != nil {
		return err
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

func (b *Binder) persist(ctx context.Context, changed stateCell, changedData []byte) error {
	document := make(map[string]json.RawMessage, len(b.states))
	for name, cell := range b.states {
		var data []byte
		if cell == changed {
			data = changedData
		} else {
			var err error
			data, err = cell.encode()
			if err != nil {
				return err
			}
		}
		document[name] = json.RawMessage(data)
	}

	data, err := json.Marshal(document)
	if err != nil {
		return err
	}
	etag, err := b.runtime.store.Write(ctx, b.identity, data, b.etag)
	if err != nil {
		b.discard = err
		return err
	}
	b.etag = etag
	return nil
}

func (b *Binder) discardError() error {
	return b.discard
}
