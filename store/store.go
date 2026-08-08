// Package store defines the persistence interfaces used by gor and provides
// in-memory and SQLite implementations.
//
// Store implementations are part of the supported extension surface. They
// persist entity state and coordinate membership and schedules through
// compare-and-swap operations.
package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/suraciii/gor/clock"
)

// GrainId names one Grain State record by its GrainType and GrainKey.
type GrainId struct {
	GrainType string
	GrainKey  string
}

// ETag identifies the version of a record, member, or schedule row.
// The zero value means that the row must not exist when it is first written.
type ETag int64

// ErrConflict reports that a compare-and-swap expected an ETag different from
// the row's current ETag. It is a sentinel error: callers can use errors.Is,
// and implementations may wrap it when returning the error.
var ErrConflict = errors.New("store: etag conflict")

// Record is the data and ETag returned for one entity identity.
//
// A missing record is returned as the zero Record with a nil error. Store
// implementations must not retain or expose the caller's mutable Data slice.
type Record struct {
	Data []byte
	ETag ETag
}

// Store persists one entity-state record per GrainId.
//
// Implementations must support concurrent calls. Write must atomically compare
// the current ETag with expect and commit only on an exact match. A missing
// record has ETag zero; a successful write stores a new value and returns the
// incremented ETag. A failed comparison must leave the record unchanged and
// return an error matching ErrConflict with errors.Is. Methods must honor the
// context and return its error when it is canceled before the operation can
// complete.
type Store interface {
	// Read returns the record for id, or a zero Record and nil when it is absent.
	Read(context.Context, GrainId) (Record, error)
	// Write replaces id's data when its current ETag equals expect.
	Write(context.Context, GrainId, []byte, ETag) (ETag, error)
}

func timeValue(value time.Time) int64 {
	return value.UnixNano()
}

func timeFromValue(value int64) time.Time {
	return time.Unix(0, value).UTC()
}

// Memory is an in-memory implementation of Store, MemberStore, and
// ScheduleStore.
type Memory struct {
	mu          sync.RWMutex
	records     map[GrainId]Record
	schedules   map[scheduleKey]Schedule
	members     map[memberKey]Member
	memberClock clock.Clock
}

var _ Store = (*Memory)(nil)
var _ ScheduleStore = (*Memory)(nil)
var _ MemberStore = (*Memory)(nil)

// NewMemory returns an empty in-memory store.
//
// If a clock is supplied, the first clock is used for MemberSnapshot.TableNow;
// when omitted, a Real clock is used. Additional clocks are ignored.
func NewMemory(memberClocks ...clock.Clock) *Memory {
	memberClock := clock.Clock(clock.Real{})
	if len(memberClocks) > 0 {
		memberClock = memberClocks[0]
	}
	return &Memory{
		records:     make(map[GrainId]Record),
		schedules:   make(map[scheduleKey]Schedule),
		members:     make(map[memberKey]Member),
		memberClock: memberClock,
	}
}

// Read returns a copy of the stored record for id, or a zero Record and nil
// when id has not been written.
func (m *Memory) Read(ctx context.Context, id GrainId) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.records[id]
	if !ok {
		return Record{}, nil
	}
	record.Data = clone(record.Data)
	return record, nil
}

// Write atomically replaces id's data when expect matches its current ETag.
// It returns the incremented ETag, or an error matching ErrConflict when the
// comparison fails.
func (m *Memory) Write(ctx context.Context, id GrainId, data []byte, expect ETag) (ETag, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.records[id]
	if current.ETag != expect {
		return 0, ErrConflict
	}

	newETag := current.ETag + 1
	m.records[id] = Record{Data: clone(data), ETag: newETag}
	return newETag, nil
}

func clone(data []byte) []byte {
	return append([]byte(nil), data...)
}
