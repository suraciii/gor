package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/suraciii/gor/clock"
)

type Identity struct {
	Type string
	Key  string
}

type ETag int64

var ErrConflict = errors.New("store: etag conflict")

type Record struct {
	Data []byte
	ETag ETag
}

type Store interface {
	Read(context.Context, Identity) (Record, error)
	Write(context.Context, Identity, []byte, ETag) (ETag, error)
}

func timeValue(value time.Time) int64 {
	return value.UnixNano()
}

func timeFromValue(value int64) time.Time {
	return time.Unix(0, value).UTC()
}

type Memory struct {
	mu          sync.RWMutex
	records     map[Identity]Record
	schedules   map[scheduleKey]Schedule
	members     map[memberKey]Member
	memberClock clock.Clock
}

var _ Store = (*Memory)(nil)
var _ ScheduleStore = (*Memory)(nil)
var _ MemberStore = (*Memory)(nil)

func NewMemory(memberClocks ...clock.Clock) *Memory {
	memberClock := clock.Clock(clock.Real{})
	if len(memberClocks) > 0 {
		memberClock = memberClocks[0]
	}
	return &Memory{
		records:     make(map[Identity]Record),
		schedules:   make(map[scheduleKey]Schedule),
		members:     make(map[memberKey]Member),
		memberClock: memberClock,
	}
}

func (m *Memory) Read(ctx context.Context, id Identity) (Record, error) {
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

func (m *Memory) Write(ctx context.Context, id Identity, data []byte, expect ETag) (ETag, error) {
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
