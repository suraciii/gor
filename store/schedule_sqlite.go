package store

import (
	"context"
	"database/sql"
	"time"
)

var _ ScheduleStore = (*SQLite)(nil)

// ListDue returns due schedules in deterministic DueAt, identity, and name
// order.
func (s *SQLite) ListDue(ctx context.Context, now time.Time) ([]Schedule, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT entity_type, entity_key, name, method, due_at, interval, etag
FROM schedule
WHERE due_at <= ?
ORDER BY due_at, entity_type, entity_key, name`, timeValue(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]Schedule, 0)
	for rows.Next() {
		var (
			entityType string
			entityKey  string
			name       string
			method     string
			dueAt      int64
			interval   int64
			etag       int64
		)
		if err := rows.Scan(&entityType, &entityKey, &name, &method, &dueAt, &interval, &etag); err != nil {
			return nil, err
		}
		result = append(result, Schedule{
			GrainId:  GrainId{GrainType: entityType, GrainKey: entityKey},
			Name:     name,
			Method:   method,
			DueAt:    timeFromValue(dueAt),
			Interval: time.Duration(interval),
			ETag:     ETag(etag),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Claim atomically checks schedule's identity, name, and ETag. It returns true
// and advances the row when nextDueAt is non-zero, or deletes the row when
// nextDueAt is zero. It returns false and nil when the row is absent or stale.
func (s *SQLite) Claim(ctx context.Context, schedule Schedule, nextDueAt time.Time) (bool, error) {
	var (
		result sql.Result
		err    error
	)
	if nextDueAt.IsZero() {
		result, err = s.writeDB.ExecContext(ctx, `
DELETE FROM schedule
WHERE entity_type = ? AND entity_key = ? AND name = ? AND etag = ?`,
			schedule.GrainId.GrainType,
			schedule.GrainId.GrainKey,
			schedule.Name,
			int64(schedule.ETag),
		)
	} else {
		result, err = s.writeDB.ExecContext(ctx, `
UPDATE schedule
SET due_at = ?, etag = etag + 1
WHERE entity_type = ? AND entity_key = ? AND name = ? AND etag = ?`,
			timeValue(nextDueAt),
			schedule.GrainId.GrainType,
			schedule.GrainId.GrainKey,
			schedule.Name,
			int64(schedule.ETag),
		)
	}
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

// Put unconditionally inserts or replaces a schedule and assigns a new ETag.
func (s *SQLite) Put(ctx context.Context, schedule Schedule) error {
	_, err := s.writeDB.ExecContext(ctx, `
INSERT INTO schedule (entity_type, entity_key, name, method, due_at, interval, etag)
VALUES (?, ?, ?, ?, ?, ?, 1)
ON CONFLICT (entity_type, entity_key, name) DO UPDATE SET
	method = excluded.method,
	due_at = excluded.due_at,
	interval = excluded.interval,
	etag = schedule.etag + 1`,
		schedule.GrainId.GrainType,
		schedule.GrainId.GrainKey,
		schedule.Name,
		schedule.Method,
		timeValue(schedule.DueAt),
		int64(schedule.Interval),
	)
	return err
}

// Delete unconditionally removes the schedule identified by id and name.
func (s *SQLite) Delete(ctx context.Context, id GrainId, name string) error {
	_, err := s.writeDB.ExecContext(ctx, `
DELETE FROM schedule
WHERE entity_type = ? AND entity_key = ? AND name = ?`, id.GrainType, id.GrainKey, name)
	return err
}
