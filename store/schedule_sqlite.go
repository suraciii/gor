package store

import (
	"context"
	"database/sql"
	"time"
)

var _ ReminderStore = (*SQLite)(nil)

// ListDue returns due Reminders in deterministic DueAt, identity, and name
// order.
func (s *SQLite) ListDue(ctx context.Context, now time.Time) ([]Reminder, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT entity_type, entity_key, name, method, first_tick_time, due_at, interval, etag
FROM schedule
WHERE due_at <= ?
ORDER BY due_at, entity_type, entity_key, name`, timeValue(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]Reminder, 0)
	for rows.Next() {
		var (
			entityType string
			entityKey  string
			name       string
			method     string
			firstTick  int64
			dueAt      int64
			interval   int64
			etag       int64
		)
		if err := rows.Scan(&entityType, &entityKey, &name, &method, &firstTick, &dueAt, &interval, &etag); err != nil {
			return nil, err
		}
		result = append(result, Reminder{
			GrainId:       GrainId{GrainType: entityType, GrainKey: entityKey},
			Name:          name,
			Method:        method,
			FirstTickTime: timeFromValue(firstTick),
			DueAt:         timeFromValue(dueAt),
			Interval:      time.Duration(interval),
			ETag:          ETag(etag),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Claim atomically checks the Reminder identity, name, and ETag. It returns
// true and advances the row when nextDueAt is non-zero, or deletes the row
// when nextDueAt is zero. It returns false and nil when the row is absent or
// stale.
func (s *SQLite) Claim(ctx context.Context, reminder Reminder, nextDueAt time.Time) (bool, error) {
	var (
		result sql.Result
		err    error
	)
	if nextDueAt.IsZero() {
		result, err = s.writeDB.ExecContext(ctx, `
DELETE FROM schedule
WHERE entity_type = ? AND entity_key = ? AND name = ? AND etag = ?`,
			reminder.GrainId.GrainType,
			reminder.GrainId.GrainKey,
			reminder.Name,
			int64(reminder.ETag),
		)
	} else {
		result, err = s.writeDB.ExecContext(ctx, `
UPDATE schedule
SET due_at = ?, etag = etag + 1
WHERE entity_type = ? AND entity_key = ? AND name = ? AND etag = ?`,
			timeValue(nextDueAt),
			reminder.GrainId.GrainType,
			reminder.GrainId.GrainKey,
			reminder.Name,
			int64(reminder.ETag),
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

// Put unconditionally inserts or replaces a Reminder and assigns a new ETag.
func (s *SQLite) Put(ctx context.Context, reminder Reminder) error {
	_, err := s.writeDB.ExecContext(ctx, `
INSERT INTO schedule (entity_type, entity_key, name, method, first_tick_time, due_at, interval, etag)
VALUES (?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT (entity_type, entity_key, name) DO UPDATE SET
method = excluded.method,
first_tick_time = excluded.first_tick_time,
due_at = excluded.due_at,
interval = excluded.interval,
etag = schedule.etag + 1`,
		reminder.GrainId.GrainType,
		reminder.GrainId.GrainKey,
		reminder.Name,
		reminder.Method,
		timeValue(reminder.FirstTickTime),
		timeValue(reminder.DueAt),
		int64(reminder.Interval),
	)
	return err
}

// Delete unconditionally removes the Reminder identified by id and name.
func (s *SQLite) Delete(ctx context.Context, id GrainId, name string) error {
	_, err := s.writeDB.ExecContext(ctx, `
DELETE FROM schedule
WHERE entity_type = ? AND entity_key = ? AND name = ?`, id.GrainType, id.GrainKey, name)
	return err
}
