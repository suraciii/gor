package domain

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"

	_ "modernc.org/sqlite"
)

// ApplicationStore owns business records for the conformance example. It is
// separate from the stores used by the Grain Runtime.
type ApplicationStore interface {
	SavePending(context.Context, PendingAction) error
	ListPending(context.Context) ([]PendingAction, error)
	ApplyPending(context.Context, string) error
	ReadApplied(context.Context, string) (AppliedRecord, bool, error)
	Close() error
}

// PendingAction is an application-owned Business Action waiting for recovery.
type PendingAction struct {
	ActionID  string
	DeviceKey string
	State     string
	TraceID   string
}

// AppliedRecord is the application receipt for one ActionID.
type AppliedRecord struct {
	ActionID  string
	DeviceKey string
	State     string
	TraceID   string
}

var (
	// ErrPendingActionConflict reports reuse of an ActionID with a new payload.
	ErrPendingActionConflict = errors.New("application action ID has a different payload")
	// ErrPendingActionNotFound reports an action that is neither pending nor applied.
	ErrPendingActionNotFound = errors.New("application pending action was not found")
)

// MemoryApplicationStore is an in-memory ApplicationStore for deterministic
// example tests.
type MemoryApplicationStore struct {
	mu      sync.Mutex
	pending map[string]PendingAction
	applied map[string]AppliedRecord
}

var _ ApplicationStore = (*MemoryApplicationStore)(nil)

// NewMemoryApplicationStore returns an empty ApplicationStore.
func NewMemoryApplicationStore() *MemoryApplicationStore {
	return &MemoryApplicationStore{
		pending: make(map[string]PendingAction),
		applied: make(map[string]AppliedRecord),
	}
}

// SavePending inserts an action, or accepts an identical repeat.
func (s *MemoryApplicationStore) SavePending(ctx context.Context, action PendingAction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if action.ActionID == "" {
		return errors.New("application action ID is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.pending[action.ActionID]; ok {
		if current != action {
			return ErrPendingActionConflict
		}
		return nil
	}
	if current, ok := s.applied[action.ActionID]; ok {
		if current != (AppliedRecord(action)) {
			return ErrPendingActionConflict
		}
		return nil
	}
	s.pending[action.ActionID] = action
	return nil
}

// ListPending returns pending actions in ActionID order.
func (s *MemoryApplicationStore) ListPending(ctx context.Context) ([]PendingAction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]PendingAction, 0, len(s.pending))
	for _, action := range s.pending {
		result = append(result, action)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ActionID < result[j].ActionID })
	return result, nil
}

// ApplyPending applies one action and creates one receipt. Repeating an
// applied ActionID succeeds without changing the receipt.
func (s *MemoryApplicationStore) ApplyPending(ctx context.Context, actionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.applied[actionID]; ok {
		return nil
	}
	action, ok := s.pending[actionID]
	if !ok {
		return ErrPendingActionNotFound
	}
	s.applied[actionID] = AppliedRecord(action)
	delete(s.pending, actionID)
	return nil
}

// ReadApplied reads one application receipt.
func (s *MemoryApplicationStore) ReadApplied(ctx context.Context, actionID string) (AppliedRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return AppliedRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.applied[actionID]
	return record, ok, nil
}

// Close releases no resources and is provided to keep the store boundary equal
// to the durable implementation.
func (s *MemoryApplicationStore) Close() error {
	return nil
}

// SQLiteApplicationStore is the durable ApplicationStore for the example. It
// owns only the pending_actions and applied_records tables in its own file.
type SQLiteApplicationStore struct {
	db *sql.DB
}

var _ ApplicationStore = (*SQLiteApplicationStore)(nil)

// OpenSQLiteApplicationStore opens or creates a business database at path.
func OpenSQLiteApplicationStore(path string) (*SQLiteApplicationStore, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)", path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open application database %q: %w", path, err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS pending_actions (
	action_id TEXT PRIMARY KEY,
	device_key TEXT NOT NULL,
	state TEXT NOT NULL,
	trace_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_records (
	action_id TEXT PRIMARY KEY,
	device_key TEXT NOT NULL,
	state TEXT NOT NULL,
	trace_id TEXT NOT NULL
)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create application schema: %w", err)
	}
	return &SQLiteApplicationStore{db: db}, nil
}

// SavePending inserts an action, or accepts an identical repeat.
func (s *SQLiteApplicationStore) SavePending(ctx context.Context, action PendingAction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if action.ActionID == "" {
		return errors.New("application action ID is empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	current, found, err := readPendingTx(ctx, tx, action.ActionID)
	if err != nil {
		return err
	}
	if found {
		if current != action {
			return ErrPendingActionConflict
		}
		return tx.Commit()
	}
	applied, found, err := readAppliedTx(ctx, tx, action.ActionID)
	if err != nil {
		return err
	}
	if found {
		if applied != AppliedRecord(action) {
			return ErrPendingActionConflict
		}
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pending_actions (action_id, device_key, state, trace_id) VALUES (?, ?, ?, ?)`,
		action.ActionID, action.DeviceKey, action.State, action.TraceID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// ListPending returns pending actions in ActionID order.
func (s *SQLiteApplicationStore) ListPending(ctx context.Context) ([]PendingAction, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT action_id, device_key, state, trace_id
FROM pending_actions
ORDER BY action_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []PendingAction
	for rows.Next() {
		var action PendingAction
		if err := rows.Scan(&action.ActionID, &action.DeviceKey, &action.State, &action.TraceID); err != nil {
			return nil, err
		}
		result = append(result, action)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// ApplyPending applies one action in one application transaction. The unique
// ActionID makes a repeat a successful no-op.
func (s *SQLiteApplicationStore) ApplyPending(ctx context.Context, actionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, found, err := readAppliedTx(ctx, tx, actionID); err != nil {
		return err
	} else if found {
		return tx.Commit()
	}
	action, found, err := readPendingTx(ctx, tx, actionID)
	if err != nil {
		return err
	}
	if !found {
		return ErrPendingActionNotFound
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO applied_records (action_id, device_key, state, trace_id)
VALUES (?, ?, ?, ?)`, action.ActionID, action.DeviceKey, action.State, action.TraceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_actions WHERE action_id = ?`, actionID); err != nil {
		return err
	}
	return tx.Commit()
}

// ReadApplied reads one application receipt.
func (s *SQLiteApplicationStore) ReadApplied(ctx context.Context, actionID string) (AppliedRecord, bool, error) {
	return readApplied(ctx, s.db, actionID)
}

// Close closes the business database.
func (s *SQLiteApplicationStore) Close() error {
	return s.db.Close()
}

type applicationQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readPending(ctx context.Context, db applicationQuery, actionID string) (PendingAction, bool, error) {
	var action PendingAction
	err := db.QueryRowContext(ctx, `
SELECT action_id, device_key, state, trace_id
FROM pending_actions WHERE action_id = ?`, actionID).
		Scan(&action.ActionID, &action.DeviceKey, &action.State, &action.TraceID)
	if errors.Is(err, sql.ErrNoRows) {
		return PendingAction{}, false, nil
	}
	return action, err == nil, err
}

func readApplied(ctx context.Context, db applicationQuery, actionID string) (AppliedRecord, bool, error) {
	var record AppliedRecord
	err := db.QueryRowContext(ctx, `
SELECT action_id, device_key, state, trace_id
FROM applied_records WHERE action_id = ?`, actionID).
		Scan(&record.ActionID, &record.DeviceKey, &record.State, &record.TraceID)
	if errors.Is(err, sql.ErrNoRows) {
		return AppliedRecord{}, false, nil
	}
	return record, err == nil, err
}

func readPendingTx(ctx context.Context, tx *sql.Tx, actionID string) (PendingAction, bool, error) {
	return readPending(ctx, tx, actionID)
}

func readAppliedTx(ctx context.Context, tx *sql.Tx, actionID string) (AppliedRecord, bool, error) {
	return readApplied(ctx, tx, actionID)
}
