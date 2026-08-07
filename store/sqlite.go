package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/suraciii/gor/clock"
	_ "modernc.org/sqlite"
)

const sqliteBusyTimeout = 5000

// Durability is the guarantee a confirmed state write keeps across a hard
// crash (power loss, operating-system crash, hard reset). A clean restart
// loses nothing at either tier.
//
// The tier applies to entity state only; the schedule and membership tables
// always run at DurabilityFull.
type Durability int

const (
	// DurabilityFull keeps every confirmed state write on storage before the
	// call returns. A hard crash loses no confirmed write.
	DurabilityFull Durability = iota
	// DurabilityRelaxed does not force each confirmed state write to storage
	// individually. A hard crash can lose the most recent confirmed writes;
	// what is on storage stays intact and readable. Close flushes the state
	// database, so a clean restart loses no confirmed write.
	DurabilityRelaxed
)

// Option configures a SQLite store.
type Option func(*options)

type options struct {
	durability Durability
}

// WithDurability sets the durability tier for entity-state writes. The
// default is DurabilityFull; DurabilityRelaxed must be chosen explicitly.
func WithDurability(d Durability) Option {
	return func(o *options) {
		o.durability = d
	}
}

// SQLite is a SQLite-backed implementation of Store, MemberStore, and
// ScheduleStore.
//
// Entity state lives in a database file derived from the named path by
// inserting "-state" before the file extension; the schedule and membership
// tables live in the named file. Backups must cover both files and their
// -wal sidecars.
//
// Call Close when the backend is no longer needed; it releases the database
// handles opened during construction and, at DurabilityRelaxed, flushes the
// state database.
type SQLite struct {
	readDB       *sql.DB
	writeDB      *sql.DB
	stateReadDB  *sql.DB
	stateWriteDB *sql.DB
	memberClock  clock.Clock
	durability   Durability
}

var _ Store = (*SQLite)(nil)
var _ MemberStore = (*SQLite)(nil)

// OpenSQLite opens or creates the SQLite database at path using a real clock
// for membership snapshots. It returns an error if the database cannot be
// opened or initialized.
func OpenSQLite(path string, opts ...Option) (*SQLite, error) {
	return openSQLite(path, clock.Real{}, opts...)
}

// OpenSQLiteWithClock opens or creates the SQLite database at path and uses
// memberClock for MemberSnapshot.TableNow. The clock must be safe for
// concurrent calls.
func OpenSQLiteWithClock(path string, memberClock clock.Clock, opts ...Option) (*SQLite, error) {
	return openSQLite(path, memberClock, opts...)
}

func openSQLite(path string, memberClock clock.Clock, opts ...Option) (*SQLite, error) {
	opt := options{durability: DurabilityFull}
	for _, apply := range opts {
		apply(&opt)
	}
	if opt.durability != DurabilityFull && opt.durability != DurabilityRelaxed {
		return nil, fmt.Errorf("store: unknown durability value %d", opt.durability)
	}
	statePath := stateFilePath(path)

	writeDB, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	if err := writeDB.Ping(); err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("open sqlite database %q: %w", path, err)
	}
	if err := createCoordinationSchema(writeDB); err != nil {
		writeDB.Close()
		return nil, err
	}

	readDB, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		writeDB.Close()
		return nil, err
	}
	readDB.SetMaxIdleConns(16)
	if err := readDB.Ping(); err != nil {
		readDB.Close()
		writeDB.Close()
		return nil, fmt.Errorf("open sqlite database %q: %w", path, err)
	}

	stateWriteDB, err := sql.Open("sqlite", sqliteDSN(statePath, opt.durability))
	if err != nil {
		readDB.Close()
		writeDB.Close()
		return nil, err
	}
	stateWriteDB.SetMaxOpenConns(1)
	stateWriteDB.SetMaxIdleConns(1)
	if err := stateWriteDB.Ping(); err != nil {
		stateWriteDB.Close()
		readDB.Close()
		writeDB.Close()
		return nil, fmt.Errorf("open sqlite database %q: %w", statePath, err)
	}
	if err := createStateSchema(stateWriteDB); err != nil {
		stateWriteDB.Close()
		readDB.Close()
		writeDB.Close()
		return nil, err
	}

	stateReadDB, err := sql.Open("sqlite", sqliteDSN(statePath, opt.durability))
	if err != nil {
		stateWriteDB.Close()
		readDB.Close()
		writeDB.Close()
		return nil, err
	}
	stateReadDB.SetMaxIdleConns(16)
	if err := stateReadDB.Ping(); err != nil {
		stateReadDB.Close()
		stateWriteDB.Close()
		readDB.Close()
		writeDB.Close()
		return nil, fmt.Errorf("open sqlite database %q: %w", statePath, err)
	}

	if err := migrateOldLayout(writeDB, statePath); err != nil {
		stateReadDB.Close()
		stateWriteDB.Close()
		readDB.Close()
		writeDB.Close()
		return nil, fmt.Errorf("migrate database %q: %w", path, err)
	}

	return &SQLite{
		readDB:       readDB,
		writeDB:      writeDB,
		stateReadDB:  stateReadDB,
		stateWriteDB: stateWriteDB,
		memberClock:  memberClock,
		durability:   opt.durability,
	}, nil
}

// Read returns a copy of the stored record for id, or a zero Record and nil
// when id has not been written.
func (s *SQLite) Read(ctx context.Context, id Identity) (Record, error) {
	var record Record
	var etag int64
	err := s.stateReadDB.QueryRowContext(ctx,
		"SELECT data, etag FROM records WHERE identity_type = ? AND identity_key = ?",
		id.Type,
		id.Key,
	).Scan(&record.Data, &etag)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, nil
	}
	if err != nil {
		return Record{}, err
	}
	record.ETag = ETag(etag)
	record.Data = clone(record.Data)
	return record, nil
}

// Write atomically replaces id's data when expect matches its current ETag.
// It returns the incremented ETag, or an error matching ErrConflict when the
// comparison fails.
func (s *SQLite) Write(ctx context.Context, id Identity, data []byte, expect ETag) (ETag, error) {
	var (
		result sql.Result
		err    error
	)
	if expect == 0 {
		result, err = s.stateWriteDB.ExecContext(ctx,
			"INSERT INTO records (identity_type, identity_key, data, etag) VALUES (?, ?, ?, 1) ON CONFLICT (identity_type, identity_key) DO NOTHING",
			id.Type,
			id.Key,
			data,
		)
	} else {
		result, err = s.stateWriteDB.ExecContext(ctx,
			"UPDATE records SET data = ?, etag = etag + 1 WHERE identity_type = ? AND identity_key = ? AND etag = ?",
			data,
			id.Type,
			id.Key,
			int64(expect),
		)
	}
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if rows != 1 {
		return 0, ErrConflict
	}
	return expect + 1, nil
}

// Close releases the SQLite database handles. At DurabilityRelaxed it first
// flushes the state database's write-ahead log into its main file, so a clean
// restart loses no confirmed write.
func (s *SQLite) Close() error {
	var errs []error
	if err := s.stateReadDB.Close(); err != nil {
		errs = append(errs, err)
	}
	if s.durability == DurabilityRelaxed {
		// The read handles are closed above so the checkpoint has no readers.
		if _, err := s.stateWriteDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.stateWriteDB.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.readDB.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.writeDB.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func sqliteDSN(path string, durability Durability) string {
	sync := "FULL"
	if durability == DurabilityRelaxed {
		sync = "NORMAL"
	}
	return fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=busy_timeout(%d)", path, sync, sqliteBusyTimeout)
}

func stateFilePath(path string) string {
	dir, base := filepath.Split(path)
	ext := filepath.Ext(base)
	return filepath.Join(dir, strings.TrimSuffix(base, ext)+"-state"+ext)
}

func createCoordinationSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS schedule (
	entity_type TEXT NOT NULL,
	entity_key TEXT NOT NULL,
	name TEXT NOT NULL,
	method TEXT NOT NULL,
	due_at INTEGER NOT NULL,
	interval INTEGER NOT NULL,
	etag INTEGER NOT NULL,
	PRIMARY KEY (entity_type, entity_key, name)
);

CREATE TABLE IF NOT EXISTS member (
	node_addr TEXT NOT NULL,
	generation TEXT NOT NULL,
	status TEXT NOT NULL,
	iam_alive_at INTEGER NOT NULL,
	suspect_votes BLOB NOT NULL DEFAULT '[]',
	etag INTEGER NOT NULL,
	PRIMARY KEY (node_addr, generation)
)`)
	return err
}

func createStateSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS records (
	identity_type TEXT NOT NULL,
	identity_key TEXT NOT NULL,
	data BLOB NOT NULL,
	etag INTEGER NOT NULL,
	PRIMARY KEY (identity_type, identity_key)
)`)
	return err
}

// migrateOldLayout moves records out of an earlier single-file database that
// still keeps the state, schedule, and membership tables together. The copy
// commits before the old rows are dropped, so an interruption between the two
// leaves the old database intact and the next open redoes the copy.
func migrateOldLayout(writeDB *sql.DB, statePath string) error {
	hasRecords, err := tableExists(writeDB, "records")
	if err != nil || !hasRecords {
		return err
	}
	if err := copyStateRows(writeDB, statePath); err != nil {
		return err
	}
	_, err = writeDB.Exec(`DROP TABLE records`)
	return err
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var found int
	err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func copyStateRows(writeDB *sql.DB, statePath string) error {
	conn, err := writeDB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// ATTACH takes no bound parameters; the URI form percent-encodes the path.
	if _, err := tx.Exec(`ATTACH DATABASE '` + sqliteFileURI(statePath) + `' AS state`); err != nil {
		return fmt.Errorf("attach state database: %w", err)
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO state.records (identity_type, identity_key, data, etag)
SELECT identity_type, identity_key, data, etag FROM records`); err != nil {
		return fmt.Errorf("copy state rows: %w", err)
	}

	var oldRows, newRows int
	if err := tx.QueryRow(`SELECT count(*) FROM records`).Scan(&oldRows); err != nil {
		return err
	}
	if err := tx.QueryRow(`SELECT count(*) FROM state.records`).Scan(&newRows); err != nil {
		return err
	}
	if oldRows != newRows {
		return fmt.Errorf("copied %d of %d state rows", newRows, oldRows)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state copy: %w", err)
	}
	_, err = conn.ExecContext(context.Background(), `DETACH DATABASE state`)
	return err
}

func sqliteFileURI(path string) string {
	abs, _ := filepath.Abs(path)
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}).String()
}
