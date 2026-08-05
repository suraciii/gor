package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/suraciii/gor/clock"
	_ "modernc.org/sqlite"
)

const sqliteBusyTimeout = 5000

type SQLite struct {
	readDB      *sql.DB
	writeDB     *sql.DB
	memberClock clock.Clock
}

var _ Store = (*SQLite)(nil)
var _ MemberStore = (*SQLite)(nil)

func OpenSQLite(path string) (*SQLite, error) {
	return openSQLite(path, clock.Real{})
}

func OpenSQLiteWithClock(path string, memberClock clock.Clock) (*SQLite, error) {
	return openSQLite(path, memberClock)
}

func openSQLite(path string, memberClock clock.Clock) (*SQLite, error) {
	dsn := sqliteDSN(path)
	writeDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	if err := writeDB.Ping(); err != nil {
		writeDB.Close()
		return nil, err
	}
	if err := createSchema(writeDB); err != nil {
		writeDB.Close()
		return nil, err
	}

	readDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		writeDB.Close()
		return nil, err
	}
	readDB.SetMaxIdleConns(16)
	if err := readDB.Ping(); err != nil {
		readDB.Close()
		writeDB.Close()
		return nil, err
	}

	return &SQLite{readDB: readDB, writeDB: writeDB, memberClock: memberClock}, nil
}

func (s *SQLite) Read(ctx context.Context, id Identity) (Record, error) {
	var record Record
	var etag int64
	err := s.readDB.QueryRowContext(ctx,
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

func (s *SQLite) Write(ctx context.Context, id Identity, data []byte, expect ETag) (ETag, error) {
	var (
		result sql.Result
		err    error
	)
	if expect == 0 {
		result, err = s.writeDB.ExecContext(ctx,
			"INSERT INTO records (identity_type, identity_key, data, etag) VALUES (?, ?, ?, 1) ON CONFLICT (identity_type, identity_key) DO NOTHING",
			id.Type,
			id.Key,
			data,
		)
	} else {
		result, err = s.writeDB.ExecContext(ctx,
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

func (s *SQLite) Close() error {
	return errors.Join(s.readDB.Close(), s.writeDB.Close())
}

func sqliteDSN(path string) string {
	return fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(%d)", path, sqliteBusyTimeout)
}

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS records (
	identity_type TEXT NOT NULL,
	identity_key TEXT NOT NULL,
	data BLOB NOT NULL,
	etag INTEGER NOT NULL,
	PRIMARY KEY (identity_type, identity_key)
);

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
