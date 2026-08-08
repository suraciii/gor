package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// oldLayoutSchema is the single-file schema gor wrote before the state rows
// moved into their own database: state, schedule, and membership tables
// together in one file.
const oldLayoutSchema = `
CREATE TABLE records (
	identity_type TEXT NOT NULL,
	identity_key TEXT NOT NULL,
	data BLOB NOT NULL,
	etag INTEGER NOT NULL,
	PRIMARY KEY (identity_type, identity_key)
);

CREATE TABLE schedule (
	entity_type TEXT NOT NULL,
	entity_key TEXT NOT NULL,
	name TEXT NOT NULL,
	method TEXT NOT NULL,
	due_at INTEGER NOT NULL,
	interval INTEGER NOT NULL,
	etag INTEGER NOT NULL,
	PRIMARY KEY (entity_type, entity_key, name)
);

CREATE TABLE member (
	node_addr TEXT NOT NULL,
	generation TEXT NOT NULL,
	status TEXT NOT NULL,
	iam_alive_at INTEGER NOT NULL,
	suspect_votes BLOB NOT NULL DEFAULT '[]',
	etag INTEGER NOT NULL,
	PRIMARY KEY (node_addr, generation)
)`

type oldScheduleSeed struct {
	identity GrainId
	name     string
	method   string
	dueAt    time.Time
	etag     ETag
}

type oldMemberSeed struct {
	nodeAddr   string
	generation string
	status     MemberStatus
	iamAliveAt time.Time
	votes      string
	etag       ETag
}

// buildOldLayoutDB creates a single-file database in the pre-split layout and
// seeds it. It closes without a checkpoint, so any WAL frames stay on disk
// like a crashed earlier process left them.
func buildOldLayoutDB(t *testing.T, path string, records int) {
	t.Helper()
	db, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		t.Fatalf("open old layout db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(oldLayoutSchema); err != nil {
		t.Fatalf("create old layout schema: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	for i := 0; i < records; i++ {
		if _, err := tx.Exec(`INSERT INTO records VALUES (?, ?, ?, ?)`,
			fmt.Sprintf("type-%d", i%4),
			fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("data-%d", i)),
			(i+1)*5,
		); err != nil {
			t.Fatalf("seed record %d: %v", i, err)
		}
	}
	for i, schedule := range []oldScheduleSeed{
		{identity: GrainId{GrainType: "account", GrainKey: "alice"}, name: "tick", method: "Tick", dueAt: time.Unix(0, 1000).UTC(), etag: 1},
		{identity: GrainId{GrainType: "account", GrainKey: "bob"}, name: "renew", method: "Renew", dueAt: time.Unix(0, 500).UTC(), etag: 3},
	} {
		if _, err := tx.Exec(`INSERT INTO schedule VALUES (?, ?, ?, ?, ?, ?, ?)`,
			schedule.identity.GrainType,
			schedule.identity.GrainKey,
			schedule.name,
			schedule.method,
			timeValue(schedule.dueAt),
			0,
			int64(schedule.etag),
		); err != nil {
			t.Fatalf("seed schedule %d: %v", i, err)
		}
	}
	for i, member := range []oldMemberSeed{
		{nodeAddr: "10.0.0.1", generation: "gen-1", status: MemberActive, iamAliveAt: time.Unix(0, 2000).UTC(), votes: "[]", etag: 1},
		{nodeAddr: "10.0.0.2", generation: "gen-2", status: MemberJoining, iamAliveAt: time.Unix(0, 3000).UTC(), votes: `[{"node_addr":"10.0.0.1","generation":"gen-1","expires_at":4000}]`, etag: 2},
	} {
		if _, err := tx.Exec(`INSERT INTO member VALUES (?, ?, ?, ?, ?, ?)`,
			member.nodeAddr,
			member.generation,
			string(member.status),
			timeValue(member.iamAliveAt),
			member.votes,
			int64(member.etag),
		); err != nil {
			t.Fatalf("seed member %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}
}

func openRawSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %q: %v", path, err)
	}
	return db
}

func tableRowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func TestMigrate_OldDatabaseReadsBackEveryConfirmedState(t *testing.T) {
	for _, tier := range []Durability{DurabilityFull, DurabilityRelaxed} {
		t.Run(fmt.Sprintf("tier-%d", tier), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "gor.db")
			const records = 200
			buildOldLayoutDB(t, path, records)

			s, err := OpenSQLite(path, WithDurability(tier))
			if err != nil {
				t.Fatalf("OpenSQLite: %v", err)
			}
			defer s.Close()

			for i := 0; i < records; i++ {
				id := GrainId{GrainType: fmt.Sprintf("type-%d", i%4), GrainKey: fmt.Sprintf("key-%d", i)}
				record, err := s.Read(context.Background(), id)
				if err != nil {
					t.Fatalf("Read %#v: %v", id, err)
				}
				if want := fmt.Sprintf("data-%d", i); string(record.Data) != want {
					t.Fatalf("record %d data = %q, want %q", i, record.Data, want)
				}
				if want := ETag((i + 1) * 5); record.ETag != want {
					t.Fatalf("record %d ETag = %d, want %d", i, record.ETag, want)
				}
			}

			reminders, err := s.ListDue(context.Background(), time.Unix(0, 100000).UTC())
			if err != nil {
				t.Fatalf("ListDue: %v", err)
			}
			if len(reminders) != 2 {
				t.Fatalf("ListDue returned %d reminders, want 2", len(reminders))
			}
			if reminders[0].GrainId != (GrainId{GrainType: "account", GrainKey: "bob"}) || reminders[0].Name != "renew" || reminders[0].Method != "Renew" || reminders[0].ETag != 3 || !reminders[0].FirstTickTime.Equal(reminders[0].DueAt) {
				t.Fatalf("reminder[0] = %#v, want bob/renew/3 with FirstTickTime equal to DueAt", reminders[0])
			}
			if reminders[1].GrainId != (GrainId{GrainType: "account", GrainKey: "alice"}) || reminders[1].Name != "tick" || reminders[1].Method != "Tick" || reminders[1].ETag != 1 || !reminders[1].FirstTickTime.Equal(reminders[1].DueAt) {
				t.Fatalf("reminder[1] = %#v, want alice/tick/1 with FirstTickTime equal to DueAt", reminders[1])
			}

			members, err := s.ListMembers(context.Background())
			if err != nil {
				t.Fatalf("ListMembers: %v", err)
			}
			if len(members.Members) != 2 {
				t.Fatalf("ListMembers returned %d members, want 2", len(members.Members))
			}
			if members.Members[0].NodeAddr != "10.0.0.1" || members.Members[0].Status != MemberActive || members.Members[0].ETag != 1 {
				t.Fatalf("member[0] = %#v, want 10.0.0.1 active etag 1", members.Members[0])
			}
			if members.Members[1].NodeAddr != "10.0.0.2" || members.Members[1].Status != MemberJoining || members.Members[1].ETag != 2 || len(members.Members[1].SuspectVotes) != 1 {
				t.Fatalf("member[1] = %#v, want 10.0.0.2 joining etag 2 with one vote", members.Members[1])
			}

			main := openRawSQLite(t, path)
			state := openRawSQLite(t, stateFilePath(path))
			if has, err := tableExists(main, "records"); err != nil || has {
				t.Fatalf("main file records table after migration: has=%v err=%v, want absent", has, err)
			}
			if count := tableRowCount(t, state, "records"); count != records {
				t.Fatalf("state file records count = %d, want %d", count, records)
			}

			reopened, err := OpenSQLite(path, WithDurability(tier))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			record, err := reopened.Read(context.Background(), GrainId{GrainType: "type-0", GrainKey: "key-0"})
			if err != nil {
				t.Fatalf("Read after reopen: %v", err)
			}
			if string(record.Data) != "data-0" || record.ETag != 5 {
				t.Fatalf("record after reopen = %#v, want data-0 etag 5", record)
			}
			reopenedReminders, err := reopened.ListDue(context.Background(), time.Unix(0, 100000).UTC())
			if err != nil {
				t.Fatalf("ListDue after reopen: %v", err)
			}
			if len(reopenedReminders) != 2 || !reopenedReminders[0].FirstTickTime.Equal(reopenedReminders[0].DueAt) || !reopenedReminders[1].FirstTickTime.Equal(reopenedReminders[1].DueAt) {
				t.Fatalf("reminders after reopen = %#v, want fallback FirstTickTime values", reopenedReminders)
			}
		})
	}
}

func TestMigrate_InterruptedMigrationCompletesOnReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")
	const records = 100
	buildOldLayoutDB(t, path, records)

	// Simulate a crash after the copy committed but before the old rows were
	// dropped: create the state database, run the copy phase only.
	statePath := stateFilePath(path)
	state, err := sql.Open("sqlite", sqliteDSN(statePath, DurabilityFull))
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	if err := createStateSchema(state); err != nil {
		t.Fatalf("create state schema: %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close state db: %v", err)
	}

	main, err := sql.Open("sqlite", sqliteDSN(path, DurabilityFull))
	if err != nil {
		t.Fatalf("open main db: %v", err)
	}
	if err := copyStateRows(main, statePath); err != nil {
		t.Fatalf("copyStateRows: %v", err)
	}
	if err := main.Close(); err != nil {
		t.Fatalf("close main db: %v", err)
	}

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite after interrupted migration: %v", err)
	}
	defer s.Close()

	for i := 0; i < records; i++ {
		id := GrainId{GrainType: fmt.Sprintf("type-%d", i%4), GrainKey: fmt.Sprintf("key-%d", i)}
		record, err := s.Read(context.Background(), id)
		if err != nil {
			t.Fatalf("Read %#v: %v", id, err)
		}
		if want := fmt.Sprintf("data-%d", i); string(record.Data) != want {
			t.Fatalf("record %d data = %q, want %q", i, record.Data, want)
		}
		if want := ETag((i + 1) * 5); record.ETag != want {
			t.Fatalf("record %d ETag = %d, want %d", i, record.ETag, want)
		}
	}

	mainProbe := openRawSQLite(t, path)
	stateProbe := openRawSQLite(t, statePath)
	if has, err := tableExists(mainProbe, "records"); err != nil || has {
		t.Fatalf("main file records table after resumed migration: has=%v err=%v, want absent", has, err)
	}
	if count := tableRowCount(t, stateProbe, "records"); count != records {
		t.Fatalf("state file records count = %d, want %d (no duplication)", count, records)
	}
}

func TestMigrate_CountMismatchAbortsLeavingOldDatabaseIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")
	const records = 100
	buildOldLayoutDB(t, path, records)

	// A state database that already holds rows the old database never had
	// must fail the migration loudly instead of dropping anything.
	statePath := stateFilePath(path)
	state, err := sql.Open("sqlite", sqliteDSN(statePath, DurabilityFull))
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	if err := createStateSchema(state); err != nil {
		t.Fatalf("create state schema: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := state.Exec(`INSERT INTO records VALUES (?, ?, ?, ?)`, "foreign", fmt.Sprintf("key-%d", i), []byte("x"), 1); err != nil {
			t.Fatalf("seed foreign row: %v", err)
		}
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close state db: %v", err)
	}

	if _, err := OpenSQLite(path); err == nil {
		t.Fatalf("OpenSQLite: expected error for mismatched state database")
	}

	mainProbe := openRawSQLite(t, path)
	if count := tableRowCount(t, mainProbe, "records"); count != records {
		t.Fatalf("main file records count = %d after failed migration, want %d", count, records)
	}
	stateProbe := openRawSQLite(t, statePath)
	if count := tableRowCount(t, stateProbe, "records"); count != 5 {
		t.Fatalf("state file records count = %d after failed migration, want 5 (aborted copy rolled back)", count)
	}
}

func TestMigrate_OldDatabaseWithoutStateRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")
	buildOldLayoutDB(t, path, 0)

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer s.Close()

	reminders, err := s.ListDue(context.Background(), time.Unix(0, 100000).UTC())
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	if len(reminders) != 2 {
		t.Fatalf("ListDue returned %d reminders, want 2", len(reminders))
	}
	members, err := s.ListMembers(context.Background())
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	if len(members.Members) != 2 {
		t.Fatalf("ListMembers returned %d members, want 2", len(members.Members))
	}

	main := openRawSQLite(t, path)
	if has, err := tableExists(main, "records"); err != nil || has {
		t.Fatalf("main file records table after migration: has=%v err=%v, want absent", has, err)
	}
}

func TestMigrate_NewLayoutDatabaseIsNotMigrated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	if _, err := s.Write(context.Background(), GrainId{GrainType: "account", GrainKey: "alice"}, []byte("x"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	main := openRawSQLite(t, path)
	state := openRawSQLite(t, stateFilePath(path))
	if has, err := tableExists(main, "records"); err != nil || has {
		t.Fatalf("fresh main file has records table: has=%v err=%v, want absent", has, err)
	}
	if count := tableRowCount(t, state, "records"); count != 1 {
		t.Fatalf("state file records count = %d, want 1", count)
	}
	for _, table := range []string{"schedule", "member"} {
		if has, err := tableExists(state, table); err != nil || has {
			t.Fatalf("state file has %s table: has=%v err=%v, want absent", table, has, err)
		}
	}
	for _, table := range []string{"schedule", "member"} {
		if has, err := tableExists(main, table); err != nil || !has {
			t.Fatalf("main file missing %s table: has=%v err=%v, want present", table, has, err)
		}
	}
}

func TestMigrate_ErrorMentionsDatabasePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gor.db")
	buildOldLayoutDB(t, path, 10)

	// Break the state file so the copy phase fails and the open must fail
	// loudly, naming the database.
	statePath := stateFilePath(path)
	broken, err := sql.Open("sqlite", sqliteDSN(statePath, DurabilityFull))
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	if _, err := broken.Exec(`CREATE TABLE records (x TEXT)`); err != nil {
		t.Fatalf("create wrong-shaped records table: %v", err)
	}
	if err := broken.Close(); err != nil {
		t.Fatalf("close state db: %v", err)
	}

	_, err = OpenSQLite(path)
	if err == nil {
		t.Fatalf("OpenSQLite: expected error for broken state database")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("OpenSQLite error %q does not mention database path %q", err, path)
	}

	main := openRawSQLite(t, path)
	if count := tableRowCount(t, main, "records"); count != 10 {
		t.Fatalf("main file records count = %d after failed open, want 10 (old database intact)", count)
	}
}
