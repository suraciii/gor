package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStore_WriteWithMatchingETagReturnsNewETag(t *testing.T) {
	memory := NewMemory()
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	etag, err := memory.Write(context.Background(), id, []byte("first"), 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if etag != 1 {
		t.Fatalf("ETag = %d, want 1", etag)
	}

	record, err := memory.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != "first" || record.ETag != etag {
		t.Fatalf("Record = %#v, want data first and ETag %d", record, etag)
	}

	nextETag, err := memory.Write(context.Background(), id, []byte("second"), etag)
	if err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if nextETag != 2 {
		t.Fatalf("second ETag = %d, want 2", nextETag)
	}
}

func TestMemoryStore_ConflictLeavesRecordUnchanged(t *testing.T) {
	memory := NewMemory()
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	etag, err := memory.Write(context.Background(), id, []byte("original"), 0)
	if err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	newETag, err := memory.Write(context.Background(), id, []byte("replacement"), etag+1)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
	if newETag != 0 {
		t.Fatalf("conflicting ETag = %d, want 0", newETag)
	}

	record, err := memory.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(record.Data) != "original" || record.ETag != etag {
		t.Fatalf("Record after conflict = %#v, want original data and ETag %d", record, etag)
	}
}

func TestMemoryStore_ZeroETagConflictsWithExistingRecord(t *testing.T) {
	memory := NewMemory()
	id := GrainId{GrainType: "account", GrainKey: "alice"}

	if _, err := memory.Write(context.Background(), id, []byte("existing"), 0); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	if _, err := memory.Write(context.Background(), id, []byte("overwrite"), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
}

func TestMemoryStore_NonzeroETagConflictsWithMissingRecord(t *testing.T) {
	memory := NewMemory()
	id := GrainId{GrainType: "account", GrainKey: "missing"}

	if _, err := memory.Write(context.Background(), id, []byte("unexpected"), 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("Write error = %v, want ErrConflict", err)
	}
}

func TestMemoryStore_ReadMissingReturnsZeroRecord(t *testing.T) {
	memory := NewMemory()

	record, err := memory.Read(context.Background(), GrainId{GrainType: "account", GrainKey: "missing"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if record.Data != nil || record.ETag != 0 {
		t.Fatalf("Record = %#v, want zero Record", record)
	}
}

func TestMemoryStore_DifferentIdentitiesAreIndependent(t *testing.T) {
	memory := NewMemory()
	alice := GrainId{GrainType: "account", GrainKey: "alice"}
	bob := GrainId{GrainType: "account", GrainKey: "bob"}

	if _, err := memory.Write(context.Background(), alice, []byte("alice"), 0); err != nil {
		t.Fatalf("alice Write: %v", err)
	}
	if _, err := memory.Write(context.Background(), bob, []byte("bob"), 0); err != nil {
		t.Fatalf("bob Write: %v", err)
	}

	aliceRecord, err := memory.Read(context.Background(), alice)
	if err != nil {
		t.Fatalf("alice Read: %v", err)
	}
	bobRecord, err := memory.Read(context.Background(), bob)
	if err != nil {
		t.Fatalf("bob Read: %v", err)
	}
	if string(aliceRecord.Data) != "alice" || aliceRecord.ETag != 1 {
		t.Fatalf("alice Record = %#v", aliceRecord)
	}
	if string(bobRecord.Data) != "bob" || bobRecord.ETag != 1 {
		t.Fatalf("bob Record = %#v", bobRecord)
	}
}

func TestMemoryStore_ReadAndWriteCopyData(t *testing.T) {
	memory := NewMemory()
	id := GrainId{GrainType: "account", GrainKey: "alice"}
	data := []byte("original")

	if _, err := memory.Write(context.Background(), id, data, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	data[0] = 'X'

	record, err := memory.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	record.Data[0] = 'Y'

	unchanged, err := memory.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if string(unchanged.Data) != "original" {
		t.Fatalf("stored data = %q, want original", unchanged.Data)
	}
}

func TestMemoryStore_MethodsHonorCanceledContext(t *testing.T) {
	memory := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wantCanceled := func(err error) {
		t.Helper()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	}

	_, err := memory.Read(ctx, GrainId{GrainType: "account", GrainKey: "alice"})
	wantCanceled(err)
	_, err = memory.Write(ctx, GrainId{GrainType: "account", GrainKey: "alice"}, nil, 0)
	wantCanceled(err)
	_, err = memory.ListDue(ctx, time.Time{})
	wantCanceled(err)
	_, err = memory.Claim(ctx, Reminder{}, time.Time{})
	wantCanceled(err)
	err = memory.Put(ctx, Reminder{})
	wantCanceled(err)
	err = memory.Delete(ctx, GrainId{GrainType: "account", GrainKey: "alice"}, "daily")
	wantCanceled(err)
	_, err = memory.WriteMember(ctx, Member{NodeAddr: "node-a", Generation: "generation-a"})
	wantCanceled(err)
	_, err = memory.ListMembers(ctx)
	wantCanceled(err)
}
