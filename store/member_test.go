package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestMemoryMemberStore(t *testing.T) {
	runMemberStoreTests(t, func(*testing.T) MemberStore {
		return NewMemory()
	})
}

func TestSQLiteMemberStore(t *testing.T) {
	runMemberStoreTests(t, func(t *testing.T) MemberStore {
		return newSQLiteTestStore(t)
	})
}

func runMemberStoreTests(t *testing.T, newBackend func(*testing.T) MemberStore) {
	t.Helper()
	t.Run("WriteAndListAll", func(t *testing.T) {
		backend := newBackend(t)
		ctx := context.Background()
		aliveAt := time.Unix(100, 123).UTC()
		members := []Member{
			{
				NodeAddr:   "node-b",
				Generation: "second",
				Status:     MemberDead,
				IamAliveAt: aliveAt.Add(time.Second),
			},
			{
				NodeAddr:   "node-a",
				Generation: "first",
				Status:     MemberActive,
				IamAliveAt: aliveAt,
			},
			{
				NodeAddr:   "node-a",
				Generation: "second",
				Status:     MemberJoining,
				IamAliveAt: aliveAt.Add(-time.Second),
			},
		}
		for i, member := range members {
			etag, err := backend.WriteMember(ctx, member)
			if err != nil {
				t.Fatalf("WriteMember %d: %v", i, err)
			}
			if etag != 1 {
				t.Fatalf("WriteMember %d ETag = %d, want 1", i, etag)
			}
		}

		got, err := backend.ListMembers(ctx)
		if err != nil {
			t.Fatalf("ListMembers: %v", err)
		}
		want := []Member{
			members[1],
			members[2],
			members[0],
		}
		for i := range want {
			want[i].ETag = 1
		}
		if len(got.Members) != len(want) {
			t.Fatalf("ListMembers returned %d rows, want %d: %#v", len(got.Members), len(want), got)
		}
		for i := range want {
			if !reflect.DeepEqual(got.Members[i], want[i]) {
				t.Fatalf("member %d = %#v, want %#v", i, got.Members[i], want[i])
			}
		}
	})

	t.Run("CASRejectsStaleAndZeroETagWrites", func(t *testing.T) {
		backend := newBackend(t)
		ctx := context.Background()
		member := Member{
			NodeAddr:   "node-cas",
			Generation: "generation",
			Status:     MemberJoining,
			IamAliveAt: time.Unix(200, 0).UTC(),
		}
		etag, err := backend.WriteMember(ctx, member)
		if err != nil {
			t.Fatalf("initial WriteMember: %v", err)
		}
		if etag != 1 {
			t.Fatalf("initial ETag = %d, want 1", etag)
		}

		if _, err := backend.WriteMember(ctx, member); !errors.Is(err, ErrConflict) {
			t.Fatalf("zero ETag replacement error = %v, want ErrConflict", err)
		}

		member.Status = MemberActive
		member.ETag = etag + 1
		if _, err := backend.WriteMember(ctx, member); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale ETag update error = %v, want ErrConflict", err)
		}

		got, err := backend.ListMembers(ctx)
		if err != nil {
			t.Fatalf("ListMembers after conflicts: %v", err)
		}
		var found *Member
		for i := range got.Members {
			if got.Members[i].NodeAddr == member.NodeAddr && got.Members[i].Generation == member.Generation {
				found = &got.Members[i]
				break
			}
		}
		if found == nil || found.Status != MemberJoining || found.ETag != etag {
			t.Fatalf("member after conflicts = %#v, want joining with ETag %d", got, etag)
		}
	})

	t.Run("CASAllowsExactlyOneWriter", func(t *testing.T) {
		backend := newBackend(t)
		ctx := context.Background()
		member := Member{
			NodeAddr:   "node-race",
			Generation: "generation",
			Status:     MemberJoining,
			IamAliveAt: time.Unix(300, 0).UTC(),
		}
		etag, err := backend.WriteMember(ctx, member)
		if err != nil {
			t.Fatalf("initial WriteMember: %v", err)
		}
		member.ETag = etag
		member.Status = MemberActive

		start := make(chan struct{})
		results := make(chan error, 2)
		var wait sync.WaitGroup
		for range 2 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				_, err := backend.WriteMember(ctx, member)
				results <- err
			}()
		}
		close(start)
		wait.Wait()
		close(results)

		wins := 0
		conflicts := 0
		for err := range results {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				t.Fatalf("WriteMember: %v", err)
			}
		}
		if wins != 1 || conflicts != 1 {
			t.Fatalf("CAS results = wins %d, conflicts %d; want one each", wins, conflicts)
		}

		got, err := backend.ListMembers(ctx)
		if err != nil {
			t.Fatalf("ListMembers after race: %v", err)
		}
		var found *Member
		for i := range got.Members {
			if got.Members[i].NodeAddr == member.NodeAddr && got.Members[i].Generation == member.Generation {
				found = &got.Members[i]
				break
			}
		}
		if found == nil || found.Status != MemberActive || found.ETag != etag+1 {
			t.Fatalf("member after race = %#v, want active with ETag %d", got, etag+1)
		}
	})
}

func TestMemoryStoresSuspectVotes(t *testing.T) {
	assertSuspectVotesPersist(t, NewMemory())
}

func TestSQLiteStoresSuspectVotes(t *testing.T) {
	assertSuspectVotesPersist(t, newSQLiteTestStore(t))
}

func assertSuspectVotesPersist(t *testing.T, backend MemberStore) {
	t.Helper()
	member := Member{NodeAddr: "node-a", Generation: "generation-a", Status: MemberActive, IamAliveAt: time.Unix(0, 0).UTC()}
	if _, err := backend.WriteMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	snapshot, err := backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	member = snapshot.Members[0]
	member.SuspectVotes = map[MemberID]SuspectVote{{NodeAddr: "node-b", Generation: "generation-b"}: {ExpiresAt: time.Unix(1, 0).UTC()}}
	if _, err := backend.WriteMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.ListMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Members[0].SuspectVotes) != 1 {
		t.Fatalf("stored votes = %#v", snapshot.Members[0].SuspectVotes)
	}
}
