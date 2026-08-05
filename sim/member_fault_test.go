//go:build sim

package sim

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/suraciii/gor/store"
)

func TestSim_DelayedMemberWriteFindsDeadRow(t *testing.T) {
	stats := runDelayedMemberWriteScenario(t)
	if stats.delayedDeadCAS != 1 {
		t.Fatalf("delayed-dead-cas = %d, want 1", stats.delayedDeadCAS)
	}
}

func runDelayedMemberWriteScenario(t *testing.T) memberStats {
	t.Helper()
	var stats memberStats
	synctest.Test(t, func(t *testing.T) {
		backend := newFakeStore(newTimerTracker())
		member := store.Member{
			NodeAddr:   "node-a",
			Generation: "generation-a",
			Status:     store.MemberJoining,
			IamAliveAt: time.Unix(0, 0).UTC(),
		}
		etag, err := backend.WriteMember(context.Background(), member)
		if err != nil {
			t.Fatalf("write joining member: %v", err)
		}
		member.ETag = etag
		member.Status = store.MemberActive
		etag, err = backend.WriteMember(context.Background(), member)
		if err != nil {
			t.Fatalf("write active member: %v", err)
		}
		member.ETag = etag

		started := make(chan struct{})
		backend.setMemberFault(memberFaultSpec{
			kind:    memberDelay,
			delay:   time.Millisecond,
			started: started,
		})
		result := make(chan error, 1)
		go func() {
			_, writeErr := backend.WriteMember(context.Background(), member)
			result <- writeErr
		}()
		<-started

		dead := member
		dead.Status = store.MemberDead
		if _, err := backend.WriteMember(context.Background(), dead); err != nil {
			t.Fatalf("mark member dead: %v", err)
		}
		backend.waitForIdle()
		if err := <-result; !errors.Is(err, store.ErrConflict) {
			t.Fatalf("delayed write error = %v, want ErrConflict", err)
		}
		stats = backend.memberStatsSnapshot()
		members, err := backend.ListMembers(context.Background())
		if err != nil {
			t.Fatalf("list members: %v", err)
		}
		if len(members) != 1 || members[0].Status != store.MemberDead {
			t.Fatalf("member after delayed write = %#v, want one dead row", members)
		}
	})
	return stats
}
