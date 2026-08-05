package cluster

import (
	"errors"
	"time"

	"github.com/suraciii/gor/store"
)

func activeSuspectVotes(votes map[MemberID]store.SuspectVote, now time.Time) map[MemberID]store.SuspectVote {
	if len(votes) == 0 {
		return nil
	}
	active := make(map[MemberID]store.SuspectVote, len(votes))
	for voter, vote := range votes {
		if vote.ExpiresAt.After(now) {
			active[voter] = vote
		}
	}
	if len(active) == 0 {
		return nil
	}
	return active
}

func voteThreshold(activeCount int) int {
	if activeCount <= 1 {
		return 0
	}
	if activeCount-1 < 2 {
		return activeCount - 1
	}
	return 2
}

func targetNeighbors(members []store.Member, target MemberID) []MemberID {
	return probeTargets(activeMemberIDs(members), target)
}

func shouldMarkDead(snapshot store.MemberSnapshot, target MemberID) bool {
	active := activeMemberIDs(snapshot.Members)
	threshold := voteThreshold(len(active))
	if threshold == 0 {
		return false
	}
	index := memberIndex(snapshot.Members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})
	if index < 0 || snapshot.Members[index].Status != store.MemberActive {
		return false
	}
	neighbors := targetNeighbors(snapshot.Members, target)
	validNeighbors := make(map[MemberID]struct{}, len(neighbors))
	for _, neighbor := range neighbors {
		validNeighbors[neighbor] = struct{}{}
	}
	count := 0
	for voter, vote := range snapshot.Members[index].SuspectVotes {
		if _, ok := validNeighbors[voter]; ok && vote.ExpiresAt.After(snapshot.TableNow) {
			count++
		}
	}
	return count >= threshold
}

func (n *Node) updateSuspectVote(target, voter MemberID, renew bool) {
	for {
		snapshot, err := n.table.ListMembers(n.ctx)
		if err != nil {
			return
		}
		members := snapshot.Members
		index := memberIndex(members, store.Member{NodeAddr: target.NodeAddr, Generation: target.Generation})
		if index < 0 || members[index].Status != store.MemberActive {
			return
		}
		if renew {
			neighbors := targetNeighbors(members, target)
			if !containsMemberID(neighbors, voter) {
				return
			}
		}

		row := members[index]
		votes := activeSuspectVotes(row.SuspectVotes, snapshot.TableNow)
		if renew {
			if votes == nil {
				votes = make(map[MemberID]store.SuspectVote)
			}
			votes[voter] = store.SuspectVote{ExpiresAt: snapshot.TableNow.Add(n.voteTTL)}
		} else {
			delete(votes, voter)
		}
		if !sameSuspectVotes(row.SuspectVotes, votes) {
			row.SuspectVotes = votes
			etag, err := n.table.WriteMember(n.ctx, row)
			if err != nil {
				if errors.Is(err, store.ErrConflict) {
					continue
				}
				return
			}
			row.ETag = etag
			members[index] = row
		}
		if !renew || !shouldMarkDead(store.MemberSnapshot{Members: members, TableNow: snapshot.TableNow}, target) {
			return
		}
		row.Status = store.MemberDead
		if _, err := n.table.WriteMember(n.ctx, row); err != nil {
			if errors.Is(err, store.ErrConflict) {
				continue
			}
		}
		return
	}
}

func sameSuspectVotes(left, right map[MemberID]store.SuspectVote) bool {
	if len(left) != len(right) {
		return false
	}
	for voter, vote := range left {
		if right[voter] != vote {
			return false
		}
	}
	return true
}

func containsMemberID(members []MemberID, target MemberID) bool {
	for _, member := range members {
		if member == target {
			return true
		}
	}
	return false
}

func (n *Node) handleProbeEvent(event probeEvent, tasks map[MemberID]probeTask, self MemberID) {
	if task, ok := tasks[event.target]; !ok || task.token != event.token {
		return
	}
	success := recordProbeEvent(event, tasks, n.probeFailures)
	if n.health == unhealthy {
		clear(n.probeFailures)
		if success {
			n.updateSuspectVote(event.target, self, false)
		}
		return
	}
	if success {
		n.updateSuspectVote(event.target, self, false)
		return
	}
	if n.probeFailures[event.target] >= n.probeFailureLimit {
		n.updateSuspectVote(event.target, self, true)
	}
}
