package store

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

type MemberStatus string

const (
	MemberJoining MemberStatus = "joining"
	MemberActive  MemberStatus = "active"
	MemberDead    MemberStatus = "dead"
)

type MemberID struct {
	NodeAddr   string `json:"node_addr"`
	Generation string `json:"generation"`
}

type SuspectVote struct {
	ExpiresAt time.Time
}

type Member struct {
	NodeAddr     string
	Generation   string
	Status       MemberStatus
	IamAliveAt   time.Time
	SuspectVotes map[MemberID]SuspectVote
	ETag         ETag
}

type MemberSnapshot struct {
	Members  []Member
	TableNow time.Time
}

type MemberStore interface {
	WriteMember(context.Context, Member) (ETag, error)
	ListMembers(context.Context) (MemberSnapshot, error)
}

type memberKey struct {
	nodeAddr   string
	generation string
}

func keyForMember(member Member) memberKey {
	return memberKey{nodeAddr: member.NodeAddr, generation: member.Generation}
}

func cloneMember(member Member) Member {
	if member.SuspectVotes != nil {
		votes := member.SuspectVotes
		member.SuspectVotes = make(map[MemberID]SuspectVote, len(member.SuspectVotes))
		for id, vote := range votes {
			member.SuspectVotes[id] = vote
		}
	}
	return member
}

type suspectVoteValue struct {
	NodeAddr   string `json:"node_addr"`
	Generation string `json:"generation"`
	ExpiresAt  int64  `json:"expires_at"`
}

func encodeSuspectVotes(votes map[MemberID]SuspectVote) ([]byte, error) {
	values := make([]suspectVoteValue, 0, len(votes))
	for id, vote := range votes {
		values = append(values, suspectVoteValue{
			NodeAddr:   id.NodeAddr,
			Generation: id.Generation,
			ExpiresAt:  vote.ExpiresAt.UnixNano(),
		})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].NodeAddr != values[j].NodeAddr {
			return values[i].NodeAddr < values[j].NodeAddr
		}
		return values[i].Generation < values[j].Generation
	})
	return json.Marshal(values)
}

func decodeSuspectVotes(data []byte) (map[MemberID]SuspectVote, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var values []suspectVoteValue
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	votes := make(map[MemberID]SuspectVote, len(values))
	for _, value := range values {
		votes[MemberID{NodeAddr: value.NodeAddr, Generation: value.Generation}] = SuspectVote{
			ExpiresAt: time.Unix(0, value.ExpiresAt).UTC(),
		}
	}
	return votes, nil
}

func sortMembers(members []Member) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].NodeAddr != members[j].NodeAddr {
			return members[i].NodeAddr < members[j].NodeAddr
		}
		return members[i].Generation < members[j].Generation
	})
}

func (m *Memory) WriteMember(ctx context.Context, member Member) (ETag, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	key := keyForMember(member)
	current := m.members[key]
	if current.ETag != member.ETag {
		return 0, ErrConflict
	}

	member.ETag = current.ETag + 1
	m.members[key] = cloneMember(member)
	return member.ETag, nil
}

func (m *Memory) ListMembers(ctx context.Context) (MemberSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MemberSnapshot{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Member, 0, len(m.members))
	for _, member := range m.members {
		result = append(result, cloneMember(member))
	}
	sortMembers(result)
	return MemberSnapshot{Members: result, TableNow: m.memberClock.Now()}, nil
}
