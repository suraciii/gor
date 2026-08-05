package store

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// MemberStatus is the lifecycle state of a cluster member.
type MemberStatus string

// MemberJoining, MemberActive, and MemberDead are the supported membership
// states.
const (
	MemberJoining MemberStatus = "joining"
	MemberActive  MemberStatus = "active"
	MemberDead    MemberStatus = "dead"
)

// MemberID identifies one member incarnation. A new Generation distinguishes
// a later incarnation that reuses the same NodeAddr.
type MemberID struct {
	NodeAddr   string `json:"node_addr"`
	Generation string `json:"generation"`
}

// SuspectVote records when a member's suspicion of another member expires.
type SuspectVote struct {
	ExpiresAt time.Time
}

// Member is one row in the cluster membership table.
//
// NodeAddr and Generation form the row identity. ETag must be the version
// returned by the latest successful write or list operation when updating an
// existing row; zero requests creation of a row that does not exist.
type Member struct {
	NodeAddr     string
	Generation   string
	Status       MemberStatus
	IamAliveAt   time.Time
	SuspectVotes map[MemberID]SuspectVote
	ETag         ETag
}

// MemberSnapshot is a complete membership-table view and the table's current
// time from the configured member clock.
type MemberSnapshot struct {
	Members  []Member
	TableNow time.Time
}

// MemberStore persists cluster membership rows.
//
// Implementations must support concurrent calls and the same atomic ETag
// compare-and-swap rule as Store. WriteMember must return an error matching
// ErrConflict when the supplied row ETag is stale, and ListMembers must return
// independent snapshot data rather than mutable storage-owned maps.
type MemberStore interface {
	// WriteMember creates or replaces one member row using member.ETag as the
	// expected version and returns the new ETag.
	WriteMember(context.Context, Member) (ETag, error)
	// ListMembers returns all member rows and the backend's current table time.
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

// WriteMember atomically creates or replaces a member row using its ETag.
// It returns the new ETag, or an error matching ErrConflict when the expected
// version does not match.
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

// ListMembers returns a copy of every member, sorted by address and
// generation, together with the current time from the configured clock.
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
