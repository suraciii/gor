package store

import (
	"context"
	"sort"
	"time"
)

type MemberStatus string

const (
	MemberJoining MemberStatus = "joining"
	MemberActive  MemberStatus = "active"
	MemberDead    MemberStatus = "dead"
)

type Member struct {
	NodeAddr   string
	Generation string
	Status     MemberStatus
	IamAliveAt time.Time
	ETag       ETag
}

type MemberStore interface {
	WriteMember(context.Context, Member) (ETag, error)
	ListMembers(context.Context) ([]Member, error)
}

type memberKey struct {
	nodeAddr   string
	generation string
}

func keyForMember(member Member) memberKey {
	return memberKey{nodeAddr: member.NodeAddr, generation: member.Generation}
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
	m.members[key] = member
	return member.ETag, nil
}

func (m *Memory) ListMembers(ctx context.Context) ([]Member, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Member, 0, len(m.members))
	for _, member := range m.members {
		result = append(result, member)
	}
	sortMembers(result)
	return result, nil
}
