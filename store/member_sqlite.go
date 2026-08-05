package store

import (
	"context"
	"database/sql"
)

// WriteMember atomically creates or replaces a member row using its ETag.
// It returns the new ETag, or an error matching ErrConflict when the expected
// version does not match.
func (s *SQLite) WriteMember(ctx context.Context, member Member) (ETag, error) {
	var (
		result sql.Result
		err    error
	)
	votes, err := encodeSuspectVotes(member.SuspectVotes)
	if err != nil {
		return 0, err
	}
	if member.ETag == 0 {
		result, err = s.writeDB.ExecContext(ctx, `
INSERT INTO member (node_addr, generation, status, iam_alive_at, suspect_votes, etag)
VALUES (?, ?, ?, ?, ?, 1)
ON CONFLICT (node_addr, generation) DO NOTHING`,
			member.NodeAddr,
			member.Generation,
			string(member.Status),
			timeValue(member.IamAliveAt),
			string(votes),
		)
	} else {
		result, err = s.writeDB.ExecContext(ctx, `
UPDATE member
SET status = ?, iam_alive_at = ?, suspect_votes = ?, etag = etag + 1
WHERE node_addr = ? AND generation = ? AND etag = ?`,
			string(member.Status),
			timeValue(member.IamAliveAt),
			string(votes),
			member.NodeAddr,
			member.Generation,
			int64(member.ETag),
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
	return member.ETag + 1, nil
}

// ListMembers returns every member, sorted by address and generation, together
// with the current time from the configured clock.
func (s *SQLite) ListMembers(ctx context.Context) (MemberSnapshot, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT node_addr, generation, status, iam_alive_at, suspect_votes, etag
FROM member
ORDER BY node_addr, generation`)
	if err != nil {
		return MemberSnapshot{}, err
	}
	defer rows.Close()

	result := make([]Member, 0)
	for rows.Next() {
		var (
			nodeAddr     string
			generation   string
			status       string
			iamAliveAt   int64
			suspectVotes []byte
			etag         int64
		)
		if err := rows.Scan(&nodeAddr, &generation, &status, &iamAliveAt, &suspectVotes, &etag); err != nil {
			return MemberSnapshot{}, err
		}
		votes, err := decodeSuspectVotes(suspectVotes)
		if err != nil {
			return MemberSnapshot{}, err
		}
		result = append(result, Member{
			NodeAddr:     nodeAddr,
			Generation:   generation,
			Status:       MemberStatus(status),
			IamAliveAt:   timeFromValue(iamAliveAt),
			SuspectVotes: votes,
			ETag:         ETag(etag),
		})
	}
	if err := rows.Err(); err != nil {
		return MemberSnapshot{}, err
	}
	return MemberSnapshot{Members: result, TableNow: s.memberClock.Now()}, nil
}
