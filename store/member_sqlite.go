package store

import (
	"context"
	"database/sql"
)

func (s *SQLite) WriteMember(ctx context.Context, member Member) (ETag, error) {
	var (
		result sql.Result
		err    error
	)
	if member.ETag == 0 {
		result, err = s.writeDB.ExecContext(ctx, `
INSERT INTO member (node_addr, generation, status, iam_alive_at, etag)
VALUES (?, ?, ?, ?, 1)
ON CONFLICT (node_addr, generation) DO NOTHING`,
			member.NodeAddr,
			member.Generation,
			string(member.Status),
			timeValue(member.IamAliveAt),
		)
	} else {
		result, err = s.writeDB.ExecContext(ctx, `
UPDATE member
SET status = ?, iam_alive_at = ?, etag = etag + 1
WHERE node_addr = ? AND generation = ? AND etag = ?`,
			string(member.Status),
			timeValue(member.IamAliveAt),
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

func (s *SQLite) ListMembers(ctx context.Context) ([]Member, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT node_addr, generation, status, iam_alive_at, etag
FROM member
ORDER BY node_addr, generation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]Member, 0)
	for rows.Next() {
		var (
			nodeAddr   string
			generation string
			status     string
			iamAliveAt int64
			etag       int64
		)
		if err := rows.Scan(&nodeAddr, &generation, &status, &iamAliveAt, &etag); err != nil {
			return nil, err
		}
		result = append(result, Member{
			NodeAddr:   nodeAddr,
			Generation: generation,
			Status:     MemberStatus(status),
			IamAliveAt: timeFromValue(iamAliveAt),
			ETag:       ETag(etag),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
