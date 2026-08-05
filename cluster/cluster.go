// Package cluster implements gor's membership, failure detection, and
// ownership view for clustered runtimes.
//
// It is an implementation package, not an application dependency. Configure
// clustering through gor.New and its cluster options, and use the root gor
// package for entity access.
package cluster

import (
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/suraciii/gor/store"
)

const virtualPointCount = 128

type View struct {
	points  []point
	members []MemberID
}

type point struct {
	hash       uint64
	nodeAddr   string
	generation string
	sequence   int
}

func NewView(snapshot []store.Member) View {
	members := make([]store.Member, 0, len(snapshot))
	for _, member := range snapshot {
		if member.Status == store.MemberActive {
			members = append(members, member)
		}
	}
	return View{points: buildPoints(members), members: activeMemberIDs(members)}
}

func Owner(view View, identity store.Identity) (string, bool) {
	if len(view.points) == 0 {
		return "", false
	}

	identityHash := hashParts(identity.Type, identity.Key)
	index := sort.Search(len(view.points), func(i int) bool {
		return view.points[i].hash >= identityHash
	})
	if index == len(view.points) {
		index = 0
	}
	return view.points[index].nodeAddr, true
}

func buildPoints(members []store.Member) []point {
	points := make([]point, 0, len(members)*virtualPointCount)
	for _, member := range members {
		for sequence := 0; sequence < virtualPointCount; sequence++ {
			points = append(points, point{
				hash:       hashParts(member.NodeAddr, member.Generation, strconv.Itoa(sequence)),
				nodeAddr:   member.NodeAddr,
				generation: member.Generation,
				sequence:   sequence,
			})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		if points[i].nodeAddr != points[j].nodeAddr {
			return points[i].nodeAddr < points[j].nodeAddr
		}
		if points[i].generation != points[j].generation {
			return points[i].generation < points[j].generation
		}
		return points[i].sequence < points[j].sequence
	})
	return points
}

func hashParts(parts ...string) uint64 {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hash.Sum64()
}
