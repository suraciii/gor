package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

var ErrNodeDead = errors.New("cluster node is dead")

type State uint8

const (
	StateJoining State = iota
	StateActive
	StateDead
)

type Config struct {
	Table             store.MemberStore
	Clock             clock.Clock
	NodeAddr          string
	Generation        string
	HeartbeatInterval time.Duration
	ViewInterval      time.Duration
	DeadAfter         time.Duration
}

type Node struct {
	table             store.MemberStore
	clock             clock.Clock
	nodeAddr          string
	generation        string
	heartbeatInterval time.Duration
	viewInterval      time.Duration
	deadAfter         time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	views  chan View
	state  atomic.Uint32
}

func New(config Config) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		table:             config.Table,
		clock:             config.Clock,
		nodeAddr:          config.NodeAddr,
		generation:        config.Generation,
		heartbeatInterval: config.HeartbeatInterval,
		viewInterval:      config.ViewInterval,
		deadAfter:         config.DeadAfter,
		ctx:               ctx,
		cancel:            cancel,
		done:              make(chan struct{}),
		views:             make(chan View, 1),
	}
	node.state.Store(uint32(StateJoining))

	self, members, err := node.join()
	if err != nil {
		cancel()
		return nil, err
	}
	node.state.Store(uint32(StateActive))
	view := NewView(members)
	node.notify(view)
	go node.run(self, view)
	return node, nil
}

func (n *Node) State() State {
	return State(n.state.Load())
}

func (n *Node) ViewChanges() <-chan View {
	return n.views
}

func (n *Node) Done() <-chan struct{} {
	return n.done
}

func (n *Node) Close() {
	n.cancel()
	<-n.done
}

func (n *Node) Kill() {
	n.state.Store(uint32(StateDead))
	n.cancel()
	<-n.done
}

func (n *Node) join() (store.Member, []store.Member, error) {
	self := store.Member{
		NodeAddr:   n.nodeAddr,
		Generation: n.generation,
		Status:     store.MemberJoining,
		IamAliveAt: n.clock.Now(),
	}
	if _, err := n.table.WriteMember(context.Background(), self); err != nil {
		return store.Member{}, nil, err
	}

	members, err := n.table.ListMembers(context.Background())
	if err != nil {
		return store.Member{}, nil, err
	}
	index := memberIndex(members, self)
	if index < 0 || members[index].Status != store.MemberJoining {
		return store.Member{}, nil, ErrNodeDead
	}

	self = members[index]
	self.Status = store.MemberActive
	self.IamAliveAt = n.clock.Now()
	etag, err := n.table.WriteMember(context.Background(), self)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return store.Member{}, nil, ErrNodeDead
		}
		return store.Member{}, nil, err
	}
	self.ETag = etag
	members[index] = self
	return self, members, nil
}

func (n *Node) run(self store.Member, view View) {
	heartbeat := n.clock.NewTicker(n.heartbeatInterval)
	viewTicker := n.clock.NewTicker(n.viewInterval)
	defer heartbeat.Stop()
	defer viewTicker.Stop()
	defer close(n.views)
	defer close(n.done)

	for {
		select {
		case <-heartbeat.C():
			updated, alive := n.heartbeat(self)
			if !alive {
				return
			}
			self = updated
		case <-viewTicker.C():
			updated, alive := n.pollView(self, view)
			if !alive {
				return
			}
			if !sameView(view, updated) {
				view = updated
				n.notify(view)
			}
		case <-n.ctx.Done():
			n.leave(self)
			return
		}
	}
}

func (n *Node) heartbeat(self store.Member) (store.Member, bool) {
	updated := self
	updated.IamAliveAt = n.clock.Now()
	etag, err := n.table.WriteMember(n.ctx, updated)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			members, listErr := n.table.ListMembers(n.ctx)
			if listErr != nil {
				return self, true
			}
			index := memberIndex(members, self)
			if index < 0 || members[index].Status == store.MemberDead {
				n.state.Store(uint32(StateDead))
				return self, false
			}
			return members[index], true
		}
		return self, true
	}
	updated.ETag = etag
	return updated, true
}

func (n *Node) pollView(self store.Member, current View) (View, bool) {
	members, err := n.table.ListMembers(n.ctx)
	if err != nil {
		return current, true
	}
	now := n.clock.Now()
	for index := range members {
		member := members[index]
		if sameMember(member, self) {
			if member.Status == store.MemberDead {
				n.state.Store(uint32(StateDead))
				return current, false
			}
			continue
		}
		if member.Status == store.MemberDead || now.Sub(member.IamAliveAt) <= n.deadAfter {
			continue
		}

		member.Status = store.MemberDead
		etag, err := n.table.WriteMember(n.ctx, member)
		if err == nil {
			member.ETag = etag
			members[index] = member
		}
	}
	return NewView(members), true
}

func (n *Node) leave(self store.Member) {
	if n.State() == StateDead {
		return
	}
	self.Status = store.MemberDead
	_, _ = n.table.WriteMember(context.Background(), self)
	n.state.Store(uint32(StateDead))
}

func (n *Node) notify(view View) {
	select {
	case <-n.views:
	default:
	}
	n.views <- view
}

func sameView(left, right View) bool {
	if len(left.points) != len(right.points) {
		return false
	}
	for index := range left.points {
		if left.points[index] != right.points[index] {
			return false
		}
	}
	return true
}

func memberIndex(members []store.Member, target store.Member) int {
	for index, member := range members {
		if sameMember(member, target) {
			return index
		}
	}
	return -1
}

func sameMember(left, right store.Member) bool {
	return left.NodeAddr == right.NodeAddr && left.Generation == right.Generation
}
