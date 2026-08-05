package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/store"
)

var (
	ErrNodeDead       = errors.New("cluster node is dead")
	ErrProberRequired = errors.New("cluster prober is required")
)

type State uint8

const (
	StateJoining State = iota
	StateActive
	StateDead
)

type MemberID struct {
	NodeAddr   string `json:"node_addr"`
	Generation string `json:"generation"`
}

type Config struct {
	Table             store.MemberStore
	Clock             clock.Clock
	Prober            Prober
	NodeAddr          string
	Generation        string
	HeartbeatInterval time.Duration
	ViewInterval      time.Duration
	DeadAfter         time.Duration
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
}

type Node struct {
	table             store.MemberStore
	clock             clock.Clock
	prober            Prober
	nodeAddr          string
	generation        string
	heartbeatInterval time.Duration
	viewInterval      time.Duration
	deadAfter         time.Duration
	probeInterval     time.Duration
	probeTimeout      time.Duration

	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	views         chan View
	state         atomic.Uint32
	probeFailures map[MemberID]int
}

func New(config Config) (*Node, error) {
	if config.Prober == nil {
		return nil, ErrProberRequired
	}
	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		table:             config.Table,
		clock:             config.Clock,
		prober:            config.Prober,
		nodeAddr:          config.NodeAddr,
		generation:        config.Generation,
		heartbeatInterval: config.HeartbeatInterval,
		viewInterval:      config.ViewInterval,
		deadAfter:         config.DeadAfter,
		probeInterval:     config.ProbeInterval,
		probeTimeout:      config.ProbeTimeout,
		ctx:               ctx,
		cancel:            cancel,
		done:              make(chan struct{}),
		views:             make(chan View, 1),
		probeFailures:     make(map[MemberID]int),
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
	probeTicker := node.clock.NewTicker(node.probeInterval)
	go node.run(self, view, node.clock.NewTicker(node.heartbeatInterval), node.clock.NewTicker(node.viewInterval), probeTicker)
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

func (n *Node) Probe() (MemberID, bool) {
	if n.State() != StateActive {
		return MemberID{}, false
	}
	select {
	case <-n.done:
		return MemberID{}, false
	default:
		return MemberID{NodeAddr: n.nodeAddr, Generation: n.generation}, true
	}
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

func (n *Node) run(self store.Member, view View, heartbeat, viewTicker, probeTicker clock.Ticker) {
	defer heartbeat.Stop()
	defer viewTicker.Stop()
	defer probeTicker.Stop()
	defer close(n.views)
	defer close(n.done)

	probeEvents := make(chan probeEvent, 2)
	probeTasks := make(map[MemberID]probeTask)
	probeTick := probeTicker.C()
	var nextProbeToken uint64
	selfID := MemberID{NodeAddr: self.NodeAddr, Generation: self.Generation}
	defer func() {
		for _, task := range probeTasks {
			task.cancel()
		}
	}()

	for {
		select {
		case <-heartbeat.C():
			updated, deadView, alive := n.heartbeat(self)
			if !alive {
				n.notify(deadView)
				return
			}
			self = updated
		case <-viewTicker.C():
			updated, alive := n.pollView(self, view)
			reconcileProbeState(probeTargets(updated.members, selfID), probeTasks, n.probeFailures)
			changed := !sameView(view, updated)
			view = updated
			if changed {
				n.notify(view)
			}
			if !alive {
				return
			}
		case <-probeTick:
			targets := probeTargets(view.members, selfID)
			reconcileProbeState(targets, probeTasks, n.probeFailures)
			for _, target := range targets {
				if _, ok := probeTasks[target]; ok {
					continue
				}
				nextProbeToken++
				token := nextProbeToken
				probeTasks[target] = probeTask{
					cancel: n.startProbe(target, token, probeEvents),
					token:  token,
				}
			}
		case event := <-probeEvents:
			recordProbeEvent(event, probeTasks, n.probeFailures)
		case <-n.ctx.Done():
			n.leave(self)
			return
		}
	}
}

func (n *Node) startProbe(target MemberID, token uint64, events chan<- probeEvent) context.CancelFunc {
	ctx, cancel := context.WithCancel(n.ctx)
	go func() {
		defer cancel()
		result := waitForProbe(ctx, n.clock, n.prober, target, n.probeTimeout)
		select {
		case events <- probeEvent{target: target, token: token, result: result}:
		case <-n.ctx.Done():
		}
	}()
	return cancel
}

func (n *Node) heartbeat(self store.Member) (store.Member, View, bool) {
	updated := self
	updated.IamAliveAt = n.clock.Now()
	etag, err := n.table.WriteMember(n.ctx, updated)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			members, listErr := n.table.ListMembers(n.ctx)
			if listErr != nil {
				return self, View{}, true
			}
			index := memberIndex(members, self)
			if index < 0 || members[index].Status == store.MemberDead {
				n.state.Store(uint32(StateDead))
				return self, NewView(members), false
			}
			return members[index], View{}, true
		}
		return self, View{}, true
	}
	updated.ETag = etag
	return updated, View{}, true
}

func (n *Node) pollView(self store.Member, current View) (View, bool) {
	snapshot, err := n.table.ListMembers(n.ctx)
	if err != nil {
		return current, true
	}
	members := append([]store.Member(nil), snapshot...)
	now := n.clock.Now()
	for index := range members {
		member := members[index]
		if sameMember(member, self) {
			if member.Status == store.MemberDead {
				n.state.Store(uint32(StateDead))
				return NewView(members), false
			}
			continue
		}
		if member.Status == store.MemberDead || now.Sub(member.IamAliveAt) <= n.deadAfter {
			continue
		}

		candidate := member
		candidate.Status = store.MemberDead
		etag, err := n.table.WriteMember(n.ctx, candidate)
		if err != nil {
			continue
		}
		candidate.ETag = etag
		members[index] = candidate
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
