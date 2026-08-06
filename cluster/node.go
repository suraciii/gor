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
	ErrNodeDead           = errors.New("cluster node is dead")
	ErrProberRequired     = errors.New("cluster prober is required")
	ErrInvalidConfig      = errors.New("cluster config is invalid")
	errMemberCheckTimeout = errors.New("cluster member check timed out")
)

type State uint8

const (
	StateJoining State = iota
	StateActive
	StateDead
)

type healthState uint8

const (
	healthy healthState = iota
	unhealthy
)

type MemberID = store.MemberID

const (
	defaultProbeInterval   = time.Second
	defaultProbeTimeout    = 500 * time.Millisecond
	defaultProbeFailures   = 3
	defaultVoteTTL         = 6 * time.Second
	defaultMaxTickGap      = 2 * time.Second
	defaultMaxTableLatency = 500 * time.Millisecond
)

type Config struct {
	Table             store.MemberStore
	Clock             clock.Clock
	Prober            Prober
	NodeAddr          string
	Generation        string
	HeartbeatInterval time.Duration
	ViewInterval      time.Duration
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	ProbeFailures     int
	VoteTTL           time.Duration
	MaxTickGap        time.Duration
	MaxTableLatency   time.Duration
}

type Node struct {
	table             store.MemberStore
	clock             clock.Clock
	prober            Prober
	nodeAddr          string
	generation        string
	heartbeatInterval time.Duration
	viewInterval      time.Duration
	probeInterval     time.Duration
	probeTimeout      time.Duration
	probeFailureLimit int
	voteTTL           time.Duration
	maxTickGap        time.Duration
	maxTableLatency   time.Duration

	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	declaredDead  chan struct{}
	views         chan View
	state         atomic.Uint32
	probeFailures map[MemberID]int
	health        healthState
	healthyTicks  int
	lastProbeAt   time.Time
}

func New(config Config) (*Node, error) {
	if config.Prober == nil {
		return nil, ErrProberRequired
	}
	if config.ProbeInterval < 0 || config.ProbeTimeout < 0 || config.ProbeFailures < 0 || config.VoteTTL < 0 || config.MaxTickGap < 0 || config.MaxTableLatency < 0 {
		return nil, ErrInvalidConfig
	}
	if config.ProbeInterval == 0 {
		config.ProbeInterval = defaultProbeInterval
	}
	if config.ProbeTimeout == 0 {
		config.ProbeTimeout = defaultProbeTimeout
	}
	if config.ProbeFailures == 0 {
		config.ProbeFailures = defaultProbeFailures
	}
	if config.VoteTTL == 0 {
		config.VoteTTL = defaultVoteTTL
	}
	if config.MaxTickGap == 0 {
		config.MaxTickGap = defaultMaxTickGap
	}
	if config.MaxTableLatency == 0 {
		config.MaxTableLatency = defaultMaxTableLatency
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
		probeInterval:     config.ProbeInterval,
		probeTimeout:      config.ProbeTimeout,
		probeFailureLimit: config.ProbeFailures,
		voteTTL:           config.VoteTTL,
		maxTickGap:        config.MaxTickGap,
		maxTableLatency:   config.MaxTableLatency,
		ctx:               ctx,
		cancel:            cancel,
		done:              make(chan struct{}),
		declaredDead:      make(chan struct{}),
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

// DeclaredDead returns a channel that closes only when the node stopped
// because the cluster declared it dead, not because of a voluntary Close or
// Kill. The root runtime uses it to distinguish external death from a stop it
// initiated itself, rather than guessing from Done closing.
func (n *Node) DeclaredDead() <-chan struct{} {
	return n.declaredDead
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

	snapshot, err := n.table.ListMembers(context.Background())
	if err != nil {
		return store.Member{}, nil, err
	}
	members := snapshot.Members
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
	var externalDeath bool
	defer func() {
		if externalDeath {
			close(n.declaredDead)
		}
	}()

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
			updated, _, alive := n.heartbeat(self)
			if !alive {
				externalDeath = true
				return
			}
			self = updated
		case <-viewTicker.C():
			updated, alive := n.pollView(self, view)
			if !alive {
				externalDeath = true
				return
			}
			reconcileProbeState(probeTargets(updated.members, selfID), probeTasks, n.probeFailures)
			changed := !sameView(view, updated)
			view = updated
			if changed {
				n.notify(view)
			}
		case probeAt := <-probeTick:
			updated, updatedView, alive, checkOK := n.selfCheck(self, view, probeAt)
			self = updated
			if !alive {
				externalDeath = true
				return
			}
			if !sameView(view, updatedView) {
				view = updatedView
				n.notify(view)
			}
			n.updateHealth(checkOK)
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
			n.handleProbeEvent(event, probeTasks, selfID)
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

func (n *Node) selfCheck(self store.Member, current View, probeAt time.Time) (store.Member, View, bool, bool) {
	checkOK := n.checkProbeInterval(probeAt)

	listStarted := n.clock.Now()
	snapshot, listErr := n.listMembersForCheck()
	listElapsed := n.clock.Now().Sub(listStarted)
	if listErr != nil || listElapsed < 0 || listElapsed > n.maxTableLatency {
		checkOK = false
	} else {
		members := snapshot.Members
		index := memberIndex(members, self)
		if index < 0 || members[index].Status == store.MemberDead {
			n.state.Store(uint32(StateDead))
			return self, NewView(members), false, false
		}
		self = members[index]
		current = NewView(members)
	}

	updated := self
	updated.IamAliveAt = n.clock.Now()
	if listErr == nil {
		updated.SuspectVotes = activeSuspectVotes(updated.SuspectVotes, snapshot.TableNow)
	}
	writeStarted := n.clock.Now()
	etag, writeErr := n.writeMemberForCheck(updated)
	writeElapsed := n.clock.Now().Sub(writeStarted)
	if writeElapsed < 0 || writeElapsed > n.maxTableLatency {
		checkOK = false
	}
	switch {
	case writeErr == nil:
		updated.ETag = etag
		self = updated
	case errors.Is(writeErr, store.ErrConflict):
		refreshStarted := n.clock.Now()
		refreshed, refreshErr := n.listMembersForCheck()
		if refreshErr != nil {
			checkOK = false
			break
		}
		refreshElapsed := n.clock.Now().Sub(refreshStarted)
		if refreshElapsed < 0 || refreshElapsed > n.maxTableLatency {
			checkOK = false
		}
		members := refreshed.Members
		index := memberIndex(members, self)
		if index < 0 || members[index].Status == store.MemberDead {
			n.state.Store(uint32(StateDead))
			return self, NewView(members), false, false
		}
		self = members[index]
		current = NewView(members)
	default:
		checkOK = false
	}
	return self, current, true, checkOK
}

func (n *Node) listMembersForCheck() (store.MemberSnapshot, error) {
	result := make(chan struct {
		snapshot store.MemberSnapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := n.table.ListMembers(n.ctx)
		result <- struct {
			snapshot store.MemberSnapshot
			err      error
		}{snapshot: snapshot, err: err}
	}()
	timer := n.clock.NewTicker(n.maxTableLatency)
	defer timer.Stop()
	select {
	case completed := <-result:
		return completed.snapshot, completed.err
	case <-timer.C():
		return store.MemberSnapshot{}, errMemberCheckTimeout
	case <-n.ctx.Done():
		return store.MemberSnapshot{}, n.ctx.Err()
	}
}

func (n *Node) writeMemberForCheck(member store.Member) (store.ETag, error) {
	result := make(chan struct {
		etag store.ETag
		err  error
	}, 1)
	go func() {
		etag, err := n.table.WriteMember(n.ctx, member)
		result <- struct {
			etag store.ETag
			err  error
		}{etag: etag, err: err}
	}()
	timer := n.clock.NewTicker(n.maxTableLatency)
	defer timer.Stop()
	select {
	case completed := <-result:
		return completed.etag, completed.err
	case <-timer.C():
		return 0, errMemberCheckTimeout
	case <-n.ctx.Done():
		return 0, n.ctx.Err()
	}
}

func (n *Node) checkProbeInterval(probeAt time.Time) bool {
	previous := n.lastProbeAt
	n.lastProbeAt = probeAt
	if previous.IsZero() {
		return true
	}
	delta := probeAt.Sub(previous)
	return delta > 0 && delta <= n.maxTickGap
}

func (n *Node) updateHealth(checkOK bool) {
	if !checkOK {
		n.health = unhealthy
		n.healthyTicks = 0
		clear(n.probeFailures)
		return
	}
	if n.health != unhealthy {
		return
	}
	n.healthyTicks++
	if n.healthyTicks == 3 {
		n.health = healthy
	}
}

func (n *Node) heartbeat(self store.Member) (store.Member, View, bool) {
	snapshot, err := n.table.ListMembers(n.ctx)
	if err != nil {
		return self, View{}, true
	}
	members := snapshot.Members
	index := memberIndex(members, self)
	if index < 0 || members[index].Status == store.MemberDead {
		n.state.Store(uint32(StateDead))
		return self, NewView(members), false
	}
	self = members[index]
	updated := self
	updated.IamAliveAt = n.clock.Now()
	updated.SuspectVotes = activeSuspectVotes(updated.SuspectVotes, snapshot.TableNow)
	etag, err := n.table.WriteMember(n.ctx, updated)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			snapshot, listErr := n.table.ListMembers(n.ctx)
			if listErr != nil {
				return self, View{}, true
			}
			members := snapshot.Members
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
	members := append([]store.Member(nil), snapshot.Members...)
	index := memberIndex(members, self)
	if index >= 0 && members[index].Status == store.MemberDead {
		n.state.Store(uint32(StateDead))
		return NewView(members), false
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
