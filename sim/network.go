//go:build sim

package sim

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suraciii/gor/store"
	"github.com/suraciii/gor/transport"
)

var (
	errSimNetworkPartition = errors.New("sim network partition")
	errSimNetworkClosed    = errors.New("sim network closed")
)

type networkPair struct {
	source      string
	destination string
}

type simulationNetwork struct {
	backend *fakeStore

	mu               sync.Mutex
	transports       map[string]*simulationTransport
	memberStore      map[string]*partitionedMemberStore
	groups           map[string]int
	partitioned      bool
	delays           map[networkPair]time.Duration
	activeDeliveries int
	deliveriesIdle   chan struct{}
	sends            atomic.Int64
	delivered        atomic.Int64
	dropped          atomic.Int64
	held             atomic.Int64
	completed        atomic.Int64
}

func newSimulationNetwork(backend *fakeStore) *simulationNetwork {
	idle := make(chan struct{})
	close(idle)
	return &simulationNetwork{
		backend:        backend,
		transports:     make(map[string]*simulationTransport),
		memberStore:    make(map[string]*partitionedMemberStore),
		deliveriesIdle: idle,
	}
}

// setDelays arms per-pair message delays. A message sent from source to
// destination while a non-zero delay is armed is held for that delay before
// its handler runs. The delay is fake-clock time: it elapses only while the
// driver is durably blocked, and the bubble clock releases the message, so a
// held message can never be released by a goroutine sleep the scheduler
// happens to honor.
func (n *simulationNetwork) setDelays(delays map[networkPair]time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delays = make(map[networkPair]time.Duration, len(delays))
	for pair, delay := range delays {
		n.delays[pair] = delay
	}
}

func (n *simulationNetwork) delayFor(source, destination string) time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.delays[networkPair{source: source, destination: destination}]
}

// beginDelivery and endDelivery track in-flight message deliveries. The
// driver blocks on the idle channel to let the bubble clock release held
// messages before observing; the same quiescence discipline the fake store
// obeys ([simulation.md](simulation.md)).
func (n *simulationNetwork) beginDelivery() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.activeDeliveries == 0 {
		n.deliveriesIdle = make(chan struct{})
	}
	n.activeDeliveries++
	return n.deliveriesIdle
}

func (n *simulationNetwork) endDelivery(idle chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.activeDeliveries--
	if n.activeDeliveries == 0 {
		close(idle)
	}
}

func (n *simulationNetwork) waitForIdle() {
	n.mu.Lock()
	idle := n.deliveriesIdle
	n.mu.Unlock()
	<-idle
}

func (n *simulationNetwork) addNode(addr string) (store.MemberStore, *simulationTransport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	endpoint := &simulationTransport{
		network: n,
		addr:    addr,
		served:  make(chan struct{}),
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
	members := &partitionedMemberStore{backend: n.backend, addr: addr}
	n.transports[addr] = endpoint
	n.memberStore[addr] = members
	return members, endpoint
}

func (n *simulationNetwork) partition(groups map[string]int) error {
	snapshot, err := n.backend.ListMembers(context.Background())
	if err != nil {
		return err
	}

	n.mu.Lock()
	stores := make([]*partitionedMemberStore, 0, len(n.memberStore))
	for _, members := range n.memberStore {
		stores = append(stores, members)
	}
	n.mu.Unlock()
	for _, members := range stores {
		members.partition(snapshot.Members)
	}

	n.mu.Lock()
	n.groups = cloneGroups(groups)
	n.partitioned = true
	n.mu.Unlock()
	return nil
}

func (n *simulationNetwork) heal() {
	n.mu.Lock()
	n.partitioned = false
	n.groups = nil
	stores := make([]*partitionedMemberStore, 0, len(n.memberStore))
	for _, members := range n.memberStore {
		stores = append(stores, members)
	}
	n.mu.Unlock()
	for _, members := range stores {
		members.heal()
	}
}

func (n *simulationNetwork) blocked(source, destination string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.partitioned {
		return false
	}
	return n.groups[source] != n.groups[destination]
}

func (n *simulationNetwork) stats() (sends, delivered, dropped, held, completed int64) {
	return n.sends.Load(), n.delivered.Load(), n.dropped.Load(), n.held.Load(), n.completed.Load()
}

func cloneGroups(groups map[string]int) map[string]int {
	clone := make(map[string]int, len(groups))
	for addr, group := range groups {
		clone[addr] = group
	}
	return clone
}

type simulationTransport struct {
	network *simulationNetwork
	addr    string

	mu        sync.Mutex
	handler   transport.Handler
	served    chan struct{}
	done      chan struct{}
	closed    chan struct{}
	serveOnce sync.Once
	closeOnce sync.Once
}

var _ transport.Transport = (*simulationTransport)(nil)

func (t *simulationTransport) Addr() string {
	return t.addr
}

func (t *simulationTransport) Serve(ctx context.Context, handler transport.Handler) error {
	t.mu.Lock()
	t.handler = handler
	t.mu.Unlock()
	t.serveOnce.Do(func() { close(t.served) })
	defer close(t.done)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return errSimNetworkClosed
	}
}

func (t *simulationTransport) Send(ctx context.Context, addr string, payload []byte) ([]byte, error) {
	t.network.sends.Add(1)
	if t.network.blocked(t.addr, addr) {
		t.network.dropped.Add(1)
		return nil, errSimNetworkPartition
	}

	t.network.mu.Lock()
	peer := t.network.transports[addr]
	t.network.mu.Unlock()
	if peer == nil {
		return nil, errors.New("sim network destination is not registered")
	}
	peer.mu.Lock()
	handler := peer.handler
	peer.mu.Unlock()
	if handler == nil {
		return nil, errors.New("sim network destination is not serving")
	}

	delay := t.network.delayFor(t.addr, addr)
	result := make(chan struct {
		payload []byte
		err     error
	}, 1)
	go func() {
		idle := t.network.beginDelivery()
		defer func() {
			t.network.completed.Add(1)
			t.network.endDelivery(idle)
		}()
		if delay > 0 {
			t.network.held.Add(1)
			time.Sleep(delay)
		}
		response, err := handler(context.Background(), payload)
		result <- struct {
			payload []byte
			err     error
		}{payload: response, err: err}
	}()
	select {
	case response := <-result:
		t.network.delivered.Add(1)
		return response.payload, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *simulationTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	<-t.done
	return nil
}

// Kill is the crash path: a crashed node does not close its transport
// gracefully, and in-flight handler results are dropped.
func (t *simulationTransport) Kill() error {
	t.closeOnce.Do(func() { close(t.closed) })
	<-t.done
	return nil
}

type partitionedMemberStore struct {
	backend *fakeStore
	addr    string

	mu          sync.Mutex
	partitioned bool
	members     map[fakeMemberKey]store.Member
}

var _ store.MemberStore = (*partitionedMemberStore)(nil)

func (s *partitionedMemberStore) partition(snapshot []store.Member) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitioned = true
	s.members = make(map[fakeMemberKey]store.Member, len(snapshot))
	for _, member := range snapshot {
		s.members[fakeMemberKey{nodeAddr: member.NodeAddr, generation: member.Generation}] = member
	}
}

func (s *partitionedMemberStore) heal() {
	s.mu.Lock()
	s.partitioned = false
	s.members = nil
	s.mu.Unlock()
}

func (s *partitionedMemberStore) WriteMember(ctx context.Context, member store.Member) (store.ETag, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	if !s.partitioned {
		s.mu.Unlock()
		return s.backend.WriteMember(ctx, member)
	}
	key := fakeMemberKey{nodeAddr: member.NodeAddr, generation: member.Generation}
	current := s.members[key]
	if current.ETag != member.ETag {
		s.mu.Unlock()
		return 0, store.ErrConflict
	}
	member.ETag = current.ETag + 1
	s.members[key] = member
	s.mu.Unlock()
	return member.ETag, nil
}

func (s *partitionedMemberStore) ListMembers(ctx context.Context) (store.MemberSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return store.MemberSnapshot{}, err
	}
	s.mu.Lock()
	if !s.partitioned {
		s.mu.Unlock()
		return s.backend.listMembersFor(ctx, s.addr)
	}
	members := make([]store.Member, 0, len(s.members))
	for _, member := range s.members {
		members = append(members, member)
	}
	s.mu.Unlock()
	sort.Slice(members, func(i, j int) bool {
		if members[i].NodeAddr != members[j].NodeAddr {
			return members[i].NodeAddr < members[j].NodeAddr
		}
		return members[i].Generation < members[j].Generation
	})
	return store.MemberSnapshot{Members: members, TableNow: s.backend.memberTableNow()}, nil
}

// nodeScheduleStore attributes the poller's list calls to the owning node so
// the schedule list fault can bind a node target, the same way
// partitionedMemberStore attributes member list calls.
type nodeScheduleStore struct {
	backend *fakeStore
	addr    string
}

var _ store.ScheduleStore = (*nodeScheduleStore)(nil)

func (s *nodeScheduleStore) ListDue(ctx context.Context, now time.Time) ([]store.Schedule, error) {
	return s.backend.listDueFor(ctx, s.addr, now)
}

func (s *nodeScheduleStore) Claim(ctx context.Context, schedule store.Schedule, nextDueAt time.Time) (bool, error) {
	return s.backend.Claim(ctx, schedule, nextDueAt)
}

func (s *nodeScheduleStore) Put(ctx context.Context, schedule store.Schedule) error {
	return s.backend.Put(ctx, schedule)
}

func (s *nodeScheduleStore) Delete(ctx context.Context, id store.Identity, name string) error {
	return s.backend.Delete(ctx, id, name)
}
