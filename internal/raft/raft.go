// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, 2014) used by RaftKV. It implements:
//
//   - leader election with randomized timeouts, pre-vote and check-quorum
//   - log replication with batched, per-peer AppendEntries and fast
//     conflict backtracking
//   - quorum (majority) commit, restricted to entries of the current term
//   - durable state in a write-ahead log, with leader group-commit fsync
//   - log compaction and InstallSnapshot for lagging followers
//   - linearizable reads via the ReadIndex protocol
//
// One Raft instance replicates one shard (replica group); a node runs many.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"raftkv/internal/wal"
	pb "raftkv/proto/raftkvpb"
)

var (
	ErrStopped = errors.New("raft: stopped")
	// ErrLost means the proposal was overwritten by a new leader and was
	// definitely not applied.
	ErrLost = errors.New("raft: proposal lost to leader change")
)

// NotLeaderError is returned when a request reaches a non-leader.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string { return "raft: not leader (leader=" + e.Leader + ")" }

// Transport sends Raft RPCs to peers (by node id).
type Transport interface {
	RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peer string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

// StateMachine is the replicated application (the KV store for one shard).
type StateMachine interface {
	// Apply applies committed entries in order and durably-or-not records
	// the last applied index/term atomically with them.
	Apply(entries []*pb.Entry) error
	// Applied returns the (index, term) of the last applied entry.
	Applied() (index, term uint64)
	// Sync makes everything applied so far durable.
	Sync() error
	// Snapshot returns a point-in-time image of the state and the index/term
	// it reflects.
	Snapshot() (data []byte, index, term uint64, err error)
	// Restore replaces the state with a snapshot image (durably).
	Restore(data []byte, index, term uint64) error
}

// Observer receives events for metrics. All methods may be nil-safe no-ops.
type Observer interface {
	ElectionStarted(group uint32)
	BecameLeader(group uint32)
	CommitLatency(group uint32, d time.Duration)
	ReplicationRTT(group uint32, peer string, d time.Duration)
	SnapshotSent(group uint32)
	SnapshotInstalled(group uint32)
	LogCompacted(group uint32)
	WALSync(d time.Duration)
}

type Config struct {
	ID        string
	Group     uint32
	Peers     []string // all member ids including ID
	Dir       string
	Transport Transport
	SM        StateMachine
	Observer  Observer

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
	// Preferred, if true, makes this replica campaign quickly at startup so
	// leadership of different shards spreads across nodes.
	Preferred bool

	MaxEntriesPerAppend int
	MaxBytesPerAppend   int
	CompactThreshold    uint64 // compact when this many applied entries sit in the log
	NoSync              bool
	// SyncGroup, if set, batches device flushes with the node's other groups.
	SyncGroup *wal.GroupSyncer
	Logger    *log.Logger
}

func (c *Config) defaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 200 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 500 * time.Millisecond
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 2048
	}
	if c.MaxBytesPerAppend == 0 {
		c.MaxBytesPerAppend = 4 << 20
	}
	if c.CompactThreshold == 0 {
		c.CompactThreshold = 20000
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

type waiter struct {
	term  uint64
	start time.Time
	ch    chan error
}

type readReq struct {
	start time.Time
	index uint64
	ch    chan error
}

type Raft struct {
	cfg    Config
	group  uint32
	id     string
	peers  []string // excluding self
	quorum int

	// Lock order: walMu -> applyMu -> mu.
	walMu   sync.Mutex // serializes all WAL writes with the log mutations they persist
	applyMu sync.Mutex // held while applying to / restoring the state machine
	mu      sync.Mutex

	wal *wal.WAL

	role     role
	term     uint64
	votedFor string
	leaderID string

	log            []*pb.Entry // log[0] is a sentinel {Index: snapIndex, Term: snapTerm}
	commitIndex    uint64
	lastApplied    uint64
	persistedIndex uint64 // highest index durably in this node's WAL

	// leader state
	epoch            uint64 // bumped on every role change; stops stale replicators
	nextIndex        map[string]uint64
	matchIndex       map[string]uint64
	lastAckSent      map[string]time.Time // send time of the latest request the peer answered in our term
	lastContact      map[string]time.Time // receive time of the latest response
	notify           map[string]chan struct{}
	leaderStartIndex uint64
	waiters          map[uint64]waiter
	reads            []readReq

	electionDeadline time.Time
	lastLeaderHeard  time.Time

	applyCond     *sync.Cond
	appliedNotify chan struct{}
	persistCh     chan struct{}
	compacting    bool
	stopCh        chan struct{}
	stopped       bool
	wg            sync.WaitGroup
	rng           *rand.Rand
}

func New(cfg Config) (*Raft, error) {
	cfg.defaults()
	w, st, err := wal.Open(fmt.Sprintf("%s/raft-%04d.wal", cfg.Dir, cfg.Group))
	if err != nil {
		return nil, err
	}
	w.NoSync = cfg.NoSync
	w.Group = cfg.SyncGroup
	if cfg.Observer != nil {
		w.OnSync = cfg.Observer.WALSync
	}
	r := &Raft{
		cfg:           cfg,
		group:         cfg.Group,
		id:            cfg.ID,
		wal:           w,
		term:          st.Term,
		votedFor:      st.Vote,
		waiters:       map[uint64]waiter{},
		appliedNotify: make(chan struct{}),
		persistCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.Group)*7919)),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			r.peers = append(r.peers, p)
		}
	}
	r.quorum = len(cfg.Peers)/2 + 1
	r.applyCond = sync.NewCond(&r.mu)
	r.log = append([]*pb.Entry{{Index: st.SnapIndex, Term: st.SnapTerm}}, st.Entries...)
	r.persistedIndex = r.lastIndex()

	// The state machine is durable on its own; resume from what it applied.
	ai, at := cfg.SM.Applied()
	if ai < st.SnapIndex {
		return nil, fmt.Errorf("raft group %d: state machine at %d is behind compacted log %d", cfg.Group, ai, st.SnapIndex)
	}
	if ai > r.lastIndex() || (ai > st.SnapIndex && r.termAt(ai) != at) {
		// Should not happen (we never apply past the WAL), but recover safely.
		r.log = []*pb.Entry{{Index: ai, Term: at}}
		r.persistedIndex = ai
	}
	r.lastApplied, r.commitIndex = ai, ai

	d := r.randomElectionTimeout()
	if cfg.Preferred {
		d = cfg.ElectionTimeoutMin / 4
	}
	r.electionDeadline = time.Now().Add(d)

	r.wg.Add(3)
	go r.ticker()
	go r.applier()
	go r.persister()
	return r, nil
}

// ---------------------------------------------------------------------------
// log helpers (mu held)
// ---------------------------------------------------------------------------

func (r *Raft) snapIndex() uint64 { return r.log[0].Index }
func (r *Raft) snapTerm() uint64  { return r.log[0].Term }
func (r *Raft) lastIndex() uint64 { return r.log[len(r.log)-1].Index }
func (r *Raft) lastTerm() uint64  { return r.log[len(r.log)-1].Term }

// termAt returns the term of entry i, or 0 if it's not in the log.
func (r *Raft) termAt(i uint64) uint64 {
	if i < r.snapIndex() || i > r.lastIndex() {
		return 0
	}
	return r.log[i-r.snapIndex()].Term
}

func (r *Raft) entryAt(i uint64) *pb.Entry { return r.log[i-r.snapIndex()] }

// slice returns a copy of entries [lo, hi].
func (r *Raft) slice(lo, hi uint64) []*pb.Entry {
	if lo > hi {
		return nil
	}
	s := r.snapIndex()
	out := make([]*pb.Entry, hi-lo+1)
	copy(out, r.log[lo-s:hi-s+1])
	return out
}

func (r *Raft) randomElectionTimeout() time.Duration {
	span := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	return r.cfg.ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(span)+1))
}

func (r *Raft) resetElectionTimer() {
	r.electionDeadline = time.Now().Add(r.randomElectionTimeout())
}

// persistHardStateLocked writes term/vote durably. Requires walMu and mu.
func (r *Raft) persistHardStateLocked() {
	if err := r.wal.SaveHardState(r.term, r.votedFor); err != nil {
		r.cfg.Logger.Fatalf("raft[%d]: wal: %v", r.group, err)
	}
	if err := r.wal.Sync(); err != nil {
		r.cfg.Logger.Fatalf("raft[%d]: wal sync: %v", r.group, err)
	}
}

// becomeFollower steps down into term. Requires mu (and walMu if term changes,
// since the caller must persist).
func (r *Raft) becomeFollower(term uint64, leaderID string) {
	wasLeader := r.role == leader
	if term > r.term {
		r.term = term
		r.votedFor = ""
	}
	r.role = follower
	r.leaderID = leaderID
	if wasLeader {
		r.epoch++
		r.failPendingLocked(&NotLeaderError{Leader: leaderID})
	}
}

func (r *Raft) failPendingLocked(err error) {
	for idx, w := range r.waiters {
		w.ch <- err
		delete(r.waiters, idx)
	}
	for _, rd := range r.reads {
		rd.ch <- err
	}
	r.reads = nil
}

// ---------------------------------------------------------------------------
// elections
// ---------------------------------------------------------------------------

func (r *Raft) ticker() {
	defer r.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
		}
		r.mu.Lock()
		now := time.Now()
		switch r.role {
		case leader:
			// Check-quorum: step down if a majority hasn't answered recently,
			// so a partitioned leader stops accepting writes.
			alive := 1
			for _, p := range r.peers {
				if now.Sub(r.lastContact[p]) < r.cfg.ElectionTimeoutMax {
					alive++
				}
			}
			if alive < r.quorum {
				r.cfg.Logger.Printf("raft[%d] %s: lost quorum contact, stepping down (term %d)", r.group, r.id, r.term)
				r.becomeFollower(r.term, "")
				r.resetElectionTimer()
			}
		default:
			if now.After(r.electionDeadline) {
				r.resetElectionTimer()
				go r.campaign()
			}
		}
		r.mu.Unlock()
	}
}

// campaign runs a pre-vote round and, if it wins, a real election.
func (r *Raft) campaign() {
	r.mu.Lock()
	if r.role == leader || r.stopped {
		r.mu.Unlock()
		return
	}
	startTerm := r.term
	pre := &pb.RequestVoteRequest{
		Group: r.group, Term: r.term + 1, Candidate: r.id,
		LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm(), PreVote: true,
	}
	r.mu.Unlock()

	if len(r.peers) > 0 && !r.collectVotes(pre) {
		return
	}

	// Real election.
	r.walMu.Lock()
	r.mu.Lock()
	if r.role == leader || r.term != startTerm || r.stopped {
		r.mu.Unlock()
		r.walMu.Unlock()
		return
	}
	r.term++
	r.role = candidate
	r.votedFor = r.id
	r.leaderID = ""
	r.persistHardStateLocked()
	r.resetElectionTimer()
	term := r.term
	req := &pb.RequestVoteRequest{
		Group: r.group, Term: term, Candidate: r.id,
		LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm(),
	}
	if r.cfg.Observer != nil {
		r.cfg.Observer.ElectionStarted(r.group)
	}
	r.mu.Unlock()
	r.walMu.Unlock()

	if !r.collectVotes(req) {
		return
	}
	r.mu.Lock()
	if r.role == candidate && r.term == term {
		r.becomeLeaderLocked()
	}
	r.mu.Unlock()
}

// collectVotes sends req to all peers and reports whether a majority granted.
func (r *Raft) collectVotes(req *pb.RequestVoteRequest) bool {
	type res struct {
		resp *pb.RequestVoteResponse
		err  error
	}
	ch := make(chan res, len(r.peers))
	for _, p := range r.peers {
		go func(p string) {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
			defer cancel()
			resp, err := r.cfg.Transport.RequestVote(ctx, p, req)
			ch <- res{resp, err}
		}(p)
	}
	granted := 1
	for range r.peers {
		x := <-ch
		if x.err != nil {
			continue
		}
		if x.resp.Term > req.Term || (!req.PreVote && x.resp.Term > r.currentTerm()) {
			r.observeTerm(x.resp.Term)
			return false
		}
		if x.resp.Granted {
			granted++
			if granted >= r.quorum {
				return true
			}
		}
	}
	return false
}

func (r *Raft) currentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term
}

// observeTerm steps down if term is newer than ours.
func (r *Raft) observeTerm(term uint64) {
	r.walMu.Lock()
	defer r.walMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if term > r.term && !r.stopped {
		r.becomeFollower(term, "")
		r.persistHardStateLocked()
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.role = leader
	r.leaderID = r.id
	r.epoch++
	r.cfg.Logger.Printf("raft[%d] %s: became leader for term %d (last index %d)", r.group, r.id, r.term, r.lastIndex())
	if r.cfg.Observer != nil {
		r.cfg.Observer.BecameLeader(r.group)
	}
	now := time.Now()
	r.nextIndex = map[string]uint64{}
	r.matchIndex = map[string]uint64{}
	r.lastAckSent = map[string]time.Time{}
	r.lastContact = map[string]time.Time{}
	r.notify = map[string]chan struct{}{}
	for _, p := range r.peers {
		r.nextIndex[p] = r.lastIndex() + 1
		r.lastContact[p] = now // grace period for check-quorum
		r.notify[p] = make(chan struct{}, 1)
	}
	// Commit a no-op from our term so earlier entries commit and ReadIndex
	// has a safe lower bound.
	r.log = append(r.log, &pb.Entry{Index: r.lastIndex() + 1, Term: r.term})
	r.leaderStartIndex = r.lastIndex()
	r.signalPersist()
	for _, p := range r.peers {
		r.wg.Add(1)
		go r.replicator(p, r.epoch, r.notify[p])
	}
	r.maybeCommitLocked()
}

// ---------------------------------------------------------------------------
// replication (leader)
// ---------------------------------------------------------------------------

func (r *Raft) signalPersist() {
	select {
	case r.persistCh <- struct{}{}:
	default:
	}
}

func (r *Raft) kickAllLocked() {
	for _, ch := range r.notify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// persister durably writes entries the leader appended in memory, batching
// many proposals into one fsync (group commit).
func (r *Raft) persister() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.persistCh:
		}
		r.walMu.Lock()
		r.mu.Lock()
		from, to := r.persistedIndex+1, r.lastIndex()
		if from < r.snapIndex()+1 {
			from = r.snapIndex() + 1
		}
		ents := r.slice(from, to)
		r.mu.Unlock()
		if len(ents) > 0 {
			if err := r.wal.SaveEntries(ents); err != nil {
				r.cfg.Logger.Fatalf("raft[%d]: wal: %v", r.group, err)
			}
			if err := r.wal.Sync(); err != nil {
				r.cfg.Logger.Fatalf("raft[%d]: wal sync: %v", r.group, err)
			}
		}
		r.mu.Lock()
		// walMu was held, so nobody truncated [from, to] in the meantime.
		if to > r.persistedIndex {
			r.persistedIndex = to
		}
		if r.role == leader {
			r.maybeCommitLocked()
		}
		r.applyCond.Broadcast()
		r.mu.Unlock()
		r.walMu.Unlock()
	}
}

func (r *Raft) replicator(peer string, epoch uint64, notify chan struct{}) {
	defer r.wg.Done()
	hb := time.NewTimer(0)
	defer hb.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-notify:
		case <-hb.C:
		}
		// Send until the peer is caught up (or we lose leadership).
		for {
			more, ok := r.replicateOnce(peer, epoch)
			if !ok {
				return
			}
			if !more {
				break
			}
		}
		if !hb.Stop() {
			select {
			case <-hb.C:
			default:
			}
		}
		hb.Reset(r.cfg.HeartbeatInterval)
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to peer.
// more reports whether there are still entries to send; ok=false means this
// replicator is stale and should exit.
func (r *Raft) replicateOnce(peer string, epoch uint64) (more, ok bool) {
	r.mu.Lock()
	if r.role != leader || r.epoch != epoch || r.stopped {
		r.mu.Unlock()
		return false, false
	}
	next := r.nextIndex[peer]
	if next <= r.snapIndex() {
		r.mu.Unlock()
		return r.sendSnapshot(peer, epoch)
	}
	prev := next - 1
	req := &pb.AppendEntriesRequest{
		Group: r.group, Term: r.term, Leader: r.id,
		PrevLogIndex: prev, PrevLogTerm: r.termAt(prev),
		LeaderCommit: r.commitIndex,
	}
	last := r.lastIndex()
	if next <= last {
		hi := last
		if hi-next+1 > uint64(r.cfg.MaxEntriesPerAppend) {
			hi = next + uint64(r.cfg.MaxEntriesPerAppend) - 1
		}
		bytes := 0
		for i := next; i <= hi; i++ {
			bytes += len(r.entryAt(i).Data) + 24
			if bytes > r.cfg.MaxBytesPerAppend && i > next {
				hi = i - 1
				break
			}
		}
		req.Entries = r.slice(next, hi)
	}
	term := r.term
	r.mu.Unlock()

	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	resp, err := r.cfg.Transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		// Peer unreachable: back off to heartbeat cadence.
		return false, true
	}
	if r.cfg.Observer != nil {
		r.cfg.Observer.ReplicationRTT(r.group, peer, time.Since(sent))
	}

	if resp.Term > term {
		r.observeTerm(resp.Term)
		return false, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != leader || r.epoch != epoch {
		return false, false
	}
	r.lastContact[peer] = time.Now()
	if sent.After(r.lastAckSent[peer]) {
		r.lastAckSent[peer] = sent
	}
	if resp.Success {
		m := req.PrevLogIndex + uint64(len(req.Entries))
		if m > r.matchIndex[peer] {
			r.matchIndex[peer] = m
		}
		if m+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = m + 1
		}
		r.maybeCommitLocked()
	} else {
		// Fast backtracking using the follower's conflict hint.
		ni := resp.ConflictIndex
		if ni < 1 {
			ni = 1
		}
		if ni <= r.matchIndex[peer] {
			ni = r.matchIndex[peer] + 1
		}
		r.nextIndex[peer] = ni
	}
	r.checkReadsLocked()
	return r.nextIndex[peer] <= r.lastIndex(), true
}

func (r *Raft) sendSnapshot(peer string, epoch uint64) (more, ok bool) {
	data, idx, sterm, err := r.cfg.SM.Snapshot()
	if err != nil {
		r.cfg.Logger.Printf("raft[%d]: snapshot: %v", r.group, err)
		return false, true
	}
	r.mu.Lock()
	if r.role != leader || r.epoch != epoch {
		r.mu.Unlock()
		return false, false
	}
	req := &pb.InstallSnapshotRequest{
		Group: r.group, Term: r.term, Leader: r.id,
		LastIncludedIndex: idx, LastIncludedTerm: sterm, Data: data,
	}
	term := r.term
	r.mu.Unlock()

	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*r.cfg.RPCTimeout)
	resp, err := r.cfg.Transport.InstallSnapshot(ctx, peer, req)
	cancel()
	if err != nil {
		return false, true
	}
	if resp.Term > term {
		r.observeTerm(resp.Term)
		return false, false
	}
	if r.cfg.Observer != nil {
		r.cfg.Observer.SnapshotSent(r.group)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != leader || r.epoch != epoch {
		return false, false
	}
	r.lastContact[peer] = time.Now()
	if sent.After(r.lastAckSent[peer]) {
		r.lastAckSent[peer] = sent
	}
	if idx > r.matchIndex[peer] {
		r.matchIndex[peer] = idx
	}
	r.nextIndex[peer] = r.matchIndex[peer] + 1
	r.checkReadsLocked()
	return r.nextIndex[peer] <= r.lastIndex(), true
}

// maybeCommitLocked advances commitIndex to the highest index replicated on a
// majority, counting the leader only for durably persisted entries.
func (r *Raft) maybeCommitLocked() {
	matches := make([]uint64, 0, len(r.peers)+1)
	matches = append(matches, r.persistedIndex)
	for _, p := range r.peers {
		matches = append(matches, r.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := matches[r.quorum-1]
	// Only entries from the current term commit by counting replicas (§5.4.2).
	if n > r.commitIndex && r.termAt(n) == r.term {
		r.commitIndex = n
		r.applyCond.Broadcast()
		r.kickAllLocked() // propagate the new commit index promptly
	}
}

// ---------------------------------------------------------------------------
// apply
// ---------------------------------------------------------------------------

func (r *Raft) applier() {
	defer r.wg.Done()
	const maxBatch = 4096
	for {
		r.mu.Lock()
		for !r.stopped && r.lastApplied >= r.applyLimitLocked() {
			r.applyCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		r.applyMu.Lock()
		r.mu.Lock()
		from, to := r.lastApplied+1, r.applyLimitLocked()
		if from < r.snapIndex()+1 { // a snapshot install moved us forward
			from = r.snapIndex() + 1
		}
		if to >= from+maxBatch {
			to = from + maxBatch - 1
		}
		if from > to {
			r.mu.Unlock()
			r.applyMu.Unlock()
			continue
		}
		ents := r.slice(from, to)
		r.mu.Unlock()

		if err := r.cfg.SM.Apply(ents); err != nil {
			r.cfg.Logger.Fatalf("raft[%d]: apply: %v", r.group, err)
		}

		r.mu.Lock()
		r.lastApplied = to
		now := time.Now()
		for _, e := range ents {
			if w, ok := r.waiters[e.Index]; ok {
				delete(r.waiters, e.Index)
				if w.term == e.Term {
					if r.cfg.Observer != nil {
						r.cfg.Observer.CommitLatency(r.group, now.Sub(w.start))
					}
					w.ch <- nil
				} else {
					w.ch <- ErrLost
				}
			}
		}
		close(r.appliedNotify)
		r.appliedNotify = make(chan struct{})
		needCompact := !r.compacting && r.lastApplied-r.snapIndex() > r.cfg.CompactThreshold
		if needCompact {
			r.compacting = true
		}
		r.mu.Unlock()
		r.applyMu.Unlock()
		if needCompact {
			go r.compact()
		}
	}
}

func (r *Raft) applyLimitLocked() uint64 {
	if r.persistedIndex < r.commitIndex {
		return r.persistedIndex
	}
	return r.commitIndex
}

// compact discards log entries already captured durably by the state machine
// (the state machine is the snapshot) and rewrites the WAL.
func (r *Raft) compact() {
	defer func() {
		r.mu.Lock()
		r.compacting = false
		r.mu.Unlock()
	}()
	idx, term := r.cfg.SM.Applied()
	if err := r.cfg.SM.Sync(); err != nil {
		r.cfg.Logger.Printf("raft[%d]: sm sync: %v", r.group, err)
		return
	}
	r.walMu.Lock()
	defer r.walMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if idx <= r.snapIndex() || r.termAt(idx) != term {
		return
	}
	keep := r.slice(idx+1, r.persistedIndex)
	newLog := append([]*pb.Entry{{Index: idx, Term: term}}, r.slice(idx+1, r.lastIndex())...)
	if err := r.wal.Rewrite(r.term, r.votedFor, idx, term, keep); err != nil {
		r.cfg.Logger.Printf("raft[%d]: compact: %v", r.group, err)
		return
	}
	r.log = newLog
	if r.cfg.Observer != nil {
		r.cfg.Observer.LogCompacted(r.group)
	}
}

// ---------------------------------------------------------------------------
// RPC handlers
// ---------------------------------------------------------------------------

func (r *Raft) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.walMu.Lock()
	defer r.walMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	upToDate := req.LastLogTerm > r.lastTerm() ||
		(req.LastLogTerm == r.lastTerm() && req.LastLogIndex >= r.lastIndex())
	// Leader stickiness: ignore candidates while we hear from a live leader.
	heardLeader := r.role == leader ||
		(r.leaderID != "" && time.Since(r.lastLeaderHeard) < r.cfg.ElectionTimeoutMin)

	if r.stopped {
		return &pb.RequestVoteResponse{Term: r.term}
	}
	if req.PreVote {
		// Pre-vote changes no state: would we vote for this candidate if it
		// started a real election?
		granted := req.Term > r.term && upToDate && !heardLeader
		return &pb.RequestVoteResponse{Term: r.term, Granted: granted}
	}

	if req.Term < r.term || (heardLeader && req.Term > r.term) {
		return &pb.RequestVoteResponse{Term: r.term}
	}
	changed := false
	if req.Term > r.term {
		r.becomeFollower(req.Term, "")
		changed = true
	}
	granted := false
	if (r.votedFor == "" || r.votedFor == req.Candidate) && upToDate {
		if r.votedFor != req.Candidate {
			r.votedFor = req.Candidate
			changed = true
		}
		granted = true
		r.resetElectionTimer()
	}
	if changed {
		r.persistHardStateLocked()
	}
	return &pb.RequestVoteResponse{Term: r.term, Granted: granted}
}

func (r *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.walMu.Lock()
	defer r.walMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || req.Term < r.term {
		return &pb.AppendEntriesResponse{Term: r.term}
	}
	// A deposed leader may still hold entries it appended but never flushed;
	// write them now so persistedIndex below is truthful.
	flushed := r.flushUnpersistedLocked()
	termChanged := req.Term > r.term
	if termChanged || r.role != follower {
		r.becomeFollower(req.Term, req.Leader)
	}
	r.leaderID = req.Leader
	r.lastLeaderHeard = time.Now()
	r.resetElectionTimer()

	prev, prevTerm := req.PrevLogIndex, req.PrevLogTerm
	ents := req.Entries
	if prev < r.snapIndex() {
		// Part of the request is already compacted (and thus committed).
		skip := r.snapIndex() - prev
		if uint64(len(ents)) <= skip {
			ents = nil
		} else {
			ents = ents[skip:]
		}
		prev, prevTerm = r.snapIndex(), r.snapTerm()
	}
	if prev > r.lastIndex() {
		if termChanged {
			r.persistHardStateLocked()
		}
		return &pb.AppendEntriesResponse{Term: r.term, ConflictIndex: r.lastIndex() + 1}
	}
	if t := r.termAt(prev); t != prevTerm {
		// Skip back over the whole conflicting term.
		ci := prev
		for ci > r.snapIndex()+1 && r.termAt(ci-1) == t {
			ci--
		}
		if termChanged {
			r.persistHardStateLocked()
		}
		return &pb.AppendEntriesResponse{Term: r.term, ConflictIndex: ci}
	}

	var toWrite []*pb.Entry
	for i, e := range ents {
		if e.Index > r.lastIndex() {
			toWrite = ents[i:]
			break
		}
		if r.termAt(e.Index) != e.Term {
			if e.Index <= r.commitIndex {
				r.cfg.Logger.Fatalf("raft[%d]: refusing to truncate committed entry %d", r.group, e.Index)
			}
			r.log = r.log[:e.Index-r.snapIndex()]
			toWrite = ents[i:]
			break
		}
	}
	if termChanged {
		if err := r.wal.SaveHardState(r.term, r.votedFor); err != nil {
			r.cfg.Logger.Fatalf("raft[%d]: wal: %v", r.group, err)
		}
	}
	if len(toWrite) > 0 {
		r.log = append(r.log, toWrite...)
		if err := r.wal.SaveEntries(toWrite); err != nil {
			r.cfg.Logger.Fatalf("raft[%d]: wal: %v", r.group, err)
		}
	}
	if termChanged || len(toWrite) > 0 || flushed {
		if err := r.wal.Sync(); err != nil {
			r.cfg.Logger.Fatalf("raft[%d]: wal sync: %v", r.group, err)
		}
	}
	r.persistedIndex = r.lastIndex()

	match := prev + uint64(len(ents))
	if req.LeaderCommit > r.commitIndex {
		c := req.LeaderCommit
		if c > match {
			c = match
		}
		if c > r.commitIndex {
			r.commitIndex = c
			r.applyCond.Broadcast()
		}
	}
	return &pb.AppendEntriesResponse{Term: r.term, Success: true, MatchIndex: match}
}

// flushUnpersistedLocked buffers any in-memory entries not yet in the WAL
// (caller must Sync). Requires walMu and mu.
func (r *Raft) flushUnpersistedLocked() bool {
	from := r.persistedIndex + 1
	if from <= r.snapIndex() {
		from = r.snapIndex() + 1
	}
	if from > r.lastIndex() {
		return false
	}
	if err := r.wal.SaveEntries(r.slice(from, r.lastIndex())); err != nil {
		r.cfg.Logger.Fatalf("raft[%d]: wal: %v", r.group, err)
	}
	r.persistedIndex = r.lastIndex()
	return true
}

func (r *Raft) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	r.walMu.Lock()
	defer r.walMu.Unlock()
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || req.Term < r.term {
		return &pb.InstallSnapshotResponse{Term: r.term}
	}
	if req.Term > r.term || r.role != follower {
		r.becomeFollower(req.Term, req.Leader)
		r.persistHardStateLocked()
	}
	r.leaderID = req.Leader
	r.lastLeaderHeard = time.Now()
	r.resetElectionTimer()

	idx, term := req.LastIncludedIndex, req.LastIncludedTerm
	if idx <= r.lastApplied {
		return &pb.InstallSnapshotResponse{Term: r.term}
	}
	if err := r.cfg.SM.Restore(req.Data, idx, term); err != nil {
		r.cfg.Logger.Printf("raft[%d]: restore: %v", r.group, err)
		return &pb.InstallSnapshotResponse{Term: r.term}
	}
	if r.termAt(idx) == term {
		r.log = append([]*pb.Entry{{Index: idx, Term: term}}, r.slice(idx+1, r.lastIndex())...)
	} else {
		r.log = []*pb.Entry{{Index: idx, Term: term}}
	}
	if err := r.wal.Rewrite(r.term, r.votedFor, idx, term, r.slice(idx+1, r.lastIndex())); err != nil {
		r.cfg.Logger.Fatalf("raft[%d]: wal rewrite: %v", r.group, err)
	}
	r.persistedIndex = r.lastIndex()
	if idx > r.commitIndex {
		r.commitIndex = idx
	}
	r.lastApplied = idx
	close(r.appliedNotify)
	r.appliedNotify = make(chan struct{})
	r.applyCond.Broadcast()
	if r.cfg.Observer != nil {
		r.cfg.Observer.SnapshotInstalled(r.group)
	}
	return &pb.InstallSnapshotResponse{Term: r.term}
}

// ---------------------------------------------------------------------------
// client-facing API
// ---------------------------------------------------------------------------

// Propose replicates data and blocks until it is committed and applied on this
// node (quorum write). Returns NotLeaderError if this node isn't the leader.
func (r *Raft) Propose(ctx context.Context, data []byte) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrStopped
	}
	if r.role != leader {
		l := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{Leader: l}
	}
	e := &pb.Entry{Index: r.lastIndex() + 1, Term: r.term, Data: data}
	r.log = append(r.log, e)
	ch := make(chan error, 1)
	r.waiters[e.Index] = waiter{term: e.Term, start: time.Now(), ch: ch}
	r.signalPersist()
	r.kickAllLocked()
	r.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.waiters, e.Index)
		r.mu.Unlock()
		return ctx.Err()
	case <-r.stopCh:
		return ErrStopped
	}
}

// ReadIndex implements linearizable reads: it records the commit index,
// confirms leadership with a heartbeat round acknowledged by a majority, and
// waits until the state machine has applied up to that index. The caller may
// then read local state.
func (r *Raft) ReadIndex(ctx context.Context) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrStopped
	}
	if r.role != leader {
		l := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{Leader: l}
	}
	idx := r.commitIndex
	if idx < r.leaderStartIndex {
		idx = r.leaderStartIndex
	}
	var ch chan error
	if len(r.peers) > 0 {
		ch = make(chan error, 1)
		r.reads = append(r.reads, readReq{start: time.Now(), index: idx, ch: ch})
		r.kickAllLocked()
	}
	r.mu.Unlock()

	if ch != nil {
		select {
		case err := <-ch:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return ErrStopped
		}
	}
	return r.WaitApplied(ctx, idx)
}

// checkReadsLocked resolves pending reads whose leadership confirmation has
// been acknowledged by a majority.
func (r *Raft) checkReadsLocked() {
	if len(r.reads) == 0 {
		return
	}
	keep := r.reads[:0]
	for _, rd := range r.reads {
		acks := 1
		for _, p := range r.peers {
			if !r.lastAckSent[p].Before(rd.start) {
				acks++
			}
		}
		if acks >= r.quorum {
			rd.ch <- nil
		} else {
			keep = append(keep, rd)
		}
	}
	r.reads = keep
}

// WaitApplied blocks until lastApplied >= idx.
func (r *Raft) WaitApplied(ctx context.Context, idx uint64) error {
	for {
		r.mu.Lock()
		if r.lastApplied >= idx {
			r.mu.Unlock()
			return nil
		}
		ch := r.appliedNotify
		r.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return ErrStopped
		}
	}
}

type Status struct {
	ID, Leader, Role         string
	Term                     uint64
	CommitIndex, LastApplied uint64
	LastIndex, SnapIndex     uint64
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID: r.id, Leader: r.leaderID, Role: r.role.String(), Term: r.term,
		CommitIndex: r.commitIndex, LastApplied: r.lastApplied,
		LastIndex: r.lastIndex(), SnapIndex: r.snapIndex(),
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == leader
}

// Stop shuts the instance down. Pending proposals fail with ErrStopped.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.epoch++
	close(r.stopCh)
	r.failPendingLocked(ErrStopped)
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
	r.walMu.Lock()
	r.wal.Close()
	r.walMu.Unlock()
}
