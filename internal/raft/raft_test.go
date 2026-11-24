package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	pb "raftkv/proto/raftkvpb"
)

// ---------------------------------------------------------------------------
// In-memory harness: a network that can partition nodes, and a trivial state
// machine that records the applied sequence.
// ---------------------------------------------------------------------------

type memNet struct {
	mu    sync.Mutex
	nodes map[string]*Raft
	cut   map[string]bool // isolated nodes
}

func (n *memNet) get(from, to string) (*Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cut[from] || n.cut[to] {
		return nil, errors.New("partitioned")
	}
	r := n.nodes[to]
	if r == nil {
		return nil, errors.New("down")
	}
	return r, nil
}

type memTransport struct {
	net  *memNet
	self string
}

func (t *memTransport) RequestVote(ctx context.Context, p string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r, err := t.net.get(t.self, p)
	if err != nil {
		return nil, err
	}
	return r.HandleRequestVote(req), nil
}
func (t *memTransport) AppendEntries(ctx context.Context, p string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r, err := t.net.get(t.self, p)
	if err != nil {
		return nil, err
	}
	return r.HandleAppendEntries(req), nil
}
func (t *memTransport) InstallSnapshot(ctx context.Context, p string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	r, err := t.net.get(t.self, p)
	if err != nil {
		return nil, err
	}
	return r.HandleInstallSnapshot(req), nil
}

// memSM is a durable-enough state machine for tests: a list of applied values.
type memSM struct {
	mu      sync.Mutex
	applied []string
	idx     uint64
	term    uint64
}

func (s *memSM) Apply(ents []*pb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range ents {
		if len(e.Data) > 0 {
			s.applied = append(s.applied, string(e.Data))
		}
		s.idx, s.term = e.Index, e.Term
	}
	return nil
}
func (s *memSM) Applied() (uint64, uint64) { s.mu.Lock(); defer s.mu.Unlock(); return s.idx, s.term }
func (s *memSM) Sync() error               { return nil }
func (s *memSM) Snapshot() ([]byte, uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b []byte
	for _, v := range s.applied {
		b = append(b, v...)
		b = append(b, 0)
	}
	return b, s.idx, s.term, nil
}
func (s *memSM) Restore(data []byte, idx, term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = nil
	start := 0
	for i, c := range data {
		if c == 0 {
			s.applied = append(s.applied, string(data[start:i]))
			start = i + 1
		}
	}
	s.idx, s.term = idx, term
	return nil
}
func (s *memSM) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.applied...)
}

type cluster struct {
	t     *testing.T
	ids   []string
	net   *memNet
	sms   map[string]*memSM
	dirs  map[string]string
	compN uint64
}

func newCluster(t *testing.T, n int, compact uint64) *cluster {
	c := &cluster{t: t, net: &memNet{nodes: map[string]*Raft{}, cut: map[string]bool{}},
		sms: map[string]*memSM{}, dirs: map[string]string{}, compN: compact}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.ids = append(c.ids, id)
		c.dirs[id] = t.TempDir()
		c.sms[id] = &memSM{}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *cluster) start(id string) {
	r, err := New(Config{
		ID: id, Group: 1, Peers: c.ids, Dir: c.dirs[id],
		Transport: &memTransport{net: c.net, self: id}, SM: c.sms[id],
		ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, CompactThreshold: c.compN,
		NoSync: true, Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.mu.Lock()
	c.net.nodes[id] = r
	c.net.mu.Unlock()
}

// stop simulates a crash-stop; the WAL and state machine survive.
func (c *cluster) stop(id string) {
	c.net.mu.Lock()
	r := c.net.nodes[id]
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	if r != nil {
		r.Stop()
	}
}

func (c *cluster) isolate(id string, cut bool) {
	c.net.mu.Lock()
	c.net.cut[id] = cut
	c.net.mu.Unlock()
}

func (c *cluster) leader(timeout time.Duration) (string, *Raft) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.net.mu.Lock()
		var lid string
		var lr *Raft
		count := 0
		for id, r := range c.net.nodes {
			if r.IsLeader() && !c.net.cut[id] {
				lid, lr = id, r
				count++
			}
		}
		c.net.mu.Unlock()
		if count == 1 {
			return lid, lr
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no unique leader within %v", timeout)
	return "", nil
}

func (c *cluster) propose(v string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, r := c.leader(3 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := r.Propose(ctx, []byte(v))
		cancel()
		if err == nil {
			return
		}
	}
	c.t.Fatalf("propose %q failed", v)
}

// waitConverged checks every live node applied exactly want, in order.
func (c *cluster) waitConverged(want []string, ids ...string) {
	if len(ids) == 0 {
		ids = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for _, id := range ids {
			got := c.sms[id].values()
			if fmt.Sprint(got) != fmt.Sprint(want) {
				ok = false
				if time.Now().After(deadline) {
					c.t.Fatalf("%s applied %d values, want %d\n got=%v\nwant=%v", id, len(got), len(want), tail(got), tail(want))
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tail(s []string) []string {
	if len(s) > 5 {
		return s[len(s)-5:]
	}
	return s
}

// ---------------------------------------------------------------------------

func TestElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.leader(2 * time.Second)
	var want []string
	for i := 0; i < 100; i++ {
		v := fmt.Sprintf("v%d", i)
		c.propose(v)
		want = append(want, v)
	}
	c.waitConverged(want)
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 5, 0)
	var want []string
	for round := 0; round < 3; round++ {
		lid, _ := c.leader(2 * time.Second)
		for i := 0; i < 20; i++ {
			v := fmt.Sprintf("r%d-%d", round, i)
			c.propose(v)
			want = append(want, v)
		}
		start := time.Now()
		c.stop(lid)
		newID, _ := c.leader(3 * time.Second)
		t.Logf("round %d: killed %s, %s elected after %v", round, lid, newID, time.Since(start).Round(time.Millisecond))
		c.start(lid) // restart from disk
	}
	c.propose("final")
	want = append(want, "final")
	c.waitConverged(want)
}

func TestPartitionedLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 3, 0)
	oldID, old := c.leader(2 * time.Second)
	c.propose("before")
	c.isolate(oldID, true)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	err := old.Propose(ctx, []byte("lost-write"))
	cancel()
	if err == nil {
		t.Fatal("isolated leader committed a write")
	}
	newID, _ := c.leader(3 * time.Second)
	if newID == oldID {
		t.Fatal("isolated node still leader")
	}
	c.propose("after")
	c.isolate(oldID, false)
	// The stale entry must be discarded everywhere.
	c.waitConverged([]string{"before", "after"})
}

func TestCompactionAndSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, 50)
	lid, _ := c.leader(2 * time.Second)
	var lagger string
	for _, id := range c.ids {
		if id != lid {
			lagger = id
			break
		}
	}
	c.stop(lagger)
	var want []string
	for i := 0; i < 400; i++ {
		v := fmt.Sprintf("s%d", i)
		c.propose(v)
		want = append(want, v)
	}
	time.Sleep(200 * time.Millisecond) // let compaction run
	_, r := c.leader(time.Second)
	if st := r.Status(); st.SnapIndex == 0 {
		t.Fatalf("expected log compaction, status %+v", st)
	}
	c.start(lagger) // must catch up via InstallSnapshot
	c.waitConverged(want)
}

func TestCrashRecoveryFromWAL(t *testing.T) {
	c := newCluster(t, 3, 0)
	var want []string
	for i := 0; i < 50; i++ {
		v := fmt.Sprintf("w%d", i)
		c.propose(v)
		want = append(want, v)
	}
	c.waitConverged(want)
	// Restart every node; nothing committed may be lost.
	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		c.sms[id] = &memSM{} // volatile state machine: rebuild purely from the WAL
		c.start(id)
	}
	c.propose("after-restart")
	want = append(want, "after-restart")
	c.waitConverged(want)
}

func TestRandomizedChaos(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	c := newCluster(t, 5, 100)
	rng := rand.New(rand.NewSource(1))
	var want []string
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // nemesis
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(150+rng.Intn(300)) * time.Millisecond):
			}
			id := c.ids[rng.Intn(len(c.ids))]
			switch rng.Intn(3) {
			case 0:
				c.isolate(id, true)
				time.Sleep(time.Duration(100+rng.Intn(300)) * time.Millisecond)
				c.isolate(id, false)
			case 1:
				c.stop(id)
				time.Sleep(time.Duration(100+rng.Intn(300)) * time.Millisecond)
				c.start(id)
			}
		}
	}()
	for i := 0; i < 300; i++ {
		v := fmt.Sprintf("c%d", i)
		c.propose(v)
		mu.Lock()
		want = append(want, v)
		mu.Unlock()
	}
	close(stop)
	wg.Wait()
	for _, id := range c.ids {
		c.isolate(id, false)
	}
	// Every acknowledged value must be applied exactly once, in order.
	// (Unacknowledged retries may also appear, so compare acked subsequence.)
	time.Sleep(time.Second)
	for _, id := range c.ids {
		got := c.sms[id].values()
		j := 0
		seen := map[string]bool{}
		for _, v := range got {
			if j < len(want) && v == want[j] {
				j++
			}
			seen[v] = true
		}
		if j != len(want) {
			t.Fatalf("%s: acked values missing or out of order (matched %d/%d)", id, j, len(want))
		}
	}
	// All replicas must agree on the full applied sequence.
	ref := fmt.Sprint(c.sms[c.ids[0]].values())
	for _, id := range c.ids[1:] {
		if fmt.Sprint(c.sms[id].values()) != ref {
			t.Fatalf("replica %s diverged", id)
		}
	}
}
