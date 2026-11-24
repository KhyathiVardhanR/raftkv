// Package server wires a RaftKV node together: one LSM engine, one Raft group
// per shard (multi-Raft), a gRPC transport between nodes, the client-facing KV
// service, and Prometheus metrics.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"raftkv/internal/raft"
	"raftkv/internal/shard"
	"raftkv/internal/storage"
	"raftkv/internal/wal"
	pb "raftkv/proto/raftkvpb"
)

const maxMsg = 256 << 20

// fastReconnect caps gRPC's reconnect backoff (default max 120s) so a restarted
// peer is reachable again within ~1s.
var fastReconnect = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
	MinConnectTimeout: time.Second,
}

type Config struct {
	ID          string
	Listen      string            // gRPC listen address
	Nodes       map[string]string // id -> advertised gRPC address (all members)
	Dir         string
	NumShards   int
	VNodes      int
	MetricsAddr string // "" disables the HTTP metrics endpoint
	NoSync      bool

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	CompactThreshold   uint64
	RequestTimeout     time.Duration
	Logger             *log.Logger
}

type Node struct {
	cfg       Config
	engine    *storage.Engine
	ring      *shard.Ring
	groups    map[uint32]*raft.Raft
	sms       map[uint32]*storage.Shard
	transport *grpcTransport
	grpc      *grpc.Server
	lis       net.Listener
	http      *http.Server
	metrics   *metrics
	syncer    *wal.GroupSyncer
	stopCh    chan struct{}

	pb.UnimplementedKVServer
}

func Start(cfg Config) (*Node, error) {
	if cfg.VNodes == 0 {
		cfg.VNodes = 64
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 3 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "["+cfg.ID+"] ", log.LstdFlags|log.Lmicroseconds)
	}
	eng, err := storage.Open(filepath.Join(cfg.Dir, "lsm"))
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:    cfg,
		engine: eng,
		ring:   shard.NewRing(cfg.NumShards, cfg.VNodes),
		groups: map[uint32]*raft.Raft{},
		sms:    map[uint32]*storage.Shard{},
		stopCh: make(chan struct{}),
	}
	n.metrics = newMetrics(n)
	syncer, err := wal.NewGroupSyncer(filepath.Join(cfg.Dir, "raft"))
	if err != nil {
		return nil, err
	}
	syncer.OnFlush = func(d time.Duration) { n.metrics.deviceFlush.Observe(d.Seconds()) }
	n.syncer = syncer
	n.transport = newGRPCTransport(cfg.Nodes)

	ids := make([]string, 0, len(cfg.Nodes))
	for id := range cfg.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	myPos := sort.SearchStrings(ids, cfg.ID)

	for s := 0; s < cfg.NumShards; s++ {
		g := uint32(s)
		sm, err := eng.Shard(g)
		if err != nil {
			return nil, err
		}
		r, err := raft.New(raft.Config{
			ID: cfg.ID, Group: g, Peers: ids, Dir: filepath.Join(cfg.Dir, "raft"),
			Transport: n.transport, SM: sm, Observer: n.metrics,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin, ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval: cfg.HeartbeatInterval, CompactThreshold: cfg.CompactThreshold,
			// Spread initial leadership: shard s prefers node s mod N.
			Preferred: s%len(ids) == myPos,
			NoSync:    cfg.NoSync,
			SyncGroup: syncer,
			Logger:    cfg.Logger,
		})
		if err != nil {
			return nil, err
		}
		n.groups[g], n.sms[g] = r, sm
	}

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	n.lis = lis
	n.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg))
	pb.RegisterKVServer(n.grpc, n)
	pb.RegisterRaftServer(n.grpc, &raftService{n: n})
	go n.grpc.Serve(lis)

	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(n.metrics.reg, promhttp.HandlerOpts{}))
		n.http = &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
		go n.http.ListenAndServe()
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-n.stopCh:
				return
			case <-t.C:
				n.metrics.refresh(n)
			}
		}
	}()
	cfg.Logger.Printf("node %s serving on %s (%d shards, peers %v)", cfg.ID, lis.Addr(), cfg.NumShards, ids)
	return n, nil
}

func (n *Node) Addr() string { return n.lis.Addr().String() }

func (n *Node) Stop() {
	close(n.stopCh)
	n.grpc.Stop()
	if n.http != nil {
		n.http.Close()
	}
	var wg sync.WaitGroup
	for _, r := range n.groups {
		wg.Add(1)
		go func(r *raft.Raft) { defer wg.Done(); r.Stop() }(r)
	}
	wg.Wait()
	n.transport.close()
	n.syncer.Close()
	n.engine.Close()
}

// Group exposes a shard's Raft instance (tests).
func (n *Node) Group(g uint32) *raft.Raft { return n.groups[g] }

// ---------------------------------------------------------------------------
// KV service
// ---------------------------------------------------------------------------

func (n *Node) header(g uint32, err error) *pb.Header {
	h := &pb.Header{Shard: g}
	var nl *raft.NotLeaderError
	switch {
	case err == nil:
		h.Code = pb.Code_OK
	case errors.As(err, &nl):
		h.Code, h.Leader = pb.Code_NOT_LEADER, nl.Leader
	case errors.Is(err, raft.ErrLost), errors.Is(err, raft.ErrStopped):
		h.Code = pb.Code_NOT_LEADER
	default:
		h.Code = pb.Code_TIMEOUT
	}
	return h
}

func (n *Node) observe(op string, start time.Time, h *pb.Header) {
	n.metrics.kvLatency.WithLabelValues(op).Observe(time.Since(start).Seconds())
	n.metrics.kvRequests.WithLabelValues(op, h.Code.String()).Inc()
}

func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	g := n.ring.Locate(req.Key)
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	resp := &pb.GetResponse{}
	err := n.groups[g].ReadIndex(ctx)
	if err == nil {
		resp.Value, resp.Found, err = n.sms[g].Get(req.Key)
	}
	resp.Header = n.header(g, err)
	n.observe("get", start, resp.Header)
	return resp, nil
}

func (n *Node) propose(ctx context.Context, op string, key []byte, cmd []byte) *pb.Header {
	start := time.Now()
	g := n.ring.Locate(key)
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	h := n.header(g, n.groups[g].Propose(ctx, cmd))
	n.observe(op, start, h)
	return h
}

func command(op byte, key, value []byte, s *pb.Session) []byte {
	return storage.Command{
		Op: op, Key: key, Value: value,
		ClientID: s.GetClientId(), RequestID: s.GetRequestId(), MinOutstanding: s.GetMinOutstanding(),
	}.Encode()
}

func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	return &pb.PutResponse{Header: n.propose(ctx, "put", req.Key, command(storage.OpPut, req.Key, req.Value, req.Session))}, nil
}

func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{Header: n.propose(ctx, "delete", req.Key, command(storage.OpDelete, req.Key, nil, req.Session))}, nil
}

func (n *Node) ClusterInfo(ctx context.Context, _ *pb.ClusterInfoRequest) (*pb.ClusterInfoResponse, error) {
	resp := &pb.ClusterInfoResponse{NodeId: n.cfg.ID, NumShards: uint32(n.cfg.NumShards), Vnodes: uint32(n.cfg.VNodes)}
	for id, addr := range n.cfg.Nodes {
		resp.Nodes = append(resp.Nodes, &pb.NodeInfo{Id: id, Addr: addr})
	}
	sort.Slice(resp.Nodes, func(i, j int) bool { return resp.Nodes[i].Id < resp.Nodes[j].Id })
	for g := uint32(0); g < uint32(n.cfg.NumShards); g++ {
		st := n.groups[g].Status()
		resp.Shards = append(resp.Shards, &pb.ShardInfo{
			Shard: g, Leader: st.Leader, Term: st.Term, CommitIndex: st.CommitIndex, AppliedIndex: st.LastApplied,
		})
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Raft service + transport
// ---------------------------------------------------------------------------

type raftService struct {
	pb.UnimplementedRaftServer
	n *Node
}

func (s *raftService) group(g uint32) (*raft.Raft, error) {
	r, ok := s.n.groups[g]
	if !ok {
		return nil, fmt.Errorf("unknown group %d", g)
	}
	return r, nil
}

func (s *raftService) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleRequestVote(req), nil
}

func (s *raftService) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleAppendEntries(req), nil
}

func (s *raftService) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	r, err := s.group(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleInstallSnapshot(req), nil
}

type grpcTransport struct {
	addrs map[string]string
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newGRPCTransport(addrs map[string]string) *grpcTransport {
	return &grpcTransport{addrs: addrs, conns: map[string]*grpc.ClientConn{}}
}

func (t *grpcTransport) client(peer string) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return pb.NewRaftClient(c), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", peer)
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(fastReconnect),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsg), grpc.MaxCallSendMsgSize(maxMsg)))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = c
	return pb.NewRaftClient(c), nil
}

func (t *grpcTransport) RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *grpcTransport) AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *grpcTransport) InstallSnapshot(ctx context.Context, peer string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

func (t *grpcTransport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
}
