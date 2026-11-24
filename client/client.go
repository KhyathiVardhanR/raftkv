// Package client is the RaftKV Go client. It discovers the cluster layout,
// routes each key to its shard via the same consistent-hash ring the servers
// use, caches each shard's leader, and follows NOT_LEADER redirects and
// retries across leader failover.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"raftkv/internal/shard"
	pb "raftkv/proto/raftkvpb"
)

var ErrTimeout = errors.New("raftkv: request timed out")

type Client struct {
	ring  *shard.Ring
	ids   []string
	conns map[string]*grpc.ClientConn
	kv    map[string]pb.KVClient

	mu      sync.Mutex
	leaders map[uint32]string
	rr      int

	// Session state for exactly-once writes.
	clientID uint64
	nextReq  uint64
	inflight map[uint64]struct{}

	// PerAttemptTimeout bounds a single RPC attempt.
	PerAttemptTimeout time.Duration
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}))
}

// New connects using any reachable seed address and learns the topology.
func New(ctx context.Context, seeds []string) (*Client, error) {
	var info *pb.ClusterInfoResponse
	var lastErr error
	for _, s := range seeds {
		c, err := dial(s)
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		info, err = pb.NewKVClient(c).ClusterInfo(cctx, &pb.ClusterInfoRequest{})
		cancel()
		c.Close()
		if err == nil {
			break
		}
		lastErr = err
	}
	if info == nil {
		return nil, fmt.Errorf("raftkv: no seed reachable: %v", lastErr)
	}
	cl := &Client{
		ring:              shard.NewRing(int(info.NumShards), int(info.Vnodes)),
		conns:             map[string]*grpc.ClientConn{},
		kv:                map[string]pb.KVClient{},
		leaders:           map[uint32]string{},
		PerAttemptTimeout: 1500 * time.Millisecond,
		clientID:          rand.Uint64() | 1,
		inflight:          map[uint64]struct{}{},
	}
	for _, n := range info.Nodes {
		c, err := dial(n.Addr)
		if err != nil {
			return nil, err
		}
		cl.ids = append(cl.ids, n.Id)
		cl.conns[n.Id] = c
		cl.kv[n.Id] = pb.NewKVClient(c)
	}
	for _, s := range info.Shards {
		if s.Leader != "" {
			cl.leaders[s.Shard] = s.Leader
		}
	}
	return cl, nil
}

func (c *Client) Close() {
	for _, cc := range c.conns {
		cc.Close()
	}
}

// Shard returns the shard a key maps to.
func (c *Client) Shard(key []byte) uint32 { return c.ring.Locate(key) }

// Nodes returns member ids.
func (c *Client) Nodes() []string { return c.ids }

func (c *Client) target(g uint32) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.leaders[g]; ok {
		return l
	}
	c.rr++
	return c.ids[c.rr%len(c.ids)]
}

func (c *Client) update(g uint32, tried string, h *pb.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h != nil && h.Leader != "" && h.Leader != tried {
		c.leaders[g] = h.Leader
		return
	}
	if c.leaders[g] == tried {
		delete(c.leaders, g)
	}
	c.rr++
}

// do runs call against the shard leader, retrying until ctx expires.
func (c *Client) do(ctx context.Context, key []byte, call func(ctx context.Context, kv pb.KVClient) (*pb.Header, error)) error {
	g := c.ring.Locate(key)
	backoff := 5 * time.Millisecond
	for {
		node := c.target(g)
		actx, cancel := context.WithTimeout(ctx, c.PerAttemptTimeout)
		h, err := call(actx, c.kv[node])
		cancel()
		if err == nil && h.Code == pb.Code_OK {
			c.mu.Lock()
			c.leaders[g] = node
			c.mu.Unlock()
			return nil
		}
		if err == nil && h.Code == pb.Code_NOT_LEADER && h.Leader != "" && h.Leader != node {
			c.update(g, node, h) // redirect immediately
			continue
		}
		c.update(g, node, h)
		if ctx.Err() != nil {
			return ErrTimeout
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ErrTimeout
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}

// begin allocates a request id; every retry of the request reuses it so the
// servers apply it at most once.
func (c *Client) begin() *pb.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextReq++
	c.inflight[c.nextReq] = struct{}{}
	return &pb.Session{ClientId: c.clientID, RequestId: c.nextReq, MinOutstanding: c.minOutstandingLocked()}
}

func (c *Client) minOutstandingLocked() uint64 {
	m := c.nextReq + 1
	for id := range c.inflight {
		if id < m {
			m = id
		}
	}
	return m
}

func (c *Client) end(s *pb.Session) {
	c.mu.Lock()
	delete(c.inflight, s.RequestId)
	c.mu.Unlock()
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	s := c.begin()
	defer c.end(s)
	return c.do(ctx, key, func(ctx context.Context, kv pb.KVClient) (*pb.Header, error) {
		c.mu.Lock()
		s.MinOutstanding = c.minOutstandingLocked()
		c.mu.Unlock()
		r, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value, Session: s})
		return r.GetHeader(), err
	})
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	s := c.begin()
	defer c.end(s)
	return c.do(ctx, key, func(ctx context.Context, kv pb.KVClient) (*pb.Header, error) {
		c.mu.Lock()
		s.MinOutstanding = c.minOutstandingLocked()
		c.mu.Unlock()
		r, err := kv.Delete(ctx, &pb.DeleteRequest{Key: key, Session: s})
		return r.GetHeader(), err
	})
}

// Get performs a linearizable read.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	err = c.do(ctx, key, func(ctx context.Context, kv pb.KVClient) (*pb.Header, error) {
		r, err := kv.Get(ctx, &pb.GetRequest{Key: key})
		if err == nil {
			value, found = r.Value, r.Found
		}
		return r.GetHeader(), err
	})
	return
}

// ClusterInfo asks a specific node for its view.
func (c *Client) ClusterInfo(ctx context.Context, node string) (*pb.ClusterInfoResponse, error) {
	return c.kv[node].ClusterInfo(ctx, &pb.ClusterInfoRequest{})
}
