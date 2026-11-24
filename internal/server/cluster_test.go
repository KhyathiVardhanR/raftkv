package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"raftkv/client"
	"raftkv/internal/lincheck"
)

type testCluster struct {
	t     *testing.T
	nodes map[string]string
	dirs  map[string]string
	mu    sync.Mutex
	live  map[string]*Node
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startCluster(t *testing.T, n, shards int) *testCluster {
	c := &testCluster{t: t, nodes: map[string]string{}, dirs: map[string]string{}, live: map[string]*Node{}}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.nodes[id] = freePort(t)
		c.dirs[id] = t.TempDir()
	}
	for id := range c.nodes {
		c.start(id, shards)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, n := range c.live {
			n.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id string, shards int) {
	n, err := Start(Config{
		ID: id, Listen: c.nodes[id], Nodes: c.nodes, Dir: c.dirs[id], NumShards: shards,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		CompactThreshold: 500, // force compaction + snapshots during the test
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.live[id] = n
	c.mu.Unlock()
}

func (c *testCluster) stop(id string) {
	c.mu.Lock()
	n := c.live[id]
	delete(c.live, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

func (c *testCluster) seeds() []string {
	var s []string
	for _, a := range c.nodes {
		s = append(s, a)
	}
	return s
}

func TestKVBasic(t *testing.T) {
	c := startCluster(t, 3, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := client.New(ctx, c.seeds())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if err := cl.Put(ctx, k, []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	shardsUsed := map[uint32]bool{}
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		shardsUsed[cl.Shard(k)] = true
		v, ok, err := cl.Get(ctx, k)
		if err != nil || !ok || string(v) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("get %s = %q %v %v", k, v, ok, err)
		}
	}
	if len(shardsUsed) != 4 {
		t.Fatalf("keys spread over %d shards, want 4", len(shardsUsed))
	}
	if err := cl.Delete(ctx, []byte("key-1")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := cl.Get(ctx, []byte("key-1")); ok {
		t.Fatal("deleted key still present")
	}
}

// TestLinearizableUnderFailures runs concurrent clients against a 5-node
// cluster while a nemesis crash-stops and restarts nodes (including leaders),
// then verifies the recorded history is linearizable.
func TestLinearizableUnderFailures(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const shards = 4
	c := startCluster(t, 5, shards)
	ctx := context.Background()
	cl, err := client.New(ctx, c.seeds())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cl.PerAttemptTimeout = 500 * time.Millisecond

	rec := lincheck.NewRecorder()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops atomic.Int64
	const clients, keys = 16, 10
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci)))
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				octx, cancel := context.WithTimeout(ctx, 2*time.Second)
				call := rec.Now()
				if rng.Intn(2) == 0 {
					v := fmt.Sprintf("c%d-%d", ci, seq)
					err := cl.Put(octx, []byte(key), []byte(v))
					rec.Put(ci, key, v, call, err == nil)
				} else {
					v, _, err := cl.Get(octx, []byte(key))
					if err == nil {
						rec.Get(ci, key, string(v), call)
					}
				}
				cancel()
				ops.Add(1)
			}
		}(ci)
	}

	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	rng := rand.New(rand.NewSource(42))
	kills := 0
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(300+rng.Intn(400)) * time.Millisecond)
		// Prefer killing a current leader to exercise failover.
		victim := ids[rng.Intn(len(ids))]
		c.mu.Lock()
		for id, n := range c.live {
			if n.Group(uint32(rng.Intn(shards))).IsLeader() {
				victim = id
				break
			}
		}
		c.mu.Unlock()
		c.stop(victim)
		kills++
		time.Sleep(time.Duration(200+rng.Intn(400)) * time.Millisecond)
		c.start(victim, shards)
	}
	close(stop)
	wg.Wait()

	t.Logf("%d ops recorded (%d attempted), %d indeterminate puts, %d node kills", rec.Len(), ops.Load(), rec.Indeterminate, kills)
	res := rec.Check(60 * time.Second)
	if res != porcupine.Ok {
		t.Fatalf("history is not linearizable: %s\n%s", res, rec.Explain(10*time.Second))
	}
}
