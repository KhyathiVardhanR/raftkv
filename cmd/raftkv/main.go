// Command raftkv runs one RaftKV node.
//
//	raftkv -id n1 -listen :7001 -metrics :9101 -dir data/n1 \
//	       -nodes n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 -shards 8
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"raftkv/internal/server"
)

func main() {
	var (
		id       = flag.String("id", "", "node id (must appear in -nodes)")
		listen   = flag.String("listen", "", "gRPC listen address (default: this node's -nodes address)")
		nodes    = flag.String("nodes", "", "comma-separated id=host:port for every member")
		dir      = flag.String("dir", "", "data directory")
		shards   = flag.Int("shards", 8, "number of shards (Raft groups)")
		vnodes   = flag.Int("vnodes", 64, "virtual nodes per shard on the hash ring")
		metrics  = flag.String("metrics", "", "Prometheus /metrics listen address")
		noSync   = flag.Bool("nosync", false, "skip fsync (unsafe; for comparison only)")
		elMin    = flag.Duration("election-min", 200*time.Millisecond, "minimum election timeout")
		elMax    = flag.Duration("election-max", 400*time.Millisecond, "maximum election timeout")
		hb       = flag.Duration("heartbeat", 50*time.Millisecond, "heartbeat interval")
		compactN = flag.Uint64("compact-threshold", 20000, "compact the log after this many applied entries")
	)
	flag.Parse()
	if *id == "" || *nodes == "" || *dir == "" {
		flag.Usage()
		os.Exit(2)
	}
	members := map[string]string{}
	for _, kv := range strings.Split(*nodes, ",") {
		p := strings.SplitN(strings.TrimSpace(kv), "=", 2)
		if len(p) != 2 {
			log.Fatalf("bad -nodes entry %q", kv)
		}
		members[p[0]] = p[1]
	}
	addr, ok := members[*id]
	if !ok {
		log.Fatalf("-id %s not in -nodes", *id)
	}
	if *listen == "" {
		listen = &addr
	}
	n, err := server.Start(server.Config{
		ID: *id, Listen: *listen, Nodes: members, Dir: *dir,
		NumShards: *shards, VNodes: *vnodes, MetricsAddr: *metrics, NoSync: *noSync,
		ElectionTimeoutMin: *elMin, ElectionTimeoutMax: *elMax, HeartbeatInterval: *hb,
		CompactThreshold: *compactN,
	})
	if err != nil {
		log.Fatal(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	n.Stop()
}
