// Command raftkv-lab spawns a local RaftKV cluster as real OS processes and
// runs experiments against it.
//
//	raftkv-lab up       -n 3                 # run a cluster until Ctrl-C
//	raftkv-lab bench    -n 3 -matrix         # spawn cluster + run benchmark matrix
//	raftkv-lab failover -n 3 -trials 20      # SIGKILL leaders under load, measure
//	                                         # recovery, check linearizability
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anishathalye/porcupine"

	"raftkv/client"
	"raftkv/internal/bench"
	"raftkv/internal/lincheck"
)

type cluster struct {
	bin      string
	dir      string
	n        int
	shards   int
	extra    []string
	ids      []string
	addrs    map[string]string
	metrics  map[string]string
	mu       sync.Mutex
	procs    map[string]*exec.Cmd
	logFiles map[string]*os.File
}

func newCluster(bin, dir string, n, shards, basePort int, extra []string) *cluster {
	c := &cluster{bin: bin, dir: dir, n: n, shards: shards, extra: extra,
		addrs: map[string]string{}, metrics: map[string]string{}, procs: map[string]*exec.Cmd{}, logFiles: map[string]*os.File{}}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.addrs[id] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		c.metrics[id] = fmt.Sprintf("127.0.0.1:%d", basePort+100+i)
	}
	return c
}

func (c *cluster) nodesFlag() string {
	var parts []string
	for _, id := range c.ids {
		parts = append(parts, id+"="+c.addrs[id])
	}
	return strings.Join(parts, ",")
}

func (c *cluster) seeds() []string {
	var s []string
	for _, id := range c.ids {
		s = append(s, c.addrs[id])
	}
	return s
}

func (c *cluster) start(id string) {
	args := append([]string{"-id", id, "-nodes", c.nodesFlag(), "-dir", filepath.Join(c.dir, id),
		"-shards", fmt.Sprint(c.shards), "-metrics", c.metrics[id]}, c.extra...)
	cmd := exec.Command(c.bin, args...)
	lf, _ := os.OpenFile(filepath.Join(c.dir, id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		log.Fatalf("start %s: %v", id, err)
	}
	c.mu.Lock()
	c.procs[id], c.logFiles[id] = cmd, lf
	c.mu.Unlock()
}

// kill sends SIGKILL: no graceful shutdown, no final flush.
func (c *cluster) kill(id string) {
	c.mu.Lock()
	cmd := c.procs[id]
	delete(c.procs, id)
	c.mu.Unlock()
	if cmd != nil {
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
	}
}

func (c *cluster) startAll() {
	os.MkdirAll(c.dir, 0o755)
	for _, id := range c.ids {
		c.start(id)
	}
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cmd := range c.procs {
		cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, cmd := range c.procs {
		cmd.Wait()
	}
}

// waitReady waits until every shard has a leader that accepts writes.
func (c *cluster) waitReady(ctx context.Context) *client.Client {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := client.New(ctx, c.seeds())
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ok := true
		for s := 0; s < c.shards*50 && ok; s++ {
			cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			ok = cl.Put(cctx, []byte(fmt.Sprintf("__ready%d", s)), []byte("1")) == nil
			cancel()
		}
		if ok {
			return cl
		}
		cl.Close()
	}
	log.Fatal("cluster did not become ready")
	return nil
}

func (c *cluster) leaderOf(ctx context.Context, cl *client.Client, shard uint32) (string, uint64) {
	var best string
	var bestTerm uint64
	for _, id := range c.ids {
		c.mu.Lock()
		_, live := c.procs[id]
		c.mu.Unlock()
		if !live {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		info, err := cl.ClusterInfo(cctx, id)
		cancel()
		if err != nil {
			continue
		}
		s := info.Shards[shard]
		if s.Leader != "" && s.Term >= bestTerm {
			best, bestTerm = s.Leader, s.Term
		}
	}
	return best, bestTerm
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raftkv-lab {up|bench|failover} [flags]")
		os.Exit(2)
	}
	sub := os.Args[1]
	fs := flag.NewFlagSet(sub, flag.ExitOnError)
	var (
		bin      = fs.String("bin", "./bin/raftkv", "path to the raftkv server binary")
		dir      = fs.String("dir", "", "data dir (default: fresh temp dir)")
		n        = fs.Int("n", 3, "number of nodes")
		shards   = fs.Int("shards", 8, "number of shards")
		port     = fs.Int("base-port", 7000, "node i listens on base-port+i, metrics on base-port+100+i")
		extra    = fs.String("server-args", "", "extra args passed to every server")
		trials   = fs.Int("trials", 20, "failover: number of leader kills")
		loadConc = fs.Int("load", 16, "failover: concurrent background clients")
		keys     = fs.Int("keys", 100000, "bench: key space")
		vsize    = fs.Int("value-size", 128, "bench: value size")
		dur      = fs.Duration("duration", 10*time.Second, "bench: duration per workload")
		matrix   = fs.Bool("matrix", true, "bench: run the preset matrix")
		conc     = fs.Int("concurrency", 64, "bench: clients when -matrix=false")
		readR    = fs.Float64("read-ratio", .5, "bench: read ratio when -matrix=false")
	)
	fs.Parse(os.Args[2:])
	if *dir == "" {
		*dir, _ = os.MkdirTemp("", "raftkv-lab-")
	}
	var ex []string
	if *extra != "" {
		ex = strings.Fields(*extra)
	}
	c := newCluster(*bin, *dir, *n, *shards, *port, ex)
	c.startAll()
	defer c.stopAll()
	ctx := context.Background()
	cl := c.waitReady(ctx)
	defer cl.Close()
	log.Printf("%d-node cluster ready (%d shards), data in %s", *n, *shards, *dir)

	switch sub {
	case "up":
		for _, id := range c.ids {
			log.Printf("  %s  grpc=%s  metrics=http://%s/metrics", id, c.addrs[id], c.metrics[id])
		}
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
	case "bench":
		if err := bench.Preload(ctx, cl, *keys, *vsize, 128); err != nil {
			log.Fatal(err)
		}
		base := bench.Workload{Keys: *keys, ValueSize: *vsize, Duration: *dur, Warmup: 2 * time.Second}
		var wls []bench.Workload
		if *matrix {
			wls = bench.Matrix(base)
		} else {
			w := base
			w.Name, w.ReadRatio, w.Concurrency = "custom", *readR, *conc
			wls = append(wls, w)
		}
		fmt.Printf("\n### %d nodes, %d shards\n\n", *n, *shards)
		fmt.Println("| Workload | Clients | Throughput (ops/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |")
		fmt.Println("|---|---:|---:|---:|---:|---:|---:|")
		for _, w := range wls {
			r := bench.Run(ctx, cl, w)
			log.Print(r)
			fmt.Printf("| %s | %d | %.0f | %.2f | %.2f | %.2f | %d |\n", r.Name, r.Concurrency, r.Throughput,
				float64(r.P50)/1e6, float64(r.P95)/1e6, float64(r.P99)/1e6, r.Errors)
		}
	case "failover":
		runFailover(ctx, c, cl, *trials, *loadConc)
	default:
		log.Fatalf("unknown subcommand %q", sub)
	}
}

func runFailover(ctx context.Context, c *cluster, cl *client.Client, trials, loadConc int) {
	// Background load, recorded for the linearizability check.
	rec := lincheck.NewRecorder()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for ci := 0; ci < loadConc; ci++ {
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
				key := fmt.Sprintf("lin%d", rng.Intn(64))
				octx, cancel := context.WithTimeout(ctx, 4*time.Second)
				call := rec.Now()
				if rng.Intn(2) == 0 {
					v := fmt.Sprintf("c%d-%d", ci, seq)
					err := cl.Put(octx, []byte(key), []byte(v))
					rec.Put(ci, key, v, call, err == nil)
				} else if v, _, err := cl.Get(octx, []byte(key)); err == nil {
					rec.Get(ci, key, string(v), call)
				}
				cancel()
			}
		}(ci)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var electT, writeT []time.Duration
	for t := 0; t < trials; t++ {
		time.Sleep(time.Duration(500+rng.Intn(500)) * time.Millisecond)
		shard := uint32(rng.Intn(c.shards))
		victim, oldTerm := c.leaderOf(ctx, cl, shard)
		if victim == "" {
			log.Printf("trial %d: shard %d has no leader yet, skipping", t, shard)
			continue
		}
		// A probe key that hashes to the shard.
		var probe []byte
		for i := 0; ; i++ {
			k := []byte(fmt.Sprintf("probe-%d-%d", t, i))
			if cl.Shard(k) == shard {
				probe = k
				break
			}
		}

		killedAt := time.Now()
		c.kill(victim)

		var elected time.Duration
		done := make(chan struct{})
		go func() { // detect the new leader
			defer close(done)
			for time.Since(killedAt) < 10*time.Second {
				l, term := c.leaderOf(ctx, cl, shard)
				if l != "" && l != victim && term > oldTerm {
					elected = time.Since(killedAt)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
		var wrote time.Duration
		// One logical write; the client retries/redirects internally until a
		// new leader accepts it.
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if cl.Put(pctx, probe, []byte("x")) == nil {
			wrote = time.Since(killedAt)
		}
		cancel()
		<-done
		log.Printf("trial %2d: SIGKILL %s (leader of shard %d, term %d) -> new leader after %v, first write after %v",
			t, victim, shard, oldTerm, elected.Round(time.Millisecond), wrote.Round(time.Millisecond))
		if elected > 0 {
			electT = append(electT, elected)
		}
		if wrote > 0 {
			writeT = append(writeT, wrote)
		}
		c.start(victim) // restart; it recovers from its WAL and catches up
	}
	close(stop)
	wg.Wait()

	summary := func(name string, ds []time.Duration) {
		if len(ds) == 0 {
			fmt.Printf("%s: no samples\n", name)
			return
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		p := func(q float64) time.Duration { return ds[int(q*float64(len(ds)-1))].Round(time.Millisecond) }
		fmt.Printf("%-28s n=%d  min=%v  p50=%v  p95=%v  max=%v\n", name, len(ds), ds[0].Round(time.Millisecond), p(.5), p(.95), ds[len(ds)-1].Round(time.Millisecond))
	}
	fmt.Printf("\n### Leader failover, %d nodes (SIGKILL under load)\n\n", c.n)
	summary("new leader elected", electT)
	summary("first successful write", writeT)

	log.Printf("checking linearizability of %d operations (%d indeterminate puts)...", rec.Len(), rec.Indeterminate)
	res := rec.Check(2 * time.Minute)
	fmt.Printf("linearizability (Porcupine) over %d ops across %d leader kills: %s\n", rec.Len(), len(electT), res)
	if res == porcupine.Illegal {
		os.Exit(1)
	}
}
