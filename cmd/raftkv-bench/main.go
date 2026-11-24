// Command raftkv-bench drives load against a running RaftKV cluster.
//
// Single workload:
//
//	raftkv-bench -addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -concurrency 64 -read-ratio 0.5
//
// Preset matrix (write-heavy, balanced, read-heavy at several concurrencies):
//
//	raftkv-bench -addrs ... -matrix
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"raftkv/client"
	"raftkv/internal/bench"
)

func main() {
	var (
		addrs    = flag.String("addrs", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated seed addresses")
		conc     = flag.Int("concurrency", 64, "concurrent clients")
		readR    = flag.Float64("read-ratio", 0.5, "fraction of operations that are reads")
		keys     = flag.Int("keys", 100000, "key space size")
		vsize    = flag.Int("value-size", 128, "value size in bytes")
		dur      = flag.Duration("duration", 15*time.Second, "measurement duration per workload")
		warmup   = flag.Duration("warmup", 2*time.Second, "warmup before measuring")
		preload  = flag.Bool("preload", true, "write every key once before measuring")
		matrix   = flag.Bool("matrix", false, "run the preset workload matrix")
		markdown = flag.Bool("markdown", false, "print results as a markdown table")
	)
	flag.Parse()
	ctx := context.Background()
	cl, err := client.New(ctx, strings.Split(*addrs, ","))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	if *preload {
		t0 := time.Now()
		if err := bench.Preload(ctx, cl, *keys, *vsize, 128); err != nil {
			log.Fatalf("preload: %v", err)
		}
		log.Printf("preloaded %d keys in %v", *keys, time.Since(t0).Round(time.Millisecond))
	}

	var wls []bench.Workload
	base := bench.Workload{Keys: *keys, ValueSize: *vsize, Duration: *dur, Warmup: *warmup}
	if *matrix {
		wls = bench.Matrix(base)
	} else {
		w := base
		w.Name, w.ReadRatio, w.Concurrency = "custom", *readR, *conc
		wls = append(wls, w)
	}

	var results []bench.Result
	for _, w := range wls {
		r := bench.Run(ctx, cl, w)
		log.Print(r)
		results = append(results, r)
	}
	if *markdown {
		fmt.Println("| Workload | Clients | Throughput (ops/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |")
		fmt.Println("|---|---:|---:|---:|---:|---:|---:|")
		for _, r := range results {
			fmt.Printf("| %s | %d | %.0f | %.2f | %.2f | %.2f | %d |\n", r.Name, r.Concurrency, r.Throughput,
				float64(r.P50)/1e6, float64(r.P95)/1e6, float64(r.P99)/1e6, r.Errors)
		}
	}
}
