// Package bench is a closed-loop load generator for RaftKV.
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/client"
)

type Workload struct {
	Name        string
	Concurrency int
	ReadRatio   float64 // 0..1
	Keys        int
	ValueSize   int
	Duration    time.Duration
	Warmup      time.Duration
}

type Result struct {
	Workload
	Ops                int64
	Errors             int64
	Throughput         float64 // ops/sec
	P50, P95, P99, Max time.Duration
}

func (r Result) String() string {
	return fmt.Sprintf("%-22s conc=%-4d read=%3.0f%%  %8.0f ops/s  p50=%6.2fms p95=%6.2fms p99=%6.2fms max=%7.2fms errors=%d",
		r.Name, r.Concurrency, r.ReadRatio*100, r.Throughput, ms(r.P50), ms(r.P95), ms(r.P99), ms(r.Max), r.Errors)
}

// Matrix returns the standard workload configurations: four read/write mixes
// at three client concurrencies.
func Matrix(base Workload) []Workload {
	var out []Workload
	for _, m := range []struct {
		name string
		read float64
	}{{"write-only", 0}, {"balanced 50/50", .5}, {"read-heavy 95/5", .95}, {"read-only", 1}} {
		for _, c := range []int{8, 32, 128} {
			w := base
			w.Name, w.ReadRatio, w.Concurrency = m.name, m.read, c
			out = append(out, w)
		}
	}
	return out
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func Key(i int) []byte { return []byte(fmt.Sprintf("user%010d", i)) }

// Preload writes every key once so reads hit existing data.
func Preload(ctx context.Context, cl *client.Client, keys, valueSize, conc int) error {
	val := make([]byte, valueSize)
	rand.Read(val)
	var next atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= keys {
					return
				}
				c, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := cl.Put(c, Key(i), val)
				cancel()
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if e, _ := firstErr.Load().(error); e != nil {
		return e
	}
	return nil
}

func Run(ctx context.Context, cl *client.Client, w Workload) Result {
	val := make([]byte, w.ValueSize)
	rand.Read(val)
	var (
		measuring atomic.Bool
		stop      atomic.Bool
		errs      atomic.Int64
		mu        sync.Mutex
		all       []time.Duration
		wg        sync.WaitGroup
	)
	for i := 0; i < w.Concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			lat := make([]time.Duration, 0, 1<<14)
			for !stop.Load() {
				k := Key(rng.Intn(w.Keys))
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				t0 := time.Now()
				var err error
				if rng.Float64() < w.ReadRatio {
					_, _, err = cl.Get(c, k)
				} else {
					err = cl.Put(c, k, val)
				}
				d := time.Since(t0)
				cancel()
				if !measuring.Load() {
					continue
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				lat = append(lat, d)
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}(time.Now().UnixNano() + int64(i))
	}
	time.Sleep(w.Warmup)
	measuring.Store(true)
	start := time.Now()
	time.Sleep(w.Duration)
	measuring.Store(false)
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(p*float64(len(all)-1))]
	}
	r := Result{Workload: w, Ops: int64(len(all)), Errors: errs.Load()}
	r.Throughput = float64(len(all)) / elapsed.Seconds()
	r.P50, r.P95, r.P99 = pct(.50), pct(.95), pct(.99)
	if len(all) > 0 {
		r.Max = all[len(all)-1]
	}
	return r
}
