package wal

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// GroupSyncer implements node-wide group commit for durability barriers.
//
// A node hosts one Raft log per shard. Making each log durable independently
// costs one device cache flush per log per write batch, and on macOS a real
// flush (F_FULLFSYNC) flushes the *entire* drive cache, so N logs contend for
// a resource that serves only a few hundred flushes per second.
//
// Instead, each log pushes its data to the device with a cheap fsync(2) and
// then calls Barrier. Concurrent callers share a single cache flush: a flush
// that starts after a caller's fsync(2) returned makes that caller's data
// durable, because the flush covers the whole device cache.
//
// On Linux, fsync(2) already flushes the device cache, so the barrier is a
// no-op there.
type GroupSyncer struct {
	mu        sync.Mutex
	cond      *sync.Cond
	requested uint64
	completed uint64
	running   bool
	err       error
	f         *os.File
	OnFlush   func(time.Duration)
}

func NewGroupSyncer(dir string) (*GroupSyncer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "barrier"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	g := &GroupSyncer{f: f}
	g.cond = sync.NewCond(&g.mu)
	return g, nil
}

func needsBarrier() bool { return runtime.GOOS == "darwin" }

// Barrier blocks until a device cache flush that began after this call has
// completed.
func (g *GroupSyncer) Barrier() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requested++
	my := g.requested
	for g.completed < my {
		if g.running {
			g.cond.Wait()
			continue
		}
		g.running = true
		target := g.requested
		g.mu.Unlock()
		start := time.Now()
		err := g.f.Sync() // F_FULLFSYNC on darwin
		if g.OnFlush != nil {
			g.OnFlush(time.Since(start))
		}
		g.mu.Lock()
		g.running = false
		g.completed = target
		g.err = err
		g.cond.Broadcast()
	}
	return g.err
}

func (g *GroupSyncer) Close() error { return g.f.Close() }

// fileSync pushes file data and metadata to the device.
func fileSync(f *os.File) error {
	if needsBarrier() {
		return syscall.Fsync(int(f.Fd())) // plain fsync(2); barrier follows
	}
	return f.Sync()
}
