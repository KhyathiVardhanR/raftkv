// Package lincheck records concurrent client operations against RaftKV and
// checks the history for linearizability with the Porcupine checker, using a
// key-value register model partitioned per key.
package lincheck

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

type Input struct {
	Op    string // "get" | "put"
	Key   string
	Value string
}

type Output struct {
	Value string // for get; "" means not found
}

type Recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
	// Indeterminate puts that timed out (may or may not have taken effect).
	Indeterminate int
}

func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

func (r *Recorder) Now() int64 { return int64(time.Since(r.start)) }

// Put records a put. If ok is false the outcome is unknown, so the operation
// is kept with an open-ended return time: it may linearize at any later point.
func (r *Recorder) Put(client int, key, value string, call int64, ok bool) {
	ret := r.Now()
	if !ok {
		ret = math.MaxInt64
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !ok {
		r.Indeterminate++
	}
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: Input{"put", key, value}, Output: Output{}, Call: call, Return: ret})
}

// Get records a successful read. Failed reads have no observable effect and
// are omitted.
func (r *Recorder) Get(client int, key, value string, call int64) {
	ret := r.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: Input{"get", key, ""}, Output: Output{value}, Call: call, Return: ret})
}

func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		by := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			by[k] = append(by[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(by))
		for _, ops := range by {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in, out, st := input.(Input), output.(Output), state.(string)
		switch in.Op {
		case "get":
			return out.Value == st, st
		case "put":
			return true, in.Value
		}
		return false, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(Input)
		if in.Op == "get" {
			return fmt.Sprintf("get(%s) -> %q", in.Key, output.(Output).Value)
		}
		return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
	},
}

// Explain returns the operations on keys whose sub-history is not
// linearizable, ordered by invocation time.
func (r *Recorder) Explain(timeout time.Duration) string {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()
	out := ""
	for _, part := range KVModel.Partition(ops) {
		if porcupine.CheckOperationsTimeout(KVModel, part, timeout) != porcupine.Illegal {
			continue
		}
		sort.Slice(part, func(i, j int) bool { return part[i].Call < part[j].Call })
		for _, op := range part {
			ret := "inf"
			if op.Return != math.MaxInt64 {
				ret = fmt.Sprintf("%.1fms", float64(op.Return)/1e6)
			}
			out += fmt.Sprintf("  c%-3d [%9.1fms, %9s] %s\n", op.ClientId, float64(op.Call)/1e6, ret, KVModel.DescribeOperation(op.Input, op.Output))
		}
		break
	}
	return out
}

// Check returns porcupine.Ok, Illegal or Unknown (checker timed out).
func (r *Recorder) Check(timeout time.Duration) porcupine.CheckResult {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()
	return porcupine.CheckOperationsTimeout(KVModel, ops, timeout)
}
