// Package shard maps keys to shards (replica groups) with consistent hashing.
// Each shard owns many virtual nodes on a 64-bit hash ring, which evens out
// the key distribution and keeps remapping minimal if the shard count changes.
package shard

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

type Ring struct {
	hashes []uint64
	owners []uint32
}

func NewRing(numShards, vnodes int) *Ring {
	type point struct {
		h uint64
		s uint32
	}
	pts := make([]point, 0, numShards*vnodes)
	for s := 0; s < numShards; s++ {
		for v := 0; v < vnodes; v++ {
			pts = append(pts, point{xxhash.Sum64String(fmt.Sprintf("shard-%d#%d", s, v)), uint32(s)})
		}
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].h < pts[j].h })
	r := &Ring{hashes: make([]uint64, len(pts)), owners: make([]uint32, len(pts))}
	for i, p := range pts {
		r.hashes[i], r.owners[i] = p.h, p.s
	}
	return r
}

// Locate returns the shard owning key: the first virtual node clockwise.
func (r *Ring) Locate(key []byte) uint32 {
	h := xxhash.Sum64(key)
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.owners[i]
}
