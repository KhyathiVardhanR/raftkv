// Package storage is the replicated state machine: a key-value store on an
// LSM-tree engine (Pebble, a RocksDB-compatible LSM written in Go). One engine
// per node holds every shard, each under its own key prefix, together with the
// shard's last-applied Raft index and its client session table, so applied
// state, its position in the log, and duplicate-detection state are always
// updated atomically.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"

	pb "raftkv/proto/raftkvpb"
)

const (
	OpPut    byte = 1
	OpDelete byte = 2
)

// Command is a replicated mutation. ClientID 0 means "no session" (applied
// unconditionally).
type Command struct {
	Op             byte
	ClientID       uint64
	RequestID      uint64
	MinOutstanding uint64
	Key, Value     []byte
}

// Encode: op | clientID u64 | requestID u64 | minOutstanding u64 | uvarint klen | key | value
func (c Command) Encode() []byte {
	b := make([]byte, 25, 25+binary.MaxVarintLen32+len(c.Key)+len(c.Value))
	b[0] = c.Op
	binary.BigEndian.PutUint64(b[1:], c.ClientID)
	binary.BigEndian.PutUint64(b[9:], c.RequestID)
	binary.BigEndian.PutUint64(b[17:], c.MinOutstanding)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	return append(b, c.Value...)
}

func decodeCommand(b []byte) (Command, error) {
	var c Command
	if len(b) < 26 {
		return c, errors.New("short command")
	}
	c.Op = b[0]
	c.ClientID = binary.BigEndian.Uint64(b[1:])
	c.RequestID = binary.BigEndian.Uint64(b[9:])
	c.MinOutstanding = binary.BigEndian.Uint64(b[17:])
	kl, n := binary.Uvarint(b[25:])
	p := 25 + n
	if n <= 0 || p+int(kl) > len(b) {
		return c, errors.New("bad command")
	}
	c.Key, c.Value = b[p:p+int(kl)], b[p+int(kl):]
	return c, nil
}

// session tracks which requests of one client have been applied, so retries
// of the same request are not applied twice (exactly-once semantics).
// Requests below floor are either applied or abandoned by the client.
type session struct {
	floor uint64
	done  map[uint64]struct{}
}

func (s *session) encode() []byte {
	ids := make([]uint64, 0, len(s.done))
	for id := range s.done {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b := binary.BigEndian.AppendUint64(nil, s.floor)
	for _, id := range ids {
		b = binary.BigEndian.AppendUint64(b, id)
	}
	return b
}

func decodeSession(b []byte) *session {
	s := &session{floor: binary.BigEndian.Uint64(b), done: map[uint64]struct{}{}}
	for p := 8; p+8 <= len(b); p += 8 {
		s.done[binary.BigEndian.Uint64(b[p:])] = struct{}{}
	}
	return s
}

// quietLogger drops Pebble's informational logs but keeps errors.
type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})      {}
func (quietLogger) Errorf(f string, a ...interface{}) { log.Printf("pebble: "+f, a...) }
func (quietLogger) Fatalf(f string, a ...interface{}) { log.Fatalf("pebble: "+f, a...) }

type Engine struct {
	db *pebble.DB
}

func Open(dir string) (*Engine, error) {
	db, err := pebble.Open(dir, &pebble.Options{
		Cache:        pebble.NewCache(128 << 20),
		MemTableSize: 64 << 20,
		Logger:       quietLogger{},
	})
	if err != nil {
		return nil, err
	}
	return &Engine{db: db}, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) Metrics() *pebble.Metrics { return e.db.Metrics() }

func prefix(tag byte, id uint32) []byte {
	b := []byte{tag, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], id)
	return b
}

// Shard returns the state machine for one shard.
func (e *Engine) Shard(id uint32) (*Shard, error) {
	s := &Shard{
		db: e.db, id: id,
		dataPrefix: prefix('d', id), dataEnd: prefix('d', id+1),
		sessPrefix: prefix('s', id), sessEnd: prefix('s', id+1),
		metaKey:  prefix('m', id),
		sessions: map[uint64]*session{},
	}
	if v, c, err := e.db.Get(s.metaKey); err == nil {
		s.appliedIdx, s.appliedTerm = binary.BigEndian.Uint64(v[0:8]), binary.BigEndian.Uint64(v[8:16])
		c.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, err
	}
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: s.sessPrefix, UpperBound: s.sessEnd})
	if err != nil {
		return nil, err
	}
	for it.First(); it.Valid(); it.Next() {
		cid := binary.BigEndian.Uint64(it.Key()[len(s.sessPrefix):])
		s.sessions[cid] = decodeSession(it.Value())
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	return s, nil
}

// Shard implements raft.StateMachine for one shard's key range.
type Shard struct {
	db                  *pebble.DB
	id                  uint32
	dataPrefix, dataEnd []byte
	sessPrefix, sessEnd []byte
	metaKey             []byte

	mu          sync.Mutex // guards applied*, sessions
	appliedIdx  uint64
	appliedTerm uint64
	sessions    map[uint64]*session
}

func (s *Shard) dataKey(k []byte) []byte {
	out := make([]byte, len(s.dataPrefix)+len(k))
	copy(out, s.dataPrefix)
	copy(out[len(s.dataPrefix):], k)
	return out
}

func (s *Shard) sessKey(cid uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), s.sessPrefix...), cid)
}

func metaValue(idx, term uint64) []byte {
	var v [16]byte
	binary.BigEndian.PutUint64(v[0:8], idx)
	binary.BigEndian.PutUint64(v[8:16], term)
	return v[:]
}

func (s *Shard) Apply(ents []*pb.Entry) error {
	b := s.db.NewBatch()
	defer b.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	dirty := map[uint64]*session{}
	for _, e := range ents {
		if len(e.Data) == 0 {
			continue // leader no-op
		}
		c, err := decodeCommand(e.Data)
		if err != nil {
			return fmt.Errorf("entry %d: %w", e.Index, err)
		}
		if c.ClientID != 0 {
			ss := s.sessions[c.ClientID]
			if ss == nil {
				ss = &session{done: map[uint64]struct{}{}}
				s.sessions[c.ClientID] = ss
			}
			_, dup := ss.done[c.RequestID]
			if c.MinOutstanding > ss.floor {
				ss.floor = c.MinOutstanding
				for id := range ss.done {
					if id < ss.floor {
						delete(ss.done, id)
					}
				}
			}
			dirty[c.ClientID] = ss
			if dup || c.RequestID < ss.floor {
				continue // retry of an applied (or abandoned) request
			}
			ss.done[c.RequestID] = struct{}{}
		}
		switch c.Op {
		case OpPut:
			b.Set(s.dataKey(c.Key), c.Value, nil)
		case OpDelete:
			b.Delete(s.dataKey(c.Key), nil)
		}
	}
	for cid, ss := range dirty {
		b.Set(s.sessKey(cid), ss.encode(), nil)
	}
	last := ents[len(ents)-1]
	b.Set(s.metaKey, metaValue(last.Index, last.Term), nil)
	// The Raft WAL already holds these entries durably; the engine's own
	// WAL need not fsync on every apply (Sync() is called before compaction).
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	s.appliedIdx, s.appliedTerm = last.Index, last.Term
	return nil
}

func (s *Shard) Applied() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appliedIdx, s.appliedTerm
}

func (s *Shard) Sync() error {
	return s.db.LogData(nil, pebble.Sync)
}

// Snapshot captures a consistent image of the shard (sessions + data) using an
// engine snapshot. Format: uvarint nSessions, n*(clientID u64, uvarint len,
// session), then repeated (uvarint klen, key, uvarint vlen, value).
func (s *Shard) Snapshot() ([]byte, uint64, uint64, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	var idx, term uint64
	if v, c, err := snap.Get(s.metaKey); err == nil {
		idx, term = binary.BigEndian.Uint64(v[0:8]), binary.BigEndian.Uint64(v[8:16])
		c.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, 0, 0, err
	}
	type kv struct{ k, v []byte }
	var sess []kv
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: s.sessPrefix, UpperBound: s.sessEnd})
	if err != nil {
		return nil, 0, 0, err
	}
	for it.First(); it.Valid(); it.Next() {
		sess = append(sess, kv{append([]byte(nil), it.Key()[len(s.sessPrefix):]...), append([]byte(nil), it.Value()...)})
	}
	it.Close()
	out := binary.AppendUvarint(nil, uint64(len(sess)))
	for _, x := range sess {
		out = append(out, x.k...)
		out = binary.AppendUvarint(out, uint64(len(x.v)))
		out = append(out, x.v...)
	}

	it, err = snap.NewIter(&pebble.IterOptions{LowerBound: s.dataPrefix, UpperBound: s.dataEnd})
	if err != nil {
		return nil, 0, 0, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()[len(s.dataPrefix):]
		v := it.Value()
		out = binary.AppendUvarint(out, uint64(len(k)))
		out = append(out, k...)
		out = binary.AppendUvarint(out, uint64(len(v)))
		out = append(out, v...)
	}
	return out, idx, term, it.Error()
}

func (s *Shard) Restore(data []byte, idx, term uint64) error {
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(s.dataPrefix, s.dataEnd, nil); err != nil {
		return err
	}
	if err := b.DeleteRange(s.sessPrefix, s.sessEnd, nil); err != nil {
		return err
	}
	sessions := map[uint64]*session{}
	ns, n := binary.Uvarint(data)
	p := n
	for i := uint64(0); i < ns; i++ {
		cid := binary.BigEndian.Uint64(data[p:])
		p += 8
		l, n := binary.Uvarint(data[p:])
		p += n
		v := data[p : p+int(l)]
		p += int(l)
		sessions[cid] = decodeSession(v)
		b.Set(s.sessKey(cid), v, nil)
	}
	for p < len(data) {
		kl, n := binary.Uvarint(data[p:])
		p += n
		k := data[p : p+int(kl)]
		p += int(kl)
		vl, n := binary.Uvarint(data[p:])
		p += n
		v := data[p : p+int(vl)]
		p += int(vl)
		b.Set(s.dataKey(k), v, nil)
	}
	b.Set(s.metaKey, metaValue(idx, term), nil)
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	s.mu.Lock()
	s.appliedIdx, s.appliedTerm = idx, term
	s.sessions = sessions
	s.mu.Unlock()
	return nil
}

// Get reads a key from local applied state.
func (s *Shard) Get(key []byte) ([]byte, bool, error) {
	v, c, err := s.db.Get(s.dataKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	c.Close()
	return out, true, nil
}
