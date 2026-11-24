// Package wal implements the Raft write-ahead log: an append-only file of
// CRC-protected records holding the persistent Raft state (term, vote), log
// entries and compaction marks. A torn or corrupt tail (e.g. from a crash
// mid-write) is detected by length/CRC checks and truncated on open.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "raftkv/proto/raftkvpb"
)

const (
	recHardState byte = 1 // term u64 | voteLen u16 | vote
	recEntries   byte = 2 // n u32 | n * (index u64 | term u64 | len u32 | data)
	recSnapshot  byte = 3 // index u64 | term u64 : entries <= index were compacted
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// State is everything recovered from the log on open.
type State struct {
	Term      uint64
	Vote      string
	SnapIndex uint64
	SnapTerm  uint64
	Entries   []*pb.Entry // contiguous, starting at SnapIndex+1
}

type WAL struct {
	path   string
	f      *os.File
	w      *bufio.Writer
	buf    []byte
	NoSync bool
	// OnSync, if set, observes the duration of every sync (for metrics).
	OnSync func(time.Duration)
	// Group, if set, shares device cache flushes with the node's other logs.
	Group *GroupSyncer
}

// Open opens (or creates) the WAL at path and replays it.
func Open(path string) (*WAL, *State, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	st, goodLen, err := replay(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	// Drop any torn tail so new records start at a clean boundary.
	if err := f.Truncate(goodLen); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(goodLen, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriterSize(f, 1<<20)}, st, nil
}

func replay(f *os.File) (*State, int64, error) {
	st := &State{}
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return st, off, nil // clean EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n == 0 || n > 512<<20 {
			return st, off, nil
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return st, off, nil // torn body
		}
		if crc32.Checksum(body, crcTable) != sum {
			return st, off, nil // corrupt tail
		}
		if err := apply(st, body); err != nil {
			return nil, 0, fmt.Errorf("wal: record at offset %d: %w", off, err)
		}
		off += 8 + int64(n)
	}
}

func apply(st *State, b []byte) error {
	switch b[0] {
	case recHardState:
		if len(b) < 11 {
			return errors.New("short hardstate")
		}
		st.Term = binary.LittleEndian.Uint64(b[1:9])
		vl := int(binary.LittleEndian.Uint16(b[9:11]))
		st.Vote = string(b[11 : 11+vl])
	case recSnapshot:
		idx := binary.LittleEndian.Uint64(b[1:9])
		term := binary.LittleEndian.Uint64(b[9:17])
		// Keep entries after the compaction point, drop the rest.
		var keep []*pb.Entry
		for _, e := range st.Entries {
			if e.Index > idx {
				keep = append(keep, e)
			}
		}
		if len(keep) > 0 && keep[0].Index != idx+1 {
			keep = nil
		}
		st.SnapIndex, st.SnapTerm, st.Entries = idx, term, keep
	case recEntries:
		n := binary.LittleEndian.Uint32(b[1:5])
		p := 5
		for i := uint32(0); i < n; i++ {
			idx := binary.LittleEndian.Uint64(b[p:])
			term := binary.LittleEndian.Uint64(b[p+8:])
			dl := int(binary.LittleEndian.Uint32(b[p+16:]))
			p += 20
			data := append([]byte(nil), b[p:p+dl]...)
			p += dl
			if idx <= st.SnapIndex {
				continue
			}
			// An entry at index i overwrites (truncates) everything from i on.
			keepTo := int(idx - st.SnapIndex - 1)
			if keepTo > len(st.Entries) {
				return fmt.Errorf("gap in log: have up to %d, got %d", st.SnapIndex+uint64(len(st.Entries)), idx)
			}
			st.Entries = append(st.Entries[:keepTo], &pb.Entry{Index: idx, Term: term, Data: data})
		}
	default:
		return fmt.Errorf("unknown record type %d", b[0])
	}
	return nil
}

func (w *WAL) writeRecord(body []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(body, crcTable))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.w.Write(body)
	return err
}

func encodeHardState(term uint64, vote string) []byte {
	b := make([]byte, 11+len(vote))
	b[0] = recHardState
	binary.LittleEndian.PutUint64(b[1:9], term)
	binary.LittleEndian.PutUint16(b[9:11], uint16(len(vote)))
	copy(b[11:], vote)
	return b
}

func encodeSnapshot(idx, term uint64) []byte {
	b := make([]byte, 17)
	b[0] = recSnapshot
	binary.LittleEndian.PutUint64(b[1:9], idx)
	binary.LittleEndian.PutUint64(b[9:17], term)
	return b
}

func (w *WAL) encodeEntries(ents []*pb.Entry) []byte {
	size := 5
	for _, e := range ents {
		size += 20 + len(e.Data)
	}
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	b := w.buf[:size]
	b[0] = recEntries
	binary.LittleEndian.PutUint32(b[1:5], uint32(len(ents)))
	p := 5
	for _, e := range ents {
		binary.LittleEndian.PutUint64(b[p:], e.Index)
		binary.LittleEndian.PutUint64(b[p+8:], e.Term)
		binary.LittleEndian.PutUint32(b[p+16:], uint32(len(e.Data)))
		p += 20
		p += copy(b[p:], e.Data)
	}
	return b
}

// SaveHardState appends the current term and vote (buffered; call Sync).
func (w *WAL) SaveHardState(term uint64, vote string) error {
	return w.writeRecord(encodeHardState(term, vote))
}

// SaveEntries appends entries (buffered; call Sync). Writing an entry with an
// index already present logically truncates the log from that index on.
func (w *WAL) SaveEntries(ents []*pb.Entry) error {
	const chunk = 4096
	for len(ents) > 0 {
		n := len(ents)
		if n > chunk {
			n = chunk
		}
		if err := w.writeRecord(w.encodeEntries(ents[:n])); err != nil {
			return err
		}
		ents = ents[n:]
	}
	return nil
}

// Sync flushes buffered records and fsyncs the file.
func (w *WAL) Sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.NoSync {
		return nil
	}
	start := time.Now()
	var err error
	if w.Group != nil && needsBarrier() {
		if err = fileSync(w.f); err == nil {
			err = w.Group.Barrier()
		}
	} else {
		err = w.f.Sync() // F_FULLFSYNC on darwin
	}
	if w.OnSync != nil {
		w.OnSync(time.Since(start))
	}
	return err
}

// Rewrite atomically replaces the WAL with a compacted image: hard state, a
// snapshot mark, and the retained entries. Used after log compaction and
// snapshot installation.
func (w *WAL) Rewrite(term uint64, vote string, snapIdx, snapTerm uint64, ents []*pb.Entry) error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	nw := &WAL{path: w.path, f: f, w: bufio.NewWriterSize(f, 1<<20), NoSync: w.NoSync, OnSync: w.OnSync, Group: w.Group}
	if err := nw.writeRecord(encodeHardState(term, vote)); err != nil {
		f.Close()
		return err
	}
	if err := nw.writeRecord(encodeSnapshot(snapIdx, snapTerm)); err != nil {
		f.Close()
		return err
	}
	if err := nw.SaveEntries(ents); err != nil {
		f.Close()
		return err
	}
	if err := nw.w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		f.Close()
		return err
	}
	if d, err := os.Open(filepath.Dir(w.path)); err == nil {
		d.Sync()
		d.Close()
	}
	w.f.Close()
	w.f, w.w = f, nw.w
	return nil
}

func (w *WAL) Close() error {
	w.w.Flush()
	return w.f.Close()
}
