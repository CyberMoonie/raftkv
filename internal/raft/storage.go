package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// storage keeps a node's persistent state in a directory:
//
//	state  current term and vote, replaced atomically on every change
//	log    the log as append-only records
//
// Each log record is a 4-byte length, a 4-byte CRC-32C of the payload and
// the Entry protobuf. A crash in the middle of an append leaves a partial
// record at the end of the file; it is detected by its length or checksum
// and cut off on the next start.
type storage struct {
	dir string
	log *os.File
	// offsets[i] is where the record of entry firstIndex+i starts.
	offsets    []int64
	firstIndex uint64
	size       int64
}

const recordHeaderSize = 8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// openStorage opens or creates the files in dir and returns what was saved.
func openStorage(dir string) (*storage, *raftpb.HardState, []*raftpb.Entry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, err
	}
	hs := &raftpb.HardState{}
	if err := readProto(filepath.Join(dir, "state"), hs); err != nil {
		return nil, nil, nil, err
	}

	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, nil, err
	}
	s := &storage{dir: dir, log: f}
	entries, err := s.readLog()
	if err != nil {
		f.Close()
		return nil, nil, nil, err
	}
	return s, hs, entries, nil
}

func (s *storage) readLog() ([]*raftpb.Entry, error) {
	data, err := io.ReadAll(s.log)
	if err != nil {
		return nil, err
	}
	var entries []*raftpb.Entry
	var off int64
	for off < int64(len(data)) {
		e, n, err := decodeRecord(data[off:])
		if err != nil {
			// Only the last record can be torn by a crash. Damage
			// anywhere else means the disk lost data we promised to keep.
			if off+n < int64(len(data)) {
				return nil, fmt.Errorf("raft: corrupt log record at offset %d: %w", off, err)
			}
			if err := s.log.Truncate(off); err != nil {
				return nil, err
			}
			break
		}
		if len(entries) > 0 && e.Index != entries[len(entries)-1].Index+1 {
			return nil, fmt.Errorf("raft: log jumps from index %d to %d", entries[len(entries)-1].Index, e.Index)
		}
		entries = append(entries, e)
		s.offsets = append(s.offsets, off)
		off += n
	}
	s.size = off
	if len(entries) > 0 {
		s.firstIndex = entries[0].Index
	}
	return entries, nil
}

// decodeRecord parses the record at the start of b and returns its size. On
// error the size covers what the record claims to span, capped at len(b).
func decodeRecord(b []byte) (*raftpb.Entry, int64, error) {
	if len(b) < recordHeaderSize {
		return nil, int64(len(b)), errors.New("truncated header")
	}
	size := int64(binary.BigEndian.Uint32(b))
	total := recordHeaderSize + size
	if total > int64(len(b)) {
		return nil, int64(len(b)), errors.New("truncated payload")
	}
	payload := b[recordHeaderSize:total]
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(b[4:]) {
		return nil, total, errors.New("checksum mismatch")
	}
	e := &raftpb.Entry{}
	if err := proto.Unmarshal(payload, e); err != nil {
		return nil, total, err
	}
	return e, total, nil
}

func encodeRecord(buf []byte, e *raftpb.Entry) ([]byte, error) {
	payload, err := proto.Marshal(e)
	if err != nil {
		return nil, err
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(payload, crcTable))
	return append(buf, payload...), nil
}

func (s *storage) saveHardState(hs *raftpb.HardState) error {
	return writeProtoAtomic(filepath.Join(s.dir, "state"), hs)
}

// appendEntries writes entries to the end of the log and syncs the file.
func (s *storage) appendEntries(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if len(s.offsets) == 0 {
		s.firstIndex = entries[0].Index
	}
	var buf []byte
	offsets := make([]int64, len(entries))
	for i, e := range entries {
		offsets[i] = s.size + int64(len(buf))
		var err error
		if buf, err = encodeRecord(buf, e); err != nil {
			return err
		}
	}
	if _, err := s.log.WriteAt(buf, s.size); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.offsets = append(s.offsets, offsets...)
	s.size += int64(len(buf))
	return nil
}

// truncateFrom removes entry index and everything after it.
func (s *storage) truncateFrom(index uint64) error {
	if len(s.offsets) == 0 || index > s.firstIndex+uint64(len(s.offsets))-1 {
		return nil
	}
	i := max(index, s.firstIndex) - s.firstIndex
	if err := s.log.Truncate(s.offsets[i]); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.size = s.offsets[i]
	s.offsets = s.offsets[:i]
	return nil
}

func (s *storage) close() error { return s.log.Close() }

// readProto reads a protobuf file; a missing file leaves m empty.
func readProto(path string, m proto.Message) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, m)
}

// writeProtoAtomic replaces path so that after a crash it holds either the
// old or the new content, never a mix: write a temporary file, sync it,
// rename it over the old one and sync the directory that records the rename.
func writeProtoAtomic(path string, m proto.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
