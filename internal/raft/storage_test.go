package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

func entriesFrom(first, last, term uint64) []*raftpb.Entry {
	var out []*raftpb.Entry
	for i := first; i <= last; i++ {
		out = append(out, &raftpb.Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func reopen(t *testing.T, s *storage) (*storage, *savedState) {
	t.Helper()
	s.close()
	s, saved, err := openStorage(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	return s, saved
}

func newStorage(t *testing.T) *storage {
	t.Helper()
	s, _, err := openStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func wantIndexes(t *testing.T, entries []*raftpb.Entry, first, last uint64) {
	t.Helper()
	if uint64(len(entries)) != last-first+1 {
		t.Fatalf("got %d entries, want %d..%d", len(entries), first, last)
	}
	for i, e := range entries {
		if e.Index != first+uint64(i) {
			t.Fatalf("entry %d has index %d, want %d", i, e.Index, first+uint64(i))
		}
	}
}

func TestStorageKeepsHardStateAndLog(t *testing.T) {
	s := newStorage(t)
	if err := s.saveHardState(&raftpb.HardState{Term: 7, VotedFor: "n2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEntries(entriesFrom(1, 5, 7)); err != nil {
		t.Fatal(err)
	}

	_, saved := reopen(t, s)
	if hs := saved.hardState; hs.Term != 7 || hs.VotedFor != "n2" {
		t.Fatalf("hard state = %v", hs)
	}
	wantIndexes(t, saved.entries, 1, 5)
}

func TestStorageTruncatesConflictingSuffix(t *testing.T) {
	s := newStorage(t)
	s.appendEntries(entriesFrom(1, 5, 1))
	if err := s.truncateFrom(3); err != nil {
		t.Fatal(err)
	}
	s.appendEntries(entriesFrom(3, 4, 2))

	_, saved := reopen(t, s)
	wantIndexes(t, saved.entries, 1, 4)
	if saved.entries[1].Term != 1 || saved.entries[2].Term != 2 {
		t.Fatalf("terms = %d, %d, want 1, 2", saved.entries[1].Term, saved.entries[2].Term)
	}
}

// A crash during an append leaves half a record at the end of the file. The
// complete records before it must survive and the garbage must go.
func TestStorageRecoversFromTornWrite(t *testing.T) {
	s := newStorage(t)
	s.appendEntries(entriesFrom(1, 3, 1))
	f, err := os.OpenFile(filepath.Join(s.dir, "log"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 50, 1, 2, 3}) // header claims 50 bytes that never arrived
	f.Close()

	s, saved := reopen(t, s)
	wantIndexes(t, saved.entries, 1, 3)
	// Appending after recovery continues cleanly.
	if err := s.appendEntries(entriesFrom(4, 4, 1)); err != nil {
		t.Fatal(err)
	}
	_, saved = reopen(t, s)
	wantIndexes(t, saved.entries, 1, 4)
}

func TestStorageRejectsCorruptionInTheMiddle(t *testing.T) {
	s := newStorage(t)
	s.appendEntries(entriesFrom(1, 3, 1))
	s.close()

	path := filepath.Join(s.dir, "log")
	data, _ := os.ReadFile(path)
	data[recordHeaderSize] ^= 0xff // first record's payload
	os.WriteFile(path, data, 0o644)

	if _, _, err := openStorage(s.dir); err == nil {
		t.Fatal("opened a log with a corrupt record before valid ones")
	}
}

func TestStorageRewritesLogAfterSnapshot(t *testing.T) {
	s := newStorage(t)
	s.appendEntries(entriesFrom(1, 10, 1))
	snap := &raftpb.Snapshot{LastIndex: 7, LastTerm: 1, Data: []byte("state")}
	if err := s.saveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := s.rewriteLog(entriesFrom(8, 10, 1)); err != nil {
		t.Fatal(err)
	}
	s.appendEntries(entriesFrom(11, 12, 2))
	if err := s.truncateFrom(12); err != nil {
		t.Fatal(err)
	}

	_, saved := reopen(t, s)
	if saved.snapshot.LastIndex != 7 || string(saved.snapshot.Data) != "state" {
		t.Fatalf("snapshot = %v", saved.snapshot)
	}
	wantIndexes(t, saved.entries, 8, 11)
}

func TestEntriesAfterSnapshot(t *testing.T) {
	snap := &raftpb.Snapshot{LastIndex: 5, LastTerm: 2}
	tests := []struct {
		name        string
		entries     []*raftpb.Entry
		first, last uint64 // expected kept range, 0..0 means none
		changed     bool
	}{
		{"continues the snapshot", entriesFrom(6, 8, 2), 6, 8, false},
		{"crash before the log was rewritten", entriesFrom(1, 8, 2), 6, 8, true},
		{"all covered by the snapshot", entriesFrom(1, 5, 2), 0, 0, true},
		{"log disagrees with the snapshot", entriesFrom(1, 8, 1), 0, 0, true},
		{"gap after the snapshot", entriesFrom(9, 10, 2), 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, changed := entriesAfter(snap, tt.entries)
			if changed != tt.changed {
				t.Fatalf("changed = %v, want %v", changed, tt.changed)
			}
			if tt.last == 0 {
				if len(kept) != 0 {
					t.Fatalf("kept %d entries, want none", len(kept))
				}
				return
			}
			wantIndexes(t, kept, tt.first, tt.last)
		})
	}
}
