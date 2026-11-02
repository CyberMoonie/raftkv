package raft

import (
	"slices"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// raftLog is the in-memory log. entries[0] is a sentinel that stands for the
// entry just before the first real one, so the consistency check for the
// first entry needs no special case. Entry i lives at entries[i-firstIndex].
type raftLog struct {
	entries []*raftpb.Entry
}

func newLog(index, term uint64) *raftLog {
	return &raftLog{entries: []*raftpb.Entry{{Index: index, Term: term}}}
}

// firstIndex is the sentinel's index: entries after it are in the log.
func (l *raftLog) firstIndex() uint64 { return l.entries[0].Index }

func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

func (l *raftLog) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// term returns the term of entry i; ok is false if i is outside the log.
func (l *raftLog) term(i uint64) (term uint64, ok bool) {
	if i < l.firstIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.firstIndex()].Term, true
}

func (l *raftLog) entry(i uint64) *raftpb.Entry {
	return l.entries[i-l.firstIndex()]
}

// slice returns a copy of entries [from, to).
func (l *raftLog) slice(from, to uint64) []*raftpb.Entry {
	return slices.Clone(l.entries[from-l.firstIndex() : to-l.firstIndex()])
}

func (l *raftLog) append(entries ...*raftpb.Entry) {
	l.entries = append(l.entries, entries...)
}

// compact discards every entry up to and including i, which becomes the new
// sentinel. Its data is dropped; only the index and term are still needed.
func (l *raftLog) compact(i uint64) {
	sentinel := &raftpb.Entry{Index: i, Term: l.entry(i).Term}
	l.entries = append([]*raftpb.Entry{sentinel}, l.entries[i-l.firstIndex()+1:]...)
}

// truncateFrom removes entry i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	l.entries = l.entries[:i-l.firstIndex()]
}

// lastIndexOfTerm returns the index of the last entry with the given term,
// or 0 if the log has none.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := len(l.entries) - 1; i > 0; i-- {
		if l.entries[i].Term == term {
			return l.entries[i].Index
		}
		if l.entries[i].Term < term {
			break
		}
	}
	return 0
}

// firstIndexOfTerm returns the first index of the run of entries that share
// entry i's term, staying after the sentinel.
func (l *raftLog) firstIndexOfTerm(i uint64) uint64 {
	term := l.entry(i).Term
	for i-1 > l.firstIndex() && l.entry(i-1).Term == term {
		i--
	}
	return i
}
