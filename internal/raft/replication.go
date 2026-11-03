package raft

import (
	"time"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// maxBatch caps how many entries one AppendEntries carries.
const maxBatch = 256

// replicator is the leader's goroutine for one follower. It keeps a single
// RPC in flight, so replies never arrive out of order.
type replicator struct {
	member  *raftpb.Member
	trigger chan struct{} // new entries or a new commit index to send
	stop    chan struct{}
}

func (n *Node) startReplicator(m *raftpb.Member) {
	r := &replicator{member: m, trigger: make(chan struct{}, 1), stop: make(chan struct{})}
	n.replicators[m.Id] = r
	n.wg.Add(1)
	go n.runReplicator(r, n.term)
}

func (n *Node) stopReplicators() {
	for id, r := range n.replicators {
		close(r.stop)
		delete(n.replicators, id)
	}
}

// triggerReplication wakes every replicator without blocking; one pending
// wake-up is enough because a replicator always sends the latest state.
func (n *Node) triggerReplication() {
	for _, r := range n.replicators {
		select {
		case r.trigger <- struct{}{}:
		default:
		}
	}
}

func (n *Node) runReplicator(r *replicator, term uint64) {
	defer n.wg.Done()
	heartbeat := time.NewTicker(n.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		more := n.replicate(r.member, term)
		if more {
			// The follower is behind: keep sending without waiting.
			select {
			case <-r.stop:
				return
			case <-n.ctx.Done():
				return
			default:
				continue
			}
		}
		select {
		case <-r.stop:
			return
		case <-n.ctx.Done():
			return
		case <-r.trigger:
		case <-heartbeat.C:
		}
	}
}

// replicate sends one AppendEntries to m (a heartbeat if there is nothing
// new) and processes the reply. It reports whether m still needs more.
func (n *Node) replicate(m *raftpb.Member, term uint64) (more bool) {
	n.mu.Lock()
	if n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false
	}
	// Any answer to this request in our term shows m still follows us, which
	// confirms the reads registered up to now (read.go).
	readSeq := n.readSeq
	next := n.nextIndex[m.Id]
	prevIndex := next - 1
	if prevIndex < n.log.firstIndex() {
		// The entries m needs were compacted away; send the snapshot.
		snap := n.snapshot
		n.mu.Unlock()
		return n.sendSnapshot(m, term, snap, readSeq)
	}
	prevTerm, _ := n.log.term(prevIndex)
	last := min(n.log.lastIndex(), prevIndex+maxBatch)
	req := &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, last+1),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := n.rpcContext()
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(ctx, m.RaftAddr, req)
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term)
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.ackRead(m.Id, readSeq)

	if resp.Success {
		match := prevIndex + uint64(len(req.Entries))
		if match > n.matchIndex[m.Id] {
			n.matchIndex[m.Id] = match
			n.advanceCommit()
		}
		n.nextIndex[m.Id] = max(n.nextIndex[m.Id], match+1)
		return n.nextIndex[m.Id] <= n.log.lastIndex()
	}

	// The follower's log does not contain prevIndex with prevTerm. Jump back
	// past the whole conflicting term instead of one entry at a time.
	next = resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		if i := n.log.lastIndexOfTerm(resp.ConflictTerm); i != 0 {
			next = i + 1
		}
	}
	if next == 0 {
		return false
	}
	n.nextIndex[m.Id] = min(next, n.log.lastIndex()+1)
	return true
}

func (n *Node) sendSnapshot(m *raftpb.Member, term uint64, snap *raftpb.Snapshot, readSeq uint64) (more bool) {
	req := &raftpb.InstallSnapshotRequest{Term: term, LeaderId: n.id, Snapshot: snap}
	ctx, cancel := n.rpcContext()
	defer cancel()
	resp, err := n.cfg.Transport.InstallSnapshot(ctx, m.RaftAddr, req)
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term)
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.ackRead(m.Id, readSeq)
	if snap.LastIndex > n.matchIndex[m.Id] {
		n.matchIndex[m.Id] = snap.LastIndex
		n.advanceCommit()
	}
	n.nextIndex[m.Id] = max(n.nextIndex[m.Id], snap.LastIndex+1)
	return n.nextIndex[m.Id] <= n.log.lastIndex()
}

// advanceCommit moves commitIndex to the highest entry stored on a majority.
// Only entries from the current term are counted directly; earlier ones are
// committed along with them (§5.4.2, Figure 8).
func (n *Node) advanceCommit() {
	for i := n.log.lastIndex(); i > n.commitIndex; i-- {
		if term, _ := n.log.term(i); term != n.term {
			break
		}
		count := 0
		for _, m := range n.members {
			if m.Id == n.id || n.matchIndex[m.Id] >= i {
				count++
			}
		}
		if n.isQuorum(count) {
			n.commitIndex = i
			n.applyCond.Broadcast()
			// Tell followers about the new commit index right away.
			n.triggerReplication()
			n.stepDownIfRemoved()
			return
		}
	}
}

// HandleAppendEntries accepts entries and heartbeats from the leader
// (Figure 2, "AppendEntries RPC").
func (n *Node) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.AppendEntriesResponse{Term: n.term}
	if req.Term < n.term || n.stopped {
		return resp
	}
	// A valid leader exists for this term, so candidates step down too.
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term)
		resp.Term = n.term
	}
	n.leaderID = req.LeaderId
	n.lastContact = time.Now()
	n.resetElectionTimer()

	prevIndex, prevTerm, entries := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	if first := n.log.firstIndex(); prevIndex < first {
		// Our snapshot already covers the start of the request. Those
		// entries are committed, so they match the leader's; skip them.
		skip := min(first-prevIndex, uint64(len(entries)))
		entries = entries[skip:]
		prevIndex, prevTerm = first, n.log.entries[0].Term
	}

	// Consistency check: our log must contain prevIndex with prevTerm.
	if prevIndex > n.log.lastIndex() {
		resp.ConflictIndex = n.log.lastIndex() + 1
		return resp
	}
	if term, _ := n.log.term(prevIndex); term != prevTerm {
		resp.ConflictTerm = term
		resp.ConflictIndex = n.log.firstIndexOfTerm(prevIndex)
		return resp
	}

	// Append what is new. An existing entry is only removed when it
	// conflicts, never just because the request ends earlier: an old,
	// delayed request must not undo entries a newer one appended. Everything
	// is on disk before we reply.
	for i, e := range entries {
		if e.Index <= n.log.lastIndex() {
			if term, _ := n.log.term(e.Index); term == e.Term {
				continue
			}
			n.log.truncateFrom(e.Index)
			n.mustPersist(n.storage.truncateFrom(e.Index))
		}
		n.log.append(entries[i:]...)
		n.mustPersist(n.storage.appendEntries(entries[i:]))
		// The new suffix may add or drop configuration entries.
		n.refreshConfig()
		break
	}

	lastNew := prevIndex + uint64(len(entries))
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = max(n.commitIndex, min(req.LeaderCommit, lastNew))
		n.applyCond.Broadcast()
	}
	resp.Success = true
	return resp
}

// HandleInstallSnapshot replaces the follower's state with the leader's
// snapshot when the follower is too far behind for log entries (§7).
func (n *Node) HandleInstallSnapshot(req *raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.InstallSnapshotResponse{Term: n.term}
	if req.Term < n.term || n.stopped {
		return resp
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term)
		resp.Term = n.term
	}
	n.leaderID = req.LeaderId
	n.lastContact = time.Now()
	n.resetElectionTimer()

	snap := req.Snapshot
	if snap.LastIndex <= n.commitIndex {
		return resp // we already have everything it covers
	}

	// Keep entries after the snapshot if our log agrees with it up to that
	// point; otherwise the whole log is stale.
	if term, ok := n.log.term(snap.LastIndex); ok && term == snap.LastTerm {
		n.log.compact(snap.LastIndex)
	} else {
		n.log = newLog(snap.LastIndex, snap.LastTerm)
	}
	n.mustPersist(n.storage.saveSnapshot(snap))
	n.mustPersist(n.storage.rewriteLog(n.log.slice(snap.LastIndex+1, n.log.lastIndex()+1)))
	n.snapshot = snap
	n.refreshConfig()
	n.commitIndex = snap.LastIndex
	n.pendingSnapshot = snap
	n.applyCond.Broadcast()
	n.logger.Info("installed snapshot from leader", "index", snap.LastIndex)
	return resp
}
