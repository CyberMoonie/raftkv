package raft

import (
	"context"
)

// readRequest is a Read waiting first for leadership confirmation, then for
// the state machine to reach index.
type readRequest struct {
	ctx   context.Context
	index uint64 // commit index when the read arrived
	seq   uint64 // heartbeats with this sequence number or later confirm it
	query []byte
	ch    chan result
}

// Read answers a read-only query without adding it to the log, and still
// returns up-to-date data (ReadIndex, dissertation §6.4):
//
//  1. Remember the commit index: every write acknowledged so far is at or
//     before it.
//  2. Make sure we are still the leader. A newer leader could already be
//     accepting writes we know nothing about, so a majority must answer a
//     heartbeat sent after the read arrived.
//  3. Wait until the state machine has applied the remembered index, then
//     query it.
//
// This costs one heartbeat round instead of a disk write and a replication
// round per read.
func (n *Node) Read(ctx context.Context, query []byte) (any, error) {
	n.mu.Lock()
	if err := n.checkLeader(); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	// Until the no-op of our term commits, our commit index may lag behind
	// the previous leader's. Every entry it committed is in our log before
	// the no-op, though, so waiting for the no-op covers them.
	n.readSeq++
	r := &readRequest{
		ctx:   ctx,
		index: max(n.commitIndex, n.leaderStart),
		seq:   n.readSeq,
		query: query,
		ch:    make(chan result, 1),
	}
	n.pendingReads = append(n.pendingReads, r)
	n.confirmReads() // a single-node cluster needs nobody else
	n.triggerReplication()
	n.mu.Unlock()

	select {
	case res := <-r.ch:
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, ErrStopped
	}
}

// ackRead records that peer answered a heartbeat sent when the read
// sequence number was seq.
func (n *Node) ackRead(peer string, seq uint64) {
	if seq > n.readAcks[peer] {
		n.readAcks[peer] = seq
		n.confirmReads()
	}
}

// confirmReads moves reads whose heartbeat round a majority has answered to
// readyReads, where the applier picks them up. Abandoned reads are dropped.
func (n *Node) confirmReads() {
	waiting := n.pendingReads[:0]
	for _, r := range n.pendingReads {
		if r.ctx.Err() != nil {
			continue
		}
		acks := 0
		for _, m := range n.members {
			if m.Id == n.id || n.readAcks[m.Id] >= r.seq {
				acks++
			}
		}
		if n.isQuorum(acks) {
			n.readyReads = append(n.readyReads, r)
		} else {
			waiting = append(waiting, r)
		}
	}
	n.pendingReads = waiting
	if len(n.readyReads) > 0 {
		n.applyCond.Broadcast()
	}
}

// failPendingReads rejects reads that can no longer be confirmed because we
// stopped being the leader. Confirmed reads are still valid and stay queued.
func (n *Node) failPendingReads() {
	for _, r := range n.pendingReads {
		r.ch <- result{err: ErrLeadershipLost}
	}
	n.pendingReads = nil
}

// runnableReads removes and returns the confirmed reads whose index has been
// applied.
func (n *Node) runnableReads() []*readRequest {
	var run []*readRequest
	waiting := n.readyReads[:0]
	for _, r := range n.readyReads {
		if r.index <= n.lastApplied {
			run = append(run, r)
		} else {
			waiting = append(waiting, r)
		}
	}
	n.readyReads = waiting
	return run
}

func (n *Node) hasRunnableReads() bool {
	for _, r := range n.readyReads {
		if r.index <= n.lastApplied {
			return true
		}
	}
	return false
}
