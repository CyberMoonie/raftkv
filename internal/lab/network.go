package lab

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/raft"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

var errLost = errors.New("lab: message lost")

// network connects the lab's nodes in memory. Every message waits the link
// latency, may be dropped, and only reaches nodes that are up and in the
// same partition group. Each message is reported to the dashboard.
type network struct {
	publish func(event string, data any)
	seq     atomic.Uint64

	mu      sync.Mutex
	nodes   map[string]*raft.Node // running nodes by ID
	groups  map[string]int        // partition group; equal groups can talk
	latency time.Duration
	jitter  time.Duration
	drop    float64 // probability of losing each message
	rng     *rand.Rand
}

func newNetwork(publish func(string, any), latency, jitter time.Duration) *network {
	return &network{
		publish: publish,
		nodes:   make(map[string]*raft.Node),
		groups:  make(map[string]int),
		latency: latency,
		jitter:  jitter,
		rng:     rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
}

func (nw *network) attach(id string, n *raft.Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = n
}

func (nw *network) detach(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.nodes, id)
}

func (nw *network) node(id string) *raft.Node {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.nodes[id]
}

func (nw *network) setConditions(latency time.Duration, drop float64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.latency, nw.jitter, nw.drop = latency, latency/4, drop
}

func (nw *network) conditions() (latency time.Duration, drop float64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.latency, nw.drop
}

func (nw *network) setGroup(id string, group int) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.groups[id] = group
}

func (nw *network) group(id string) int {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.groups[id]
}

func (nw *network) heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	clear(nw.groups)
}

func (nw *network) reachableLocked(from, to string) bool {
	_, fromUp := nw.nodes[from]
	_, toUp := nw.nodes[to]
	return fromUp && toUp && nw.groups[from] == nw.groups[to]
}

// message is what the dashboard draws as a packet moving between nodes.
type message struct {
	ID    uint64 `json:"id"`
	Kind  string `json:"kind"` // prevote, vote, heartbeat, append, snapshot
	From  string `json:"from"`
	To    string `json:"to"`
	Label string `json:"label"`
	Ms    int64  `json:"ms"`    // travel time
	Fate  string `json:"fate"`  // ok, drop (lost) or blocked (partition or crash)
	Reply bool   `json:"reply"` // false for the request, true for the answer
	OK    bool   `json:"ok"`    // for replies: granted / accepted
}

// travel carries one message from one node to another.
func (nw *network) travel(ctx context.Context, m message) error {
	nw.mu.Lock()
	d := nw.latency
	if nw.jitter > 0 {
		d += time.Duration(nw.rng.Int64N(int64(2*nw.jitter))) - nw.jitter
	}
	d = max(d, time.Millisecond)
	switch {
	case !nw.reachableLocked(m.From, m.To):
		m.Fate = "blocked"
	case nw.rng.Float64() < nw.drop:
		m.Fate = "drop"
	default:
		m.Fate = "ok"
	}
	nw.mu.Unlock()

	m.Ms = d.Milliseconds()
	nw.publish("msg", m)
	if m.Fate != "ok" {
		d /= 2 // the dashboard shows it dying halfway
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return ctx.Err()
	}
	if m.Fate != "ok" {
		return errLost
	}
	nw.mu.Lock()
	defer nw.mu.Unlock()
	if !nw.reachableLocked(m.From, m.To) {
		return errLost // cut while in flight
	}
	return nil
}

// call sends a request, lets the receiving node handle it and carries the
// answer back. Messages are cloned so nodes never share memory.
func call[Req, Resp proto.Message](nw *network, ctx context.Context, from, to string, req Req,
	describe func(Req) (kind, label string), handle func(*raft.Node, Req) Resp, answer func(Resp) (string, bool)) (Resp, error) {
	var zero Resp
	kind, label := describe(req)
	id := nw.seq.Add(1)
	if err := nw.travel(ctx, message{ID: id, Kind: kind, From: from, To: to, Label: label}); err != nil {
		return zero, err
	}
	n := nw.node(to)
	if n == nil {
		return zero, errLost
	}
	resp := handle(n, proto.Clone(req).(Req))
	label, ok := answer(resp)
	if err := nw.travel(ctx, message{ID: id, Kind: kind, From: to, To: from, Label: label, Reply: true, OK: ok}); err != nil {
		return zero, err
	}
	return proto.Clone(resp).(Resp), nil
}

// transport is one node's view of the network.
type transport struct {
	nw   *network
	from string
}

func (t transport) RequestVote(ctx context.Context, to string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return call(t.nw, ctx, t.from, to, req,
		func(r *raftpb.RequestVoteRequest) (string, string) {
			if r.PreVote {
				return "prevote", fmt.Sprintf("pre-vote? t%d", r.Term)
			}
			return "vote", fmt.Sprintf("vote? t%d", r.Term)
		},
		(*raft.Node).HandleRequestVote,
		func(r *raftpb.RequestVoteResponse) (string, bool) {
			if r.VoteGranted {
				return "yes", true
			}
			return "no", false
		})
}

func (t transport) AppendEntries(ctx context.Context, to string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return call(t.nw, ctx, t.from, to, req,
		func(r *raftpb.AppendEntriesRequest) (string, string) {
			if len(r.Entries) == 0 {
				return "heartbeat", "♥"
			}
			first, last := r.Entries[0].Index, r.Entries[len(r.Entries)-1].Index
			if first == last {
				return "append", fmt.Sprintf("#%d", first)
			}
			return "append", fmt.Sprintf("#%d-%d", first, last)
		},
		(*raft.Node).HandleAppendEntries,
		func(r *raftpb.AppendEntriesResponse) (string, bool) {
			if r.Success {
				return "ok", true
			}
			return "behind", false
		})
}

func (t transport) InstallSnapshot(ctx context.Context, to string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return call(t.nw, ctx, t.from, to, req,
		func(r *raftpb.InstallSnapshotRequest) (string, string) {
			return "snapshot", fmt.Sprintf("snap ≤#%d", r.Snapshot.LastIndex)
		},
		(*raft.Node).HandleInstallSnapshot,
		func(*raftpb.InstallSnapshotResponse) (string, bool) { return "ok", true })
}
