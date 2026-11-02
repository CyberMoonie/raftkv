package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

var errUnreachable = errors.New("unreachable")

// network delivers RPCs between nodes in the same process and can cut nodes
// off to simulate crashes and partitions.
type network struct {
	mu    sync.Mutex
	nodes map[string]*Node // by address
	down  map[string]bool
}

type memTransport struct {
	net  *network
	from string
}

func (t memTransport) peer(addr string) (*Node, error) {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	n, ok := t.net.nodes[addr]
	if !ok || t.net.down[t.from] || t.net.down[addr] {
		return nil, errUnreachable
	}
	return n, nil
}

// call clones the request and reply so nodes never share messages, as if
// they had crossed a real network.
func call[Req, Resp proto.Message](t memTransport, addr string, req Req, handle func(*Node, Req) Resp) (Resp, error) {
	var zero Resp
	n, err := t.peer(addr)
	if err != nil {
		return zero, err
	}
	resp := handle(n, proto.Clone(req).(Req))
	// A reply is lost if the link went down while the call was running.
	if _, err := t.peer(addr); err != nil {
		return zero, err
	}
	return proto.Clone(resp).(Resp), nil
}

func (t memTransport) RequestVote(_ context.Context, addr string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return call(t, addr, req, (*Node).HandleRequestVote)
}

func (t memTransport) AppendEntries(_ context.Context, addr string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return call(t, addr, req, (*Node).HandleAppendEntries)
}

// testFSM records the commands applied to it.
type testFSM struct {
	mu      sync.Mutex
	applied []string
}

func (f *testFSM) Apply(_ uint64, data []byte) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(data))
	return len(f.applied)
}

func (f *testFSM) values() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.applied)
}

type cluster struct {
	t       *testing.T
	net     *network
	members []*raftpb.Member
	dirs    []string
	nodes   []*Node
	fsms    []*testFSM
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{
		t:     t,
		net:   &network{nodes: make(map[string]*Node), down: make(map[string]bool)},
		nodes: make([]*Node, size),
		fsms:  make([]*testFSM, size),
	}
	for i := range size {
		id := fmt.Sprintf("n%d", i+1)
		c.members = append(c.members, &raftpb.Member{Id: id, RaftAddr: id, ClientAddr: id + ":6379"})
		c.dirs = append(c.dirs, t.TempDir())
	}
	for i := range size {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) start(i int) {
	c.t.Helper()
	m := c.members[i]
	fsm := &testFSM{}
	n, err := New(Config{
		ID:                m.Id,
		DataDir:           c.dirs[i],
		Members:           c.members,
		Transport:         memTransport{net: c.net, from: m.RaftAddr},
		StateMachine:      fsm,
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		Logger:            slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.mu.Lock()
	c.net.nodes[m.RaftAddr] = n
	c.net.mu.Unlock()
	c.nodes[i], c.fsms[i] = n, fsm
	n.Start()
}

// restart stops node i and starts it again from its data directory with an
// empty state machine, like a process that crashed and came back.
func (c *cluster) restart(i int) {
	c.nodes[i].Stop()
	c.start(i)
}

func (c *cluster) setDown(i int, down bool) {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	c.net.down[c.members[i].RaftAddr] = down
}

func (c *cluster) disconnect(i int) { c.setDown(i, true) }
func (c *cluster) connect(i int)    { c.setDown(i, false) }

func (c *cluster) isDown(i int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.down[c.members[i].RaftAddr]
}

// leader waits until exactly one connected node is leader and returns its
// position.
func (c *cluster) leader() int {
	c.t.Helper()
	leader := -1
	eventually(c.t, 5*time.Second, "a single leader", func() bool {
		leader = -1
		for i, n := range c.nodes {
			if c.isDown(i) || n.Status().Role != Leader {
				continue
			}
			if leader != -1 {
				return false
			}
			leader = i
		}
		return leader != -1
	})
	return leader
}

// propose submits data through the current leader, retrying while
// leadership moves.
func (c *cluster) propose(data string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.nodes[c.leader()].Propose(ctx, []byte(data))
		cancel()
		if err == nil {
			return
		}
	}
	c.t.Fatalf("could not commit %q", data)
}

// waitApplied waits until the given nodes (all of them by default) have
// applied exactly want.
func (c *cluster) waitApplied(want []string, nodes ...int) {
	c.t.Helper()
	if nodes == nil {
		for i := range c.nodes {
			nodes = append(nodes, i)
		}
	}
	for _, i := range nodes {
		eventually(c.t, 5*time.Second, fmt.Sprintf("node %d to apply %d entries", i, len(want)), func() bool {
			return slices.Equal(c.fsms[i].values(), want)
		})
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func commands(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}
