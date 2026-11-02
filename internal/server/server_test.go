package server

import (
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/CyberMoonie/raftkv/internal/kv"
	"github.com/CyberMoonie/raftkv/internal/raft"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
)

// testNode is a full node: Raft over real gRPC plus the Redis server.
type testNode struct {
	member *raftpb.Member
	node   *raft.Node
}

func startNode(t *testing.T, id string, bootstrap bool) *testNode {
	t.Helper()
	raftLn := listen(t)
	clientLn := listen(t)
	self := &raftpb.Member{Id: id, RaftAddr: raftLn.Addr().String(), ClientAddr: clientLn.Addr().String()}

	transport := raft.NewGRPCTransport()
	cfg := raft.Config{
		ID:                id,
		DataDir:           t.TempDir(),
		Transport:         transport,
		StateMachine:      kv.New(),
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		Logger:            slog.New(slog.DiscardHandler),
	}
	if bootstrap {
		cfg.Bootstrap = []*raftpb.Member{self}
	}
	node, err := raft.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := raft.NewGRPCServer(node)
	go grpcServer.Serve(raftLn)
	node.Start()
	srv := New(node, cfg.Logger)
	go srv.Serve(clientLn)

	t.Cleanup(func() {
		srv.Close()
		node.Stop()
		grpcServer.Stop()
		transport.Close()
	})
	return &testNode{member: self, node: node}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

type client struct {
	t *testing.T
	r *resp.Reader
	w *resp.Writer
}

func dial(t *testing.T, n *testNode) *client {
	t.Helper()
	conn, err := net.Dial("tcp", n.member.ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, r: resp.NewReader(conn), w: resp.NewWriter(conn)}
}

func (c *client) do(args ...string) resp.Value {
	c.t.Helper()
	c.w.WriteCommand(args...)
	if err := c.w.Flush(); err != nil {
		c.t.Fatal(err)
	}
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatal(err)
	}
	return v
}

// doUntil repeats a command until its reply satisfies ok, for steps that
// depend on an election or a membership change finishing.
func (c *client) doUntil(ok func(resp.Value) bool, args ...string) resp.Value {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		v := c.do(args...)
		if ok(v) {
			return v
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%v: last reply %+v", args, v)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func isOK(v resp.Value) bool { return v.Kind == resp.KindSimple && v.Str == "OK" }

func TestSingleNode(t *testing.T) {
	n := startNode(t, "n1", true)
	c := dial(t, n)

	if v := c.do("PING"); v.Str != "PONG" {
		t.Fatalf("PING: %+v", v)
	}
	c.doUntil(isOK, "SET", "greeting", "hello")
	if v := c.do("GET", "greeting"); string(v.Bulk) != "hello" {
		t.Fatalf("GET: %+v", v)
	}
	if v := c.do("INCR", "counter"); v.Int != 1 {
		t.Fatalf("INCR: %+v", v)
	}
	if v := c.do("NOPE"); v.Kind != resp.KindError {
		t.Fatalf("unknown command: %+v", v)
	}
}

func TestPipelinedCommands(t *testing.T) {
	n := startNode(t, "n1", true)
	c := dial(t, n)
	c.doUntil(isOK, "SET", "warmup", "1")

	for range 10 {
		c.w.WriteCommand("INCR", "n")
	}
	c.w.Flush()
	for i := range 10 {
		v, err := c.r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if v.Int != int64(i+1) {
			t.Fatalf("reply %d: %+v", i, v)
		}
	}
}

// Nodes join through the Redis port with RAFT ADD, the way raftkv -join
// does, and followers redirect clients to the leader.
func TestClusterJoinAndRedirect(t *testing.T) {
	leader := startNode(t, "n1", true)
	followers := []*testNode{startNode(t, "n2", false), startNode(t, "n3", false)}

	admin := dial(t, leader)
	for _, f := range followers {
		m := f.member
		admin.doUntil(isOK, "RAFT", "ADD", m.Id, m.RaftAddr, m.ClientAddr)
	}
	info := admin.do("RAFT", "INFO")
	if !strings.Contains(string(info.Bulk), "members:3") {
		t.Fatalf("RAFT INFO:\n%s", info.Bulk)
	}

	admin.doUntil(isOK, "SET", "k", "v")
	f := dial(t, followers[0])
	want := "MOVED 0 " + leader.member.ClientAddr
	f.doUntil(func(v resp.Value) bool { return v.Str == want }, "GET", "k")

	// After the leader is removed, one of the followers takes over with the
	// data.
	admin.do("RAFT", "REMOVE", "n1")
	g := dial(t, followers[1])
	deadline := time.Now().Add(5 * time.Second)
	for string(f.do("GET", "k").Bulk) != "v" && string(g.do("GET", "k").Bulk) != "v" {
		if time.Now().After(deadline) {
			t.Fatal("no follower took over after the leader was removed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
