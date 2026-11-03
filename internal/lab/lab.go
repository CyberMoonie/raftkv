// Package lab runs a raftkv cluster inside one process, on a simulated
// network, and serves a dashboard to watch and break it: the Raft Lab.
//
// The nodes are the real raft.Node and kv.Store used by raftkv; only the
// network between them is simulated, so that messages can be slowed down,
// dropped, partitioned and drawn.
package lab

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/kv"
	"github.com/CyberMoonie/raftkv/internal/kv/kvpb"
	"github.com/CyberMoonie/raftkv/internal/raft"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
	"github.com/CyberMoonie/raftkv/internal/server"
)

// Speed scales every timer and the network, from slow motion for watching
// each message to roughly real-world timing.
type Speed struct {
	Heartbeat time.Duration
	Election  time.Duration
	Latency   time.Duration
}

var speeds = map[string]Speed{
	"slow":   {Heartbeat: 900 * time.Millisecond, Election: 3 * time.Second, Latency: 350 * time.Millisecond},
	"normal": {Heartbeat: 250 * time.Millisecond, Election: 1200 * time.Millisecond, Latency: 90 * time.Millisecond},
	"fast":   {Heartbeat: 50 * time.Millisecond, Election: 300 * time.Millisecond, Latency: 10 * time.Millisecond},
}

const (
	clusterSize       = 5
	snapshotThreshold = 30
	logTail           = 40
)

type Options struct {
	// RedisBind is the host the nodes' Redis ports listen on, and
	// RedisPort the first port: node n1 gets RedisPort, n2 RedisPort+1...
	// A zero port disables them; the dashboard console still works.
	RedisBind string
	RedisPort int
	Logger    *slog.Logger
}

type Lab struct {
	opts Options
	hub  *hub

	mu      sync.Mutex
	speed   string
	preVote bool
	dir     string
	net     *network
	nodes   []*labNode
	nextID  int    // number of the next node: n1, n2, ...
	commit  uint64 // highest commit index announced in the feed
	stop    chan struct{}
}

type labNode struct {
	member    *raftpb.Member
	dir       string
	bootstrap []*raftpb.Member
	up        bool
	node      *raft.Node
	store     *store
	srv       *server.Server
}

func (n *labNode) id() string { return n.member.Id }

func New(opts Options) (*Lab, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	l := &Lab{opts: opts, hub: newHub(), speed: "slow", preVote: true}
	if err := l.reset(); err != nil {
		return nil, err
	}
	return l, nil
}

// Close stops every node and deletes the lab's data.
func (l *Lab) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.teardown()
}

// reset replaces the cluster with a fresh one. Called with l.mu held, or
// before the lab is shared.
func (l *Lab) reset() error {
	l.teardown()
	dir, err := os.MkdirTemp("", "raftkv-lab-")
	if err != nil {
		return err
	}
	sp := speeds[l.speed]
	l.dir = dir
	l.net = newNetwork(l.hub.publish, sp.Latency, sp.Latency/4)
	l.commit = 0
	l.nodes = nil
	l.nextID = 1

	var members []*raftpb.Member
	for range clusterSize {
		members = append(members, l.newMember())
	}
	for _, m := range members {
		n := &labNode{member: m, dir: filepath.Join(dir, m.Id), bootstrap: members}
		l.nodes = append(l.nodes, n)
		if err := l.startNode(n); err != nil {
			return err
		}
	}
	l.stop = make(chan struct{})
	go l.watchCommits(l.stop, l.net)
	l.hub.publish("reset", map[string]any{"speed": l.speed, "preVote": l.preVote})
	l.hub.feedf("system", "", "New cluster of %d nodes, %s speed, Pre-Vote %s. Every node starts as a follower with an election timer; nobody is leader yet.",
		clusterSize, l.speed, onOff(l.preVote))
	return nil
}

func (l *Lab) teardown() {
	if l.stop != nil {
		close(l.stop)
		l.stop = nil
	}
	var wg sync.WaitGroup
	for _, n := range l.nodes {
		if n.up {
			wg.Add(1)
			go func() {
				defer wg.Done()
				l.stopNode(n)
			}()
		}
	}
	wg.Wait()
	if l.dir != "" {
		os.RemoveAll(l.dir)
		l.dir = ""
	}
}

func (l *Lab) newMember() *raftpb.Member {
	i := l.nextID
	l.nextID++
	id := fmt.Sprintf("n%d", i)
	// Without Redis ports, MOVED names the node itself; the console
	// resolves it.
	m := &raftpb.Member{Id: id, RaftAddr: id, ClientAddr: id}
	if l.opts.RedisPort > 0 {
		m.ClientAddr = fmt.Sprintf("127.0.0.1:%d", l.opts.RedisPort+i-1)
	}
	return m
}

func (l *Lab) startNode(n *labNode) error {
	sp := speeds[l.speed]
	n.store = &store{kv: kv.New()}
	node, err := raft.New(raft.Config{
		ID:                n.id(),
		DataDir:           n.dir,
		Bootstrap:         n.bootstrap,
		Transport:         transport{nw: l.net, from: n.id()},
		StateMachine:      n.store,
		HeartbeatInterval: sp.Heartbeat,
		ElectionTimeout:   sp.Election,
		SnapshotThreshold: snapshotThreshold,
		DisablePreVote:    !l.preVote,
		Logger:            slog.New(feedHandler{hub: l.hub}),
	})
	if err != nil {
		return err
	}
	n.node, n.up = node, true
	l.net.attach(n.id(), node)
	node.Start()

	n.srv = server.New(node, l.opts.Logger)
	if l.opts.RedisPort > 0 {
		_, port, _ := net.SplitHostPort(n.member.ClientAddr)
		ln, err := net.Listen("tcp", net.JoinHostPort(l.opts.RedisBind, port))
		if err != nil {
			l.opts.Logger.Warn("redis port unavailable, console only", "node", n.id(), "err", err)
		} else {
			go n.srv.Serve(ln)
		}
	}
	return nil
}

func (l *Lab) stopNode(n *labNode) {
	n.up = false
	l.net.detach(n.id())
	n.srv.Close()
	n.node.Stop()
}

// watchCommits announces in the feed every time the leader's commit index
// moves forward.
func (l *Lab) watchCommits(stop chan struct{}, nw *network) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		l.mu.Lock()
		if l.net != nw {
			l.mu.Unlock()
			return
		}
		leader := l.leader()
		if leader == nil {
			l.mu.Unlock()
			continue
		}
		st := leader.node.Status()
		from, to := l.commit+1, st.CommitIndex
		if to >= from {
			l.commit = to
		}
		l.mu.Unlock()
		if to < from {
			continue
		}

		count := to - from + 1
		var what []string
		for _, e := range leader.node.LogTail(int(count)) {
			if e.Index >= from && e.Index <= to {
				what = append(what, fmt.Sprintf("#%d %s", e.Index, describe(e)))
			}
		}
		// Entries may already sit in a snapshot; then only the range is known.
		if len(what) != int(count) || len(what) > 4 {
			what = []string{fmt.Sprintf("entries #%d–#%d", from, to)}
		}
		it, them := "it", "it"
		if count > 1 {
			it, them = "they", "them"
		}
		l.hub.feedf("commit", leader.id(), "✔ COMMITTED %s. A majority (%d of %d nodes) stores %s, so %s can never be lost; every node now applies %s to its data.",
			strings.Join(what, ", "), len(st.Members)/2+1, len(st.Members), them, it, them)
	}
}

// leader returns a running node that believes it leads, preferring the
// highest term. Called with l.mu held.
func (l *Lab) leader() *labNode {
	var best *labNode
	var bestTerm uint64
	for _, n := range l.nodes {
		if !n.up {
			continue
		}
		if st := n.node.Status(); st.Role == raft.Leader && st.Term >= bestTerm {
			best, bestTerm = n, st.Term
		}
	}
	return best
}

func (l *Lab) find(id string) *labNode {
	for _, n := range l.nodes {
		if n.id() == id {
			return n
		}
	}
	return nil
}

// describe renders a log entry for people.
func describe(e *raftpb.Entry) string {
	switch e.Type {
	case raftpb.EntryType_ENTRY_TYPE_NOOP:
		return "no-op"
	case raftpb.EntryType_ENTRY_TYPE_CONFIG:
		var cfg raftpb.Configuration
		proto.Unmarshal(e.Data, &cfg)
		ids := make([]string, len(cfg.Members))
		for i, m := range cfg.Members {
			ids[i] = m.Id
		}
		return "config [" + strings.Join(ids, " ") + "]"
	}
	var cmd kvpb.Command
	if err := proto.Unmarshal(e.Data, &cmd); err != nil {
		return "?"
	}
	args := make([]string, len(cmd.Args))
	for i, a := range cmd.Args {
		args[i] = quote(string(a))
	}
	return strings.Join(args, " ")
}

func quote(s string) string {
	if s == "" || strings.ContainsAny(s, " \t\"") {
		return fmt.Sprintf("%q", s)
	}
	return s
}

func onOff(b bool) string {
	if b {
		return "ON"
	}
	return "OFF"
}

// store wraps the key-value store with a lock so the dashboard can read a
// node's data while Raft applies entries to it.
type store struct {
	mu sync.Mutex
	kv *kv.Store
}

func (s *store) Apply(index uint64, data []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv.Apply(index, data)
}

func (s *store) Query(query []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv.Query(query)
}

func (s *store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv.Snapshot()
}

func (s *store) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv.Restore(data)
}

func (s *store) items() []*kvpb.Item {
	data, err := s.Snapshot()
	if err != nil {
		return nil
	}
	var snap kvpb.Snapshot
	proto.Unmarshal(data, &snap)
	return snap.Items
}

// Command runs one redis-cli style command line against a node and returns
// the transcript. With follow it follows MOVED redirects like redis-cli -c.
func (l *Lab) Command(id, line string, follow bool) []string {
	args, err := splitArgs(line)
	if err != nil {
		return []string{"(error) " + err.Error()}
	}
	if len(args) == 0 {
		return nil
	}
	var out []string
	for hop := 0; hop < 3; hop++ {
		l.mu.Lock()
		n := l.find(id)
		var srv *server.Server
		var up bool
		if n != nil {
			srv, up = n.srv, n.up
		}
		l.mu.Unlock()
		if n == nil || !up {
			l.hub.feedf("client", id, "client → %s: %s, but %s is down, so the connection is refused.", id, line, id)
			return append(out, fmt.Sprintf("Could not connect to %s: connection refused", id))
		}

		l.hub.feedf("client", id, "client → %s: %s", id, line)
		bargs := make([][]byte, len(args))
		for i, a := range args {
			bargs[i] = []byte(a)
		}
		reply := srv.Execute(bargs)
		if follow && reply.Kind == resp.KindError && strings.HasPrefix(reply.Str, "MOVED ") {
			to := l.nodeByAddr(strings.Fields(reply.Str)[2])
			l.hub.feedf("client", id, "%s is a follower and cannot serve clients, so it answers MOVED: the leader is %s. The client retries there.", id, to)
			out = append(out, fmt.Sprintf("-> Redirected to %s", to))
			id = to
			continue
		}
		if reply.Kind != resp.KindError && kv.IsReadOnly(bargs) {
			l.hub.feedf("read", id, "%s answered %s from memory with ReadIndex: one heartbeat round confirmed it is still the leader, and nothing was written to the log.", id, strings.ToUpper(args[0]))
		}
		lines := formatReply(reply, "")
		summary := lines[0]
		if len(lines) > 1 {
			summary = fmt.Sprintf("%d lines", len(lines))
		}
		l.hub.feedf("client", id, "%s → client: %s", id, summary)
		return append(out, lines...)
	}
	return append(out, "(error) too many redirects")
}

func (l *Lab) nodeByAddr(addr string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, n := range l.nodes {
		if n.member.ClientAddr == addr || n.id() == addr {
			return n.id()
		}
	}
	return addr
}

// splitArgs splits a command line like a shell would: on spaces, with
// "double quotes" around arguments that contain spaces.
func splitArgs(line string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inQuotes, started := false, false
	for _, r := range line {
		switch {
		case r == '"':
			inQuotes, started = !inQuotes, true
		case (r == ' ' || r == '\t') && !inQuotes:
			if started {
				args = append(args, cur.String())
				cur.Reset()
				started = false
			}
		default:
			cur.WriteRune(r)
			started = true
		}
	}
	if inQuotes {
		return nil, errors.New("unbalanced quotes")
	}
	if started {
		args = append(args, cur.String())
	}
	return args, nil
}

// formatReply prints a reply the way redis-cli does.
func formatReply(v resp.Value, indent string) []string {
	switch v.Kind {
	case resp.KindSimple:
		return []string{v.Str}
	case resp.KindError:
		return []string{"(error) " + v.Str}
	case resp.KindInteger:
		return []string{fmt.Sprintf("(integer) %d", v.Int)}
	case resp.KindBulk:
		if v.Null {
			return []string{"(nil)"}
		}
		return []string{fmt.Sprintf("%q", v.Bulk)}
	}
	if len(v.Elems) == 0 {
		return []string{"(empty array)"}
	}
	var out []string
	for i, e := range v.Elems {
		for j, line := range formatReply(e, indent+"   ") {
			if j == 0 {
				out = append(out, fmt.Sprintf("%s%d) %s", indent, i+1, line))
			} else {
				out = append(out, line)
			}
		}
	}
	return out
}

// Action is an experiment requested by the dashboard.
type Action struct {
	Type    string         `json:"type"`
	Node    string         `json:"node"`
	Speed   string         `json:"speed"`
	PreVote bool           `json:"preVote"`
	Latency int            `json:"latency"` // ms
	Drop    int            `json:"drop"`    // percent
	Count   int            `json:"count"`
	Groups  map[string]int `json:"groups"`
}

// Do performs an action and returns an error a person can read.
func (l *Lab) Do(a Action) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.find(a.Node)
	needNode := func() error {
		if n == nil {
			return fmt.Errorf("unknown node %q", a.Node)
		}
		return nil
	}

	switch a.Type {
	case "reset":
		if _, ok := speeds[a.Speed]; ok {
			l.speed = a.Speed
		}
		l.preVote = a.PreVote
		return l.reset()

	case "crash":
		if err := needNode(); err != nil || !n.up {
			return err
		}
		l.stopNode(n)
		l.hub.feedf("chaos", n.id(), "✖ %s CRASHED. Its memory is gone, but its term, vote and log are safe on disk. The others stop hearing from it.", n.id())

	case "restart":
		if err := needNode(); err != nil || n.up {
			return err
		}
		if err := l.startNode(n); err != nil {
			return err
		}
		st := n.node.Status()
		snap := ""
		if st.SnapshotIndex > 0 {
			snap = fmt.Sprintf(", %d of them inside a snapshot", st.SnapshotIndex)
		}
		l.hub.feedf("chaos", n.id(), "↻ %s RESTARTED. From disk it recovered term %d and a log up to #%d%s. It rebuilds its data and catches up with the leader.",
			n.id(), st.Term, st.LastIndex, snap)

	case "isolate":
		if err := needNode(); err != nil {
			return err
		}
		if l.net.group(n.id()) == 0 {
			l.net.setGroup(n.id(), 100+slices.Index(l.nodes, n))
			l.hub.feedf("network", n.id(), "⚡ %s is ISOLATED: no message gets in or out. It is still running and thinks it is fine.", n.id())
		} else {
			l.net.setGroup(n.id(), 0)
			l.hub.feedf("network", n.id(), "%s is RECONNECTED to the network.", n.id())
		}

	case "partition":
		l.net.heal()
		var a1, b []string
		for _, n := range l.nodes {
			g := a.Groups[n.id()]
			l.net.setGroup(n.id(), g)
			if g == 0 {
				a1 = append(a1, n.id())
			} else {
				b = append(b, n.id())
			}
		}
		l.hub.feedf("network", "", "✂ NETWORK SPLIT: [%s] | [%s]. Each side can only talk to itself, and only a side with a majority can elect a leader or commit.",
			strings.Join(a1, " "), strings.Join(b, " "))

	case "heal":
		l.net.heal()
		l.hub.feedf("network", "", "Network HEALED: every running node can reach every other again.")

	case "network":
		latency := time.Duration(min(max(a.Latency, 0), 2000)) * time.Millisecond
		drop := float64(min(max(a.Drop, 0), 90)) / 100
		l.net.setConditions(latency, drop)
		l.hub.feedf("network", "", "Network now: %v latency per message, %d%% of messages lost.", latency, int(drop*100))

	case "snapshot":
		if err := needNode(); err != nil || !n.up {
			return err
		}
		n.node.RequestSnapshot()

	case "burst":
		leader := l.leader()
		if leader == nil {
			return errors.New("no leader right now; wait for an election")
		}
		count := min(max(a.Count, 1), 100)
		l.hub.feedf("client", leader.id(), "A burst of %d SET commands hits leader %s at once. It packs them into a few AppendEntries messages.", count, leader.id())
		for i := range count {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				data, _ := kv.Encode([][]byte{[]byte("SET"), fmt.Appendf(nil, "burst:%02d", i), fmt.Appendf(nil, "%d", time.Now().UnixMilli())}, time.Now().UnixMilli())
				leader.node.Propose(ctx, data)
			}()
		}

	case "add":
		leader := l.leader()
		if leader == nil {
			return errors.New("no leader right now; wait for an election")
		}
		m := l.newMember()
		n := &labNode{member: m, dir: filepath.Join(l.dir, m.Id)}
		l.nodes = append(l.nodes, n)
		if err := l.startNode(n); err != nil {
			return err
		}
		l.hub.feedf("membership", m.Id, "+ %s starts with an empty log and no configuration, so it stays quiet. Asking leader %s to add it to the cluster…", m.Id, leader.id())
		go l.changeMembers(leader, func(ctx context.Context) error { return leader.node.AddMember(ctx, m) })

	case "remove":
		if err := needNode(); err != nil {
			return err
		}
		leader := l.leader()
		if leader == nil {
			return errors.New("no leader right now; wait for an election")
		}
		id := n.id()
		go l.changeMembers(leader, func(ctx context.Context) error { return leader.node.RemoveMember(ctx, id) })

	default:
		return fmt.Errorf("unknown action %q", a.Type)
	}
	return nil
}

func (l *Lab) changeMembers(leader *labNode, change func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		err := change(ctx)
		if err == nil {
			l.hub.feedf("membership", leader.id(), "Membership change COMMITTED.")
			return
		}
		if !errors.Is(err, raft.ErrConfigChangeInProgress) || ctx.Err() != nil {
			l.hub.feedf("error", leader.id(), "Membership change failed: %v", err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}
