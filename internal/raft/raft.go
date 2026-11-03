// Package raft implements the Raft consensus algorithm from "In Search of an
// Understandable Consensus Algorithm" (Ongaro and Ousterhout). Comments refer
// to that paper by section and figure.
//
// Membership changes follow chapter 4 of Ongaro's dissertation, "Consensus:
// Bridging Theory and Practice".
//
// A Node keeps one mutex for all its state. It never holds the mutex while
// waiting on the network or on the state machine.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// StateMachine is the application being replicated. Apply is called exactly
// once for every committed command, in log order, on every node. Query
// answers a read-only query without changing the state. Snapshot serializes
// the current state and Restore replaces the state with a snapshot. Calls
// never overlap.
type StateMachine interface {
	Apply(index uint64, data []byte) any
	Query(query []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

type Config struct {
	ID string
	// DataDir holds the node's term, vote, log and snapshot.
	DataDir string
	// Bootstrap starts a new cluster with these members if the node has no
	// saved state yet. Either one node bootstraps alone and the others are
	// added with AddMember, or every initial node bootstraps with the same
	// list. Nodes that join an existing cluster leave it empty.
	Bootstrap    []*raftpb.Member
	Transport    Transport
	StateMachine StateMachine

	// HeartbeatInterval is how often a leader contacts idle followers.
	HeartbeatInterval time.Duration
	// ElectionTimeout is the minimum time a follower waits without hearing
	// from a leader before it starts an election. Each wait is random in
	// [ElectionTimeout, 2*ElectionTimeout) so nodes rarely time out together.
	ElectionTimeout time.Duration
	// SnapshotThreshold is how many applied entries the log may hold before
	// the node snapshots the state machine and discards them.
	SnapshotThreshold uint64
	// DisablePreVote makes nodes start elections directly, as in the
	// original paper. Only useful to see what Pre-Vote prevents.
	DisablePreVote bool
	Logger         *slog.Logger
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

var (
	ErrStopped = errors.New("raft: node stopped")
	// ErrLeadershipLost means another leader overwrote the proposed entry,
	// so it was not applied.
	ErrLeadershipLost = errors.New("raft: leadership lost before the entry was committed")
)

// NotLeaderError is returned when a request reaches a node that is not the
// leader. Leader is the current leader, or nil if none is known yet.
type NotLeaderError struct {
	Leader *raftpb.Member
}

func (e *NotLeaderError) Error() string {
	if e.Leader == nil {
		return "raft: not the leader, and no leader is known"
	}
	return "raft: not the leader, the leader is " + e.Leader.Id
}

type Node struct {
	cfg     Config
	id      string
	logger  *slog.Logger
	storage *storage

	mu        sync.Mutex
	applyCond *sync.Cond // signalled when commitIndex moves or the node stops

	// Persistent state on all servers (Figure 2).
	term     uint64
	votedFor string
	log      *raftLog

	// Volatile state.
	role             Role
	leaderID         string
	commitIndex      uint64
	lastApplied      uint64
	electionDeadline time.Time
	electionTimeout  time.Duration // the random timeout behind the deadline
	lastContact      time.Time     // last message from a current leader
	// campaign numbers vote rounds. Any role change starts a new number, so
	// late answers to an old round are ignored.
	campaign uint64

	// members is the latest configuration in the log; configIndex is its
	// entry, or the snapshot's last index if it came from the snapshot.
	members     []*raftpb.Member
	configIndex uint64

	// Leader state, reset after every election.
	leaderStart uint64 // index of the no-op that opened this term
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	replicators map[string]*replicator

	// waiters are Propose calls waiting for their entry to be applied.
	waiters map[uint64]waiter

	// ReadIndex state (read.go). readSeq numbers reads; readAcks holds, per
	// follower, the readSeq of the latest heartbeat it answered.
	readSeq      uint64
	readAcks     map[string]uint64
	pendingReads []*readRequest // waiting for leadership confirmation
	readyReads   []*readRequest // confirmed, waiting for the applier

	// snapshot is the latest one, kept to send to followers that are behind.
	snapshot *raftpb.Snapshot
	// pendingSnapshot came from the leader and waits for the applier to
	// load it into the state machine.
	pendingSnapshot   *raftpb.Snapshot
	snapshotRequested bool

	stopped bool
	ctx     context.Context // cancelled by Stop, aborts in-flight RPCs
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type waiter struct {
	term uint64 // term of the proposed entry
	ch   chan result
}

type result struct {
	value any
	err   error
}

func New(cfg Config) (*Node, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("raft: Config.ID is required")
	case cfg.DataDir == "":
		return nil, errors.New("raft: Config.DataDir is required")
	case cfg.Transport == nil:
		return nil, errors.New("raft: Config.Transport is required")
	case cfg.StateMachine == nil:
		return nil, errors.New("raft: Config.StateMachine is required")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 500 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	st, saved, err := openStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	snap := saved.snapshot
	entries, changed := entriesAfter(snap, saved.entries)
	if changed {
		if err := st.rewriteLog(entries); err != nil {
			st.close()
			return nil, err
		}
	}
	if snap.LastIndex > 0 {
		if err := cfg.StateMachine.Restore(snap.Data); err != nil {
			st.close()
			return nil, fmt.Errorf("raft: restoring snapshot: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		logger:      cfg.Logger.With("node", cfg.ID),
		storage:     st,
		term:        saved.hardState.Term,
		votedFor:    saved.hardState.VotedFor,
		log:         newLog(snap.LastIndex, snap.LastTerm),
		commitIndex: snap.LastIndex,
		lastApplied: snap.LastIndex,
		snapshot:    snap,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		replicators: make(map[string]*replicator),
		waiters:     make(map[uint64]waiter),
		ctx:         ctx,
		cancel:      cancel,
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.log.append(entries...)

	if len(cfg.Bootstrap) > 0 && n.log.lastIndex() == 0 && n.term == 0 {
		if err := n.bootstrap(cfg.Bootstrap); err != nil {
			st.close()
			return nil, err
		}
	}
	n.refreshConfig()
	return n, nil
}

// bootstrap writes the first configuration as entry 1 of term 1. Nodes that
// bootstrap with the same members write identical entries, so their logs
// agree from the start.
func (n *Node) bootstrap(members []*raftpb.Member) error {
	data, err := proto.Marshal(&raftpb.Configuration{Members: members})
	if err != nil {
		return err
	}
	n.term = 1
	n.persistHardState()
	e := &raftpb.Entry{Index: 1, Term: 1, Type: raftpb.EntryType_ENTRY_TYPE_CONFIG, Data: data}
	n.log.append(e)
	n.mustPersist(n.storage.appendEntries([]*raftpb.Entry{e}))
	return nil
}

// entriesAfter returns the saved entries that continue snap, and whether
// that differs from what the log file holds. A crash between saving a
// snapshot and rewriting the log can leave entries the snapshot already
// covers, or, after InstallSnapshot, entries that do not continue it at all.
func entriesAfter(snap *raftpb.Snapshot, entries []*raftpb.Entry) ([]*raftpb.Entry, bool) {
	if len(entries) == 0 || entries[0].Index == snap.LastIndex+1 {
		return entries, false
	}
	first := entries[0].Index
	if first > snap.LastIndex+1 {
		return nil, true
	}
	i := snap.LastIndex - first // position of the snapshot's last entry
	if i >= uint64(len(entries)) || entries[i].Term != snap.LastTerm {
		return nil, true
	}
	return entries[i+1:], true
}

// Start runs the election timer and the applier in the background.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	n.wg.Add(2)
	go n.runTicker()
	go n.runApplier()
}

// Stop shuts the node down and waits for its goroutines to exit.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.cancel()
	n.stopReplicators()
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
	if err := n.storage.close(); err != nil {
		n.logger.Error("closing storage", "err", err)
	}
}

// Propose appends data to the log and waits until it is committed and
// applied, then returns what the state machine's Apply returned for it. Only
// the leader accepts proposals; other nodes return a *NotLeaderError.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if err := n.checkLeader(); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	return n.propose(ctx, raftpb.EntryType_ENTRY_TYPE_COMMAND, data)
}

func (n *Node) checkLeader() error {
	if n.stopped {
		return ErrStopped
	}
	if n.role != Leader {
		return n.notLeaderError()
	}
	return nil
}

// propose appends an entry as leader, releases the lock and waits for the
// entry to be applied. The caller holds the lock and has checked leadership.
func (n *Node) propose(ctx context.Context, typ raftpb.EntryType, data []byte) (any, error) {
	e := n.appendEntry(typ, data)
	ch := make(chan result, 1)
	n.waiters[e.Index] = waiter{term: e.Term, ch: ch}
	n.advanceCommit()
	n.triggerReplication()
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r.value, r.err
	case <-ctx.Done():
		n.mu.Lock()
		if w, ok := n.waiters[e.Index]; ok && w.ch == ch {
			delete(n.waiters, e.Index)
		}
		n.mu.Unlock()
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, ErrStopped
	}
}

type Status struct {
	ID          string
	Role        Role
	Term        uint64
	VotedFor    string
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	// SnapshotIndex is the last entry covered by the latest snapshot.
	SnapshotIndex uint64
	Members       []*raftpb.Member
	// ElectionDeadline is when a follower or candidate starts an election
	// unless it hears from a leader; ElectionTimeout is the random wait that
	// led to it.
	ElectionDeadline time.Time
	ElectionTimeout  time.Duration
	// Progress is the leader's view of each follower's log, nil on others.
	Progress []Progress
}

// Progress is how far the leader has replicated its log to one follower.
type Progress struct {
	ID    string
	Match uint64 // highest entry known to be stored on the follower
	Next  uint64 // next entry to send
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := Status{
		ID:               n.id,
		Role:             n.role,
		Term:             n.term,
		VotedFor:         n.votedFor,
		LeaderID:         n.leaderID,
		CommitIndex:      n.commitIndex,
		LastApplied:      n.lastApplied,
		LastIndex:        n.log.lastIndex(),
		SnapshotIndex:    n.log.firstIndex(),
		Members:          n.members,
		ElectionDeadline: n.electionDeadline,
		ElectionTimeout:  n.electionTimeout,
	}
	if n.role == Leader {
		for _, m := range n.members {
			if m.Id != n.id {
				st.Progress = append(st.Progress, Progress{ID: m.Id, Match: n.matchIndex[m.Id], Next: n.nextIndex[m.Id]})
			}
		}
	}
	return st
}

// LogTail returns copies of up to count of the newest entries in the log.
func (n *Node) LogTail(count int) []*raftpb.Entry {
	n.mu.Lock()
	defer n.mu.Unlock()
	from := n.log.firstIndex() + 1
	if last := n.log.lastIndex(); last+1-from > uint64(count) {
		from = last + 1 - uint64(count)
	}
	entries := n.log.slice(from, n.log.lastIndex()+1)
	for i, e := range entries {
		entries[i] = proto.CloneOf(e)
	}
	return entries
}

// RequestSnapshot asks the node to snapshot and compact its log as soon as
// it has applied anything new, without waiting for SnapshotThreshold.
func (n *Node) RequestSnapshot() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotRequested = true
	n.applyCond.Broadcast()
}

// runTicker starts an election whenever the election timeout passes without
// a heartbeat from a leader (§5.2).
func (n *Node) runTicker() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		// Only members campaign: a node that was removed, or is waiting to
		// be added, stays quiet.
		if n.role != Leader && time.Now().After(n.electionDeadline) && n.member(n.id) != nil {
			if n.cfg.DisablePreVote {
				n.startElection()
			} else {
				n.startPreVote()
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionTimer() {
	n.electionTimeout = n.cfg.ElectionTimeout + rand.N(n.cfg.ElectionTimeout)
	n.electionDeadline = time.Now().Add(n.electionTimeout)
}

// startPreVote asks the other members whether they would vote for us in the
// next term, without raising our own term (dissertation §9.6). A node cut off
// from the cluster keeps failing here instead of raising its term again and
// again, so when it reconnects it cannot force a healthy leader to step down.
func (n *Node) startPreVote() {
	n.resetElectionTimer()
	n.logger.Info("starting pre-vote", "term", n.term+1)
	req := &raftpb.RequestVoteRequest{
		Term:         n.term + 1,
		CandidateId:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
		PreVote:      true,
	}
	n.requestVotes(req, n.startElection)
}

// startElection turns this node into a candidate for a new term and asks
// every other member for its vote.
func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardState()
	n.resetElectionTimer()
	n.logger.Info("starting election", "term", n.term)

	req := &raftpb.RequestVoteRequest{
		Term:         n.term,
		CandidateId:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	n.requestVotes(req, n.becomeLeader)
}

// requestVotes sends req to every other member and calls won, with the lock
// held, once a majority including ourselves has granted it.
func (n *Node) requestVotes(req *raftpb.RequestVoteRequest, won func()) {
	n.campaign++
	campaign := n.campaign
	votes := 1 // our own
	if n.isQuorum(votes) {
		won()
		return
	}
	for _, m := range n.members {
		if m.Id == n.id {
			continue
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ctx, cancel := n.rpcContext()
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, m.RaftAddr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.becomeFollower(resp.Term)
				return
			}
			if n.campaign != campaign || !resp.VoteGranted {
				return
			}
			votes++
			if n.isQuorum(votes) {
				won()
			}
		}()
	}
}

// HandleRequestVote answers a candidate (Figure 2, "RequestVote RPC").
func (n *Node) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term || n.stopped {
		return &raftpb.RequestVoteResponse{Term: n.term}
	}
	// A removed server stops hearing from the leader but is never told it
	// was removed, so it keeps starting elections with ever higher terms.
	// While we know a live leader, we ignore such requests without adopting
	// their term, so they cannot depose it (dissertation §4.2.3).
	if n.role == Leader || (n.leaderID != "" && time.Since(n.lastContact) < n.cfg.ElectionTimeout) {
		return &raftpb.RequestVoteResponse{Term: n.term}
	}

	// Election restriction (§5.4.1): only vote for a candidate whose log is at
	// least as up-to-date as ours, so a leader always holds every committed
	// entry.
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if req.PreVote {
		// Answer as a real election at req.Term would, but change nothing.
		return &raftpb.RequestVoteResponse{Term: n.term, VoteGranted: req.Term > n.term && upToDate}
	}

	if req.Term > n.term {
		n.becomeFollower(req.Term)
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateId) && upToDate {
		n.votedFor = req.CandidateId
		n.persistHardState()
		n.resetElectionTimer()
		return &raftpb.RequestVoteResponse{Term: n.term, VoteGranted: true}
	}
	return &raftpb.RequestVoteResponse{Term: n.term}
}

// becomeFollower adopts term if it is newer and steps down from leader or
// candidate.
func (n *Node) becomeFollower(term uint64) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.leaderID = ""
		n.persistHardState()
	}
	if n.role == Leader {
		n.stopReplicators()
		n.failPendingReads()
		n.leaderID = ""
	}
	if n.role != Follower {
		n.logger.Info("became follower", "term", n.term)
	}
	n.role = Follower
	n.campaign++
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.campaign++
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.term)

	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	n.readAcks = make(map[string]uint64)
	n.syncReplicators()
	// A leader may only count replicas for entries of its own term (§5.4.2),
	// so entries left over from earlier terms are committed only once it
	// commits one of its own. A no-op entry gets that done right away (§8).
	n.leaderStart = n.appendEntry(raftpb.EntryType_ENTRY_TYPE_NOOP, nil).Index
	n.advanceCommit()
	n.triggerReplication()
}

// appendEntry adds an entry of the current term to the leader's log.
func (n *Node) appendEntry(typ raftpb.EntryType, data []byte) *raftpb.Entry {
	e := &raftpb.Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: typ, Data: data}
	n.log.append(e)
	n.mustPersist(n.storage.appendEntries([]*raftpb.Entry{e}))
	if typ == raftpb.EntryType_ENTRY_TYPE_CONFIG {
		n.refreshConfig()
		n.syncReplicators()
	}
	return e
}

func (n *Node) persistHardState() {
	n.mustPersist(n.storage.saveHardState(&raftpb.HardState{Term: n.term, VotedFor: n.votedFor}))
}

// mustPersist crashes the node when the disk fails. Raft's guarantees rest
// on what was saved, so carrying on without it could lose committed data or
// elect two leaders; stopping is the safe failure.
func (n *Node) mustPersist(err error) {
	if err != nil {
		panic(fmt.Sprintf("raft %s: persisting state: %v", n.id, err))
	}
}

// runApplier feeds committed entries to the state machine and answers the
// Propose calls waiting for them. It is the only goroutine that touches the
// state machine, so it also loads and takes snapshots.
func (n *Node) runApplier() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex &&
			!n.snapshotDue() && !n.hasRunnableReads() {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			n.mu.Unlock()
			err := n.cfg.StateMachine.Restore(snap.Data)
			n.mu.Lock()
			if err != nil {
				panic(fmt.Sprintf("raft %s: restoring snapshot: %v", n.id, err))
			}
			n.lastApplied = snap.LastIndex
			continue
		}
		if reads := n.runnableReads(); len(reads) > 0 {
			n.mu.Unlock()
			for _, r := range reads {
				r.ch <- result{value: n.cfg.StateMachine.Query(r.query)}
			}
			n.mu.Lock()
			continue
		}
		if n.snapshotDue() {
			n.takeSnapshot()
			continue
		}

		entries := n.log.slice(n.lastApplied+1, n.commitIndex+1)
		n.mu.Unlock()
		results := make([]any, len(entries))
		for i, e := range entries {
			if e.Type == raftpb.EntryType_ENTRY_TYPE_COMMAND {
				results[i] = n.cfg.StateMachine.Apply(e.Index, e.Data)
			}
		}
		n.mu.Lock()

		for i, e := range entries {
			n.lastApplied = e.Index
			w, ok := n.waiters[e.Index]
			if !ok {
				continue
			}
			delete(n.waiters, e.Index)
			// A different term at this index means a newer leader replaced
			// the proposed entry with its own.
			if w.term == e.Term {
				w.ch <- result{value: results[i]}
			} else {
				w.ch <- result{err: ErrLeadershipLost}
			}
		}
	}
}

func (n *Node) snapshotDue() bool {
	if n.lastApplied <= n.log.firstIndex() {
		return false
	}
	return n.snapshotRequested || n.lastApplied-n.log.firstIndex() >= n.cfg.SnapshotThreshold
}

// takeSnapshot saves the state machine as of lastApplied and drops the log
// entries it covers (§7). The state machine is serialized without the lock
// held, so the node keeps replicating meanwhile.
func (n *Node) takeSnapshot() {
	n.snapshotRequested = false
	index := n.lastApplied
	term, _ := n.log.term(index)
	n.mu.Unlock()
	data, err := n.cfg.StateMachine.Snapshot()
	n.mu.Lock()
	if err != nil {
		panic(fmt.Sprintf("raft %s: taking snapshot: %v", n.id, err))
	}
	if index <= n.log.firstIndex() {
		return // a snapshot from the leader got further meanwhile
	}

	members, _ := n.configAt(index)
	snap := &raftpb.Snapshot{
		LastIndex: index,
		LastTerm:  term,
		Data:      data,
		Config:    &raftpb.Configuration{Members: members},
	}
	n.mustPersist(n.storage.saveSnapshot(snap))
	n.log.compact(index)
	n.mustPersist(n.storage.rewriteLog(n.log.slice(index+1, n.log.lastIndex()+1)))
	n.snapshot = snap
	n.logger.Info("took snapshot", "index", index, "bytes", len(data))
}

func (n *Node) isQuorum(votes int) bool {
	return votes > len(n.members)/2
}

func (n *Node) member(id string) *raftpb.Member {
	for _, m := range n.members {
		if m.Id == id {
			return m
		}
	}
	return nil
}

func (n *Node) notLeaderError() error {
	return &NotLeaderError{Leader: n.member(n.leaderID)}
}

// rpcContext bounds one RPC. Waiting longer than an election timeout is
// pointless: by then the cluster has moved on.
func (n *Node) rpcContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
}
