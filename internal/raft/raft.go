// Package raft implements the Raft consensus algorithm from "In Search of an
// Understandable Consensus Algorithm" (Ongaro and Ousterhout). Comments refer
// to that paper by section and figure.
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

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// StateMachine is the application being replicated. Apply is called exactly
// once for every committed command, in log order, on every node.
type StateMachine interface {
	Apply(index uint64, data []byte) any
}

type Config struct {
	ID string
	// Members is the whole cluster, the same on every node.
	Members      []*raftpb.Member
	Transport    Transport
	StateMachine StateMachine

	// HeartbeatInterval is how often a leader contacts idle followers.
	HeartbeatInterval time.Duration
	// ElectionTimeout is the minimum time a follower waits without hearing
	// from a leader before it starts an election. Each wait is random in
	// [ElectionTimeout, 2*ElectionTimeout) so nodes rarely time out together.
	ElectionTimeout time.Duration
	Logger          *slog.Logger
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
	cfg    Config
	id     string
	logger *slog.Logger

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
	members          []*raftpb.Member
	electionDeadline time.Time

	// Leader state, reset after every election.
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	replicators map[string]*replicator

	// waiters are Propose calls waiting for their entry to be applied.
	waiters map[uint64]waiter

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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		logger:      cfg.Logger.With("node", cfg.ID),
		log:         newLog(0, 0),
		members:     cfg.Members,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		replicators: make(map[string]*replicator),
		waiters:     make(map[uint64]waiter),
		ctx:         ctx,
		cancel:      cancel,
	}
	n.applyCond = sync.NewCond(&n.mu)
	return n, nil
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
}

// Propose appends data to the log and waits until it is committed and
// applied, then returns what the state machine's Apply returned for it. Only
// the leader accepts proposals; other nodes return a *NotLeaderError.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != Leader {
		err := n.notLeaderError()
		n.mu.Unlock()
		return nil, err
	}
	e := n.appendEntry(raftpb.EntryType_ENTRY_TYPE_COMMAND, data)
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
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	Members     []*raftpb.Member
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Role:        n.role,
		Term:        n.term,
		LeaderID:    n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   n.log.lastIndex(),
		Members:     n.members,
	}
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
		if n.role != Leader && time.Now().After(n.electionDeadline) {
			n.startElection()
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionTimer() {
	timeout := n.cfg.ElectionTimeout + rand.N(n.cfg.ElectionTimeout)
	n.electionDeadline = time.Now().Add(timeout)
}

// startElection turns this node into a candidate for a new term and asks
// every other member for its vote.
func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionTimer()
	n.logger.Info("starting election", "term", n.term)

	term := n.term
	req := &raftpb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	votes := 1 // our own
	if n.isQuorum(votes) {
		n.becomeLeader()
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
			if n.role != Candidate || n.term != term || !resp.VoteGranted {
				return
			}
			votes++
			if n.isQuorum(votes) {
				n.becomeLeader()
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
	if req.Term > n.term {
		n.becomeFollower(req.Term)
	}

	// Election restriction (§5.4.1): only vote for a candidate whose log is at
	// least as up-to-date as ours, so a leader always holds every committed
	// entry.
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if (n.votedFor == "" || n.votedFor == req.CandidateId) && upToDate {
		n.votedFor = req.CandidateId
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
	}
	if n.role == Leader {
		n.stopReplicators()
	}
	if n.role != Follower {
		n.logger.Info("became follower", "term", n.term)
	}
	n.role = Follower
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.term)

	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for _, m := range n.members {
		if m.Id != n.id {
			n.nextIndex[m.Id] = n.log.lastIndex() + 1
			n.startReplicator(m)
		}
	}
	// A leader may only count replicas for entries of its own term (§5.4.2),
	// so entries left over from earlier terms are committed only once it
	// commits one of its own. A no-op entry gets that done right away (§8).
	n.appendEntry(raftpb.EntryType_ENTRY_TYPE_NOOP, nil)
	n.advanceCommit()
	n.triggerReplication()
}

// appendEntry adds an entry of the current term to the leader's log.
func (n *Node) appendEntry(typ raftpb.EntryType, data []byte) *raftpb.Entry {
	e := &raftpb.Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: typ, Data: data}
	n.log.append(e)
	return e
}

// runApplier feeds committed entries to the state machine and answers the
// Propose calls waiting for them.
func (n *Node) runApplier() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
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
