// Package server accepts Redis clients and runs their commands through Raft.
//
// Writes go through the Raft log. Reads skip it with ReadIndex: the leader
// confirms it is still the leader and answers from its state machine. Either
// way a reply reflects every write acknowledged before it (linearizability).
// Followers do not serve commands; they redirect the client to the leader.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/CyberMoonie/raftkv/internal/kv"
	"github.com/CyberMoonie/raftkv/internal/raft"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
)

// requestTimeout bounds how long a command waits to be committed.
const requestTimeout = 5 * time.Second

type Server struct {
	node   *raft.Node
	logger *slog.Logger

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
}

func New(node *raft.Node, logger *slog.Logger) *Server {
	return &Server{node: node, logger: logger, conns: make(map[net.Conn]struct{})}
}

// Serve accepts clients on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ln.Close()
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// Close stops accepting clients and closes every open connection.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for conn := range s.conns {
		conn.Close()
	}
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				w.WriteValue(resp.Errorf("ERR %v", err))
				w.Flush()
			}
			return
		}
		reply, quit := s.execute(args)
		w.WriteValue(reply)
		// Flush once per pipelined batch rather than once per command.
		if quit || r.Buffered() == 0 {
			if err := w.Flush(); err != nil || quit {
				return
			}
		}
	}
}

// execute runs one command and reports whether the client asked to quit.
func (s *Server) execute(args [][]byte) (reply resp.Value, quit bool) {
	switch strings.ToUpper(string(args[0])) {
	case "PING":
		if len(args) > 1 {
			return resp.Bulk(args[1]), false
		}
		return resp.Simple("PONG"), false
	case "QUIT":
		return resp.OK(), true
	case "COMMAND":
		// redis-cli asks for command docs on start; an empty list is enough.
		return resp.Array(), false
	case "RAFT":
		return s.raftCommand(args), false
	}

	if err := kv.Validate(args); err != nil {
		return resp.Error(err.Error()), false
	}
	// Only the leader proposes, so this is the leader's clock (see kv.Encode).
	data, err := kv.Encode(args, time.Now().UnixMilli())
	if err != nil {
		return resp.Errorf("ERR %v", err), false
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var v any
	if kv.IsReadOnly(args) {
		v, err = s.node.Read(ctx, data)
	} else {
		v, err = s.node.Propose(ctx, data)
	}
	if err != nil {
		return errorReply(err), false
	}
	return v.(resp.Value), false
}

const raftUsage = "ERR usage: RAFT INFO | RAFT ADD <id> <raft-addr> <client-addr> | RAFT REMOVE <id> | RAFT SNAPSHOT"

// raftCommand implements the cluster admin commands.
func (s *Server) raftCommand(args [][]byte) resp.Value {
	if len(args) < 2 {
		return resp.Error(raftUsage)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	sub := strings.ToUpper(string(args[1]))
	switch {
	case sub == "INFO" && len(args) == 2:
		return resp.Bulk([]byte(info(s.node.Status())))
	case sub == "ADD" && len(args) == 5:
		m := &raftpb.Member{Id: string(args[2]), RaftAddr: string(args[3]), ClientAddr: string(args[4])}
		if err := s.node.AddMember(ctx, m); err != nil {
			return errorReply(err)
		}
		return resp.OK()
	case sub == "REMOVE" && len(args) == 3:
		if err := s.node.RemoveMember(ctx, string(args[2])); err != nil {
			return errorReply(err)
		}
		return resp.OK()
	case sub == "SNAPSHOT" && len(args) == 2:
		s.node.RequestSnapshot()
		return resp.OK()
	}
	return resp.Error(raftUsage)
}

// info formats a node's status like the output of Redis's INFO command.
func info(st raft.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "id:%s\r\nrole:%s\r\nterm:%d\r\nleader:%s\r\n", st.ID, st.Role, st.Term, st.LeaderID)
	fmt.Fprintf(&b, "commit_index:%d\r\nlast_applied:%d\r\nlast_index:%d\r\nsnapshot_index:%d\r\n",
		st.CommitIndex, st.LastApplied, st.LastIndex, st.SnapshotIndex)
	fmt.Fprintf(&b, "members:%d\r\n", len(st.Members))
	for _, m := range st.Members {
		fmt.Fprintf(&b, "member:%s raft=%s client=%s\r\n", m.Id, m.RaftAddr, m.ClientAddr)
	}
	return b.String()
}

// errorReply turns a Raft error into a Redis error a client can act on.
func errorReply(err error) resp.Value {
	var notLeader *raft.NotLeaderError
	switch {
	case errors.As(err, &notLeader):
		// MOVED is how Redis Cluster sends a client to the node that owns a
		// key. Here the leader owns every key, so the slot is always 0, and
		// redis-cli -c follows the redirect on its own.
		if notLeader.Leader != nil && notLeader.Leader.ClientAddr != "" {
			return resp.Errorf("MOVED 0 %s", notLeader.Leader.ClientAddr)
		}
		return resp.Error("TRYAGAIN no leader elected yet")
	case errors.Is(err, context.DeadlineExceeded):
		return resp.Error("TRYAGAIN timed out waiting for a majority of the cluster")
	case errors.Is(err, raft.ErrLeadershipLost):
		return resp.Error("TRYAGAIN leadership changed, retry")
	case errors.Is(err, raft.ErrConfigChangeInProgress):
		return resp.Error("TRYAGAIN another membership change is in progress")
	}
	return resp.Errorf("ERR %v", err)
}
