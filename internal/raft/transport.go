package raft

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

// Transport sends Raft RPCs to other nodes.
type Transport interface {
	RequestVote(ctx context.Context, addr string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, addr string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, addr string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// maxMessageSize raises gRPC's 4 MiB default so large batches and snapshots
// fit.
const maxMessageSize = 64 << 20

// GRPCTransport is a Transport over gRPC. Connections are created on first
// use and reused afterwards.
type GRPCTransport struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]raftpb.RaftClient
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]raftpb.RaftClient),
	}
}

func (t *GRPCTransport) client(addr string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	)
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	t.conns[addr] = conn
	t.clients[addr] = c
	return c, nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, addr string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, addr string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, addr string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

// Close closes every connection.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var errs []error
	for addr, conn := range t.conns {
		errs = append(errs, conn.Close())
		delete(t.conns, addr)
		delete(t.clients, addr)
	}
	return errors.Join(errs...)
}

// NewGRPCServer returns a gRPC server that delivers incoming Raft RPCs to n.
func NewGRPCServer(n *Node) *grpc.Server {
	s := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageSize), grpc.MaxSendMsgSize(maxMessageSize))
	raftpb.RegisterRaftServer(s, grpcHandler{n: n})
	return s
}

type grpcHandler struct {
	raftpb.UnimplementedRaftServer
	n *Node
}

func (h grpcHandler) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return h.n.HandleRequestVote(req), nil
}

func (h grpcHandler) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return h.n.HandleAppendEntries(req), nil
}

func (h grpcHandler) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return h.n.HandleInstallSnapshot(req), nil
}
