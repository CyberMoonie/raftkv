// Command raftkv runs one node of a raftkv cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/CyberMoonie/raftkv/internal/kv"
	"github.com/CyberMoonie/raftkv/internal/raft"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
	"github.com/CyberMoonie/raftkv/internal/server"
)

type options struct {
	id           string
	raftAddr     string
	clientAddr   string
	clientListen string
	dataDir      string
	bootstrap    bool
	join         string
}

func main() {
	var opts options
	flag.StringVar(&opts.id, "id", "", "unique node ID (required)")
	flag.StringVar(&opts.raftAddr, "raft-addr", "127.0.0.1:7001", "address for Raft traffic between nodes")
	flag.StringVar(&opts.clientAddr, "client-addr", "127.0.0.1:6379", "address Redis clients use to reach this node")
	flag.StringVar(&opts.clientListen, "client-listen", "", "address to accept clients on (default: -client-addr)")
	flag.StringVar(&opts.dataDir, "data-dir", "", "directory for the node's state (default: data/<id>)")
	flag.BoolVar(&opts.bootstrap, "bootstrap", false, "start a new cluster with this node as its only member")
	flag.StringVar(&opts.join, "join", "", "comma-separated client addresses of members to join the cluster through")
	flag.Parse()

	if opts.id == "" {
		fmt.Fprintln(os.Stderr, "raftkv: -id is required")
		flag.Usage()
		os.Exit(2)
	}
	if opts.clientListen == "" {
		opts.clientListen = opts.clientAddr
	}
	if opts.dataDir == "" {
		opts.dataDir = filepath.Join("data", opts.id)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opts, logger); err != nil {
		logger.Error("raftkv stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options, logger *slog.Logger) error {
	self := &raftpb.Member{Id: opts.id, RaftAddr: opts.raftAddr, ClientAddr: opts.clientAddr}
	raftLn, err := net.Listen("tcp", opts.raftAddr)
	if err != nil {
		return err
	}
	clientLn, err := net.Listen("tcp", opts.clientListen)
	if err != nil {
		raftLn.Close()
		return err
	}

	transport := raft.NewGRPCTransport()
	defer transport.Close()
	cfg := raft.Config{
		ID:           opts.id,
		DataDir:      opts.dataDir,
		Transport:    transport,
		StateMachine: kv.New(),
		Logger:       logger,
	}
	if opts.bootstrap {
		cfg.Bootstrap = []*raftpb.Member{self}
	}
	node, err := raft.New(cfg)
	if err != nil {
		raftLn.Close()
		clientLn.Close()
		return err
	}

	grpcServer := raft.NewGRPCServer(node)
	defer grpcServer.Stop()
	node.Start()
	defer node.Stop()
	srv := server.New(node, logger)
	defer srv.Close()

	errc := make(chan error, 2)
	go func() { errc <- grpcServer.Serve(raftLn) }()
	go func() { errc <- srv.Serve(clientLn) }()
	logger.Info("raftkv started", "id", opts.id, "raft", opts.raftAddr, "clients", opts.clientListen)

	// A node that already knows the cluster (from its data directory) is a
	// member and does not need to join again.
	if opts.join != "" && len(node.Status().Members) == 0 {
		if err := joinCluster(ctx, strings.Split(opts.join, ","), self, logger); err != nil {
			return err
		}
	}

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		return nil
	case err := <-errc:
		return err
	}
}

// joinCluster asks the cluster to add this node by sending RAFT ADD to the
// given client addresses in turn. Followers answer with MOVED and a busy
// leader with TRYAGAIN, so it keeps cycling until the leader accepts.
func joinCluster(ctx context.Context, addrs []string, self *raftpb.Member, logger *slog.Logger) error {
	for {
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			err := requestAdd(ctx, addr, self)
			if err == nil {
				logger.Info("joined the cluster", "via", addr)
				return nil
			}
			logger.Info("join attempt failed", "via", addr, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func requestAdd(ctx context.Context, addr string, self *raftpb.Member) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)

	w := resp.NewWriter(conn)
	w.WriteCommand("RAFT", "ADD", self.Id, self.RaftAddr, self.ClientAddr)
	if err := w.Flush(); err != nil {
		return err
	}
	reply, err := resp.NewReader(conn).ReadValue()
	if err != nil {
		return err
	}
	if reply.Kind == resp.KindError {
		return errors.New(reply.Str)
	}
	return nil
}
