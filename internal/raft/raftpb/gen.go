// Package raftpb holds the protobuf messages and gRPC service used between
// Raft nodes.
package raftpb

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative raft.proto
