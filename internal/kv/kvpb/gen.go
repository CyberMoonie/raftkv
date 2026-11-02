// Package kvpb holds the protobuf messages the key-value store writes into
// the Raft log.
package kvpb

//go:generate protoc --go_out=. --go_opt=paths=source_relative kv.proto
