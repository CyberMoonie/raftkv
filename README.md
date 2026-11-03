# raftkv

[![CI](https://github.com/CyberMoonie/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/CyberMoonie/raftkv/actions/workflows/ci.yml)

A Redis-compatible key-value store replicated with the [Raft](https://raft.github.io/raft.pdf) consensus algorithm, written in Go from scratch.

Use it with `redis-cli` or any Redis client. Data survives as long as a majority of nodes is up.

## Features

- **Leader election and log replication**: randomized timeouts, Pre-Vote, election restriction, fast conflict backtracking
- **Persistence**: term, vote and log are fsynced before replying; torn writes are detected with CRC-32C
- **Log compaction**: snapshots, plus InstallSnapshot for followers that fall behind
- **Membership changes**: add or remove one node at a time, while the cluster keeps running
- **Linearizable**: writes go through the log; reads use ReadIndex (one heartbeat round, no disk write)

## Raft Lab

An interactive dashboard that runs a 5-node cluster on a simulated network and shows every message, election and commit as it happens. Crash nodes, split the network, add packet loss, run Redis commands, and follow lessons that explain Raft and Redis from zero.

```sh
go run ./cmd/raftkv-lab                # http://localhost:8080, Redis on ports 7379-7383
docker compose --profile lab up lab    # same, in Docker
```

## Run with Docker

```sh
docker compose up -d --build                # 3 nodes on ports 6379, 6380, 6381
redis-cli -c -p 6380 SET user:1 Cristian    # -c follows the redirect to the leader
redis-cli -c -p 6381 GET user:1
redis-cli -p 6379 RAFT INFO                 # role, term, log indexes, members
docker compose stop node1                   # kill a node; the others elect a new leader
docker compose --profile extra up -d node4  # a 4th node joins at runtime
```

## Run locally

```sh
go run ./cmd/raftkv -id=n1 -raft-addr=127.0.0.1:7001 -client-addr=127.0.0.1:6379 -bootstrap
go run ./cmd/raftkv -id=n2 -raft-addr=127.0.0.1:7002 -client-addr=127.0.0.1:6380 -join=127.0.0.1:6379
go run ./cmd/raftkv -id=n3 -raft-addr=127.0.0.1:7003 -client-addr=127.0.0.1:6381 -join=127.0.0.1:6379
```

## Commands

`GET` `SET [EX|PX]` `DEL` `EXISTS` `INCR` `DECR` `INCRBY` `EXPIRE` `TTL` `PERSIST` `KEYS` `DBSIZE` `PING`

Cluster: `RAFT INFO`, `RAFT ADD <id> <raft-addr> <client-addr>`, `RAFT REMOVE <id>`, `RAFT SNAPSHOT`

## Tests

```sh
go test -race ./...
```

Clusters run over an in-memory network that can cut nodes off, so the tests cover crashes, partitions, restarts and membership changes. A [Porcupine](https://github.com/anishathalye/porcupine) check runs concurrent clients through all of that and verifies the results are linearizable.

## Built with

Go, gRPC, Protocol Buffers, Docker, Porcupine

## License

[MIT](LICENSE)
