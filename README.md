# RaftKV — Sharded, Fault-Tolerant Key-Value Store

A distributed key-value store in Go. Raft consensus is implemented from scratch (no consensus library). Keys are spread across many Raft groups (shards) with consistent hashing. Every write is durably replicated to a quorum before it is acknowledged. Reads are linearizable.

**Stack:** Go · Raft (from scratch) · gRPC/Protobuf · Pebble LSM engine (a RocksDB-compatible LSM in pure Go) · Docker · Prometheus · Porcupine (linearizability checker)

## Architecture

```
            client (consistent-hash routing + leader cache + exactly-once sessions)
                │ gRPC
   ┌────────────┼──────────────────────────────┐
   ▼            ▼                              ▼
 node n1      node n2                        node n3       ← 3–5 processes / containers
 ┌──────────────────────┐
 │ KV service (gRPC)     │  shard = ring.Locate(key)
 │ ┌──── shard 0 ────┐   │
 │ │ Raft group      │◄──┼── AppendEntries / RequestVote / InstallSnapshot (gRPC) ──► peers
 │ │ WAL (CRC, fsync)│   │
 │ └─────────────────┘   │  … one Raft group per shard (multi-Raft), leaders spread across nodes
 │ group-commit syncer   │  one device flush shared by all shards' WALs
 │ Pebble LSM (state machine: data + applied index + client sessions, per-shard prefix)
 │ /metrics (Prometheus) │
 └──────────────────────┘
```

### Raft (`internal/raft`)
- **Leader election:** randomized timeouts (200–400 ms), **pre-vote** so a node rejoining after a partition can't disrupt a healthy leader, **check-quorum** so an isolated leader steps down, and leader stickiness.
- **Log replication:** one replicator per peer with batched AppendEntries (up to 2048 entries / 4 MB per batch) and fast conflict backtracking. The leader replicates while it is still writing to its own disk.
- **Quorum commit:** an entry commits once a majority has it durably. Only entries from the current term commit by counting replicas (Raft §5.4.2), and a new leader commits a no-op from its own term.
- **Linearizable reads (ReadIndex):** the leader records its commit index, confirms it is still leader through one heartbeat round acknowledged by a majority, then serves the read locally once that index is applied. Reads are batched, and nothing is written to the log for them.
- **Durability:** a write-ahead log with CRC-framed records (term/vote, entries, compaction marks). On restart, a torn or corrupt tail is detected and truncated.
- **Snapshotting and log compaction:** the LSM state machine stores its applied index atomically with the data, so it doubles as the snapshot. After a synced state-machine flush, the log is compacted and the WAL is rewritten atomically (write to a temp file, fsync, rename). A follower that has fallen behind the compacted log gets a point-in-time engine snapshot through `InstallSnapshot`.

### Sharding (`internal/shard`)
A consistent-hash ring (xxhash, 64 virtual nodes per shard) maps keys to shards. Each shard is its own Raft group replicated on every node. Shard *s* prefers node *s mod N* as leader, which spreads write load across the cluster.

### Exactly-once writes (`internal/storage`)
Clients retry across leader changes, so a timed-out `Put` can commit twice. If another client's write lands between the two copies, that breaks linearizability. The Porcupine checker caught this during development. Each write now carries `(client_id, request_id, min_outstanding)`. The state machine keeps a per-client table of applied request IDs, persisted and snapshotted with the data, and skips duplicates.

### Group commit (`internal/wal/groupsync.go`)
A node holds one WAL per shard. On macOS a durable flush (`F_FULLFSYNC`) flushes the whole drive cache, and this machine manages only about 270–525 of them per second, so 24 independent WALs queued behind each other (measured at about 46 ms per fsync). Instead, each WAL issues a cheap `fsync(2)`, and concurrent writers share a single `F_FULLFSYNC` barrier across all shards on the node. This is similar to the shared log in TiKV and CockroachDB. It raised write throughput from 1,634 to 6,199 ops/s (later runs: 7,795) and cut write p99 from 166 ms to 33–41 ms at 128 clients.

## Results

**Setup:** Apple M4 (10 cores, 16 GB) on macOS. Every node is a separate OS process on the same laptop, and all nodes share one SSD. fsync is **on**: every acknowledged write is on stable storage on a majority. 8 shards, 100k keys, 128-byte values, closed-loop clients, 10 s per run after a 2 s warmup. Reproduce with `make bench`.

### Throughput and latency

| Workload | Clients | 3 nodes ops/s | 3 nodes p99 | 5 nodes ops/s | 5 nodes p99 |
|---|---:|---:|---:|---:|---:|
| write-only | 8 | 394 | 37.2 ms | 390 | 38.6 ms |
| write-only | 32 | 1,181 | 43.6 ms | 1,140 | 51.2 ms |
| write-only | 128 | **7,795** | **33.1 ms** | **5,016** | **45.8 ms** |
| balanced 50/50 | 8 | 741 | 30.3 ms | 634 | 34.8 ms |
| balanced 50/50 | 32 | 1,569 | 40.8 ms | 1,644 | 41.0 ms |
| balanced 50/50 | 128 | **4,915** | **45.1 ms** | **5,189** | **42.4 ms** |
| read-heavy 95/5 | 8 | 3,848 | 18.3 ms | 3,037 | 22.8 ms |
| read-heavy 95/5 | 32 | 6,992 | 23.7 ms | 5,433 | 29.7 ms |
| read-heavy 95/5 | 128 | **14,069** | **30.2 ms** | **10,812** | **35.6 ms** |
| read-only | 8 | 30,746 | 0.65 ms | 20,076 | 1.58 ms |
| read-only | 32 | 62,638 | 1.94 ms | 34,816 | 5.41 ms |
| read-only | 128 | 108,033 | 5.00 ms | 54,911 | 14.93 ms |

Full tables with p50 and p95: [results/bench-3node.md](results/bench-3node.md) and [results/bench-5node.md](results/bench-5node.md).

Writes at low concurrency are limited by the shared laptop SSD. Every node's durable flush goes to the same device, which serializes them. With one disk per node (a real deployment) or on Linux NVMe, where fsync costs under 1 ms, write latency would drop sharply.

### Leader failover (SIGKILL under load)

`raftkv-lab failover` runs 16 clients doing reads and writes on a cluster of real processes. It sends SIGKILL to the leader of a random shard, measures when a new leader appears and when a write to that shard succeeds again, restarts the killed node (it recovers from its WAL and catches up), and repeats 20 times. At the end, it checks the whole history for linearizability.

| Cluster | New leader elected (min / p50 / p95 / max) | First write succeeds (min / p50 / p95 / max) | Linearizability |
|---|---|---|---|
| 3 nodes | 208 / 289 / 394 / 399 ms | 329 / 347 / 513 / 514 ms | ✅ Ok — 15,531 ops |
| 5 nodes | 231 / 268 / 434 / 536 ms | 334 / 360 / 533 / 831 ms | ✅ Ok — 15,590 ops |

Raw logs: [results/failover-3node.txt](results/failover-3node.txt) and [results/failover-5node.txt](results/failover-5node.txt).

### Correctness testing
- `internal/raft`: election, replication, failover on 5 nodes, a partitioned leader that must not commit, compaction followed by catch-up through `InstallSnapshot`, full-cluster restart recovered from the WAL alone, and randomized chaos (crashes plus partitions). Passes under `-race`.
- `internal/server`: an end-to-end gRPC cluster of 5 nodes with 16 concurrent clients while a nemesis crash-stops and restarts nodes, preferring current leaders. The recorded history is checked with **Porcupine**. Passed 8 of 8 repeated runs, about 4,000–5,000 operations and 7–8 node kills each.

## Running

```bash
make build                      # bin/raftkv, bin/raftkv-bench, bin/raftkv-lab
make test                       # unit + integration + linearizability tests (-race)
make bench                      # 3- and 5-node benchmark matrix → results/
make failover                   # SIGKILL failover trials + linearizability check
./bin/raftkv-lab up -n 3        # local cluster; metrics at http://127.0.0.1:7101/metrics
```

With Docker:

```bash
docker compose up -d --build                      # 3 nodes + Prometheus on :9090
docker compose --profile tools run --rm bench -matrix -markdown
docker compose kill n1                            # crash a node, watch raftkv_raft_elections_total
```

### Prometheus metrics
| Metric | Meaning |
|---|---|
| `raftkv_kv_requests_total{op,code}` / `raftkv_kv_request_duration_seconds` | Client throughput and latency |
| `raftkv_raft_commit_latency_seconds` | Replication latency (proposal → quorum commit → apply) |
| `raftkv_raft_append_entries_rtt_seconds{peer}` | Per-peer replication RTT, including the follower's fsync |
| `raftkv_wal_fsync_seconds`, `raftkv_wal_group_flush_seconds` | Disk sync cost and group-commit flushes |
| `raftkv_raft_term`, `raftkv_raft_is_leader`, `raftkv_raft_elections_total` | Leadership and failover events |
| `raftkv_raft_commit_index`, `raftkv_raft_applied_index`, `raftkv_raft_log_entries` | Replication progress and log size |
| `raftkv_raft_snapshots_{sent,installed}_total`, `raftkv_raft_log_compactions_total` | Snapshotting |
| `process_*`, `go_*`, `raftkv_lsm_disk_bytes` | CPU, memory, goroutines, and storage use |

## Layout
```
cmd/raftkv          server binary
cmd/raftkv-bench    load generator against any cluster
cmd/raftkv-lab      spawns local clusters as processes: up | bench | failover
client/             Go client: routing, leader cache, retries, sessions
internal/raft       Raft consensus
internal/wal        write-ahead log + group-commit syncer
internal/storage    Pebble-backed state machine (data, sessions, applied index)
internal/shard      consistent-hash ring
internal/server     node wiring, gRPC services, Prometheus metrics
internal/lincheck   history recorder + Porcupine KV model
internal/bench      workload generator
proto/              gRPC/Protobuf definitions
```

## Limitations
- Membership is static: no joint consensus, and nodes can't be added or removed at runtime.
- There is no shard rebalancing or leadership-transfer RPC. Leaders spread through a startup preference only.
- Session entries are never expired. A production system would add session leases.
- The Docker setup is included but these benchmarks ran as local processes. Docker networking would add overhead.
