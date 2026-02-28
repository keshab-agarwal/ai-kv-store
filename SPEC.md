# Distributed In-Memory Key-Value Store — Specification

## Architecture

A single-cluster, single-datacenter distributed KV store providing **linearizable**
Get/Put/Delete operations on 128-bit opaque keys with values up to 1 MiB.

### Consensus Protocol

The system uses a **coordinator-based quorum replication** protocol (designed from
scratch — not a named algorithm):

- **Roles**: Each node is a Follower, Candidate, or Coordinator.
- **Epochs**: Monotonically increasing election terms. Each epoch has at most one coordinator.
- **Election**: Followers start an election after a randomized timeout (300-500ms).
  A candidate requests votes from all peers. A vote is granted if the candidate's
  epoch is higher and its log is at least as up-to-date. Majority wins.
- **Writes**: The coordinator appends to its local log, replicates to all followers,
  and commits once a majority (including itself) acknowledges. A no-op entry is
  appended at the start of each new epoch to commit entries from prior epochs.
- **Reads**: The coordinator serves reads from its local state machine after verifying
  it still holds a valid lease (renewed by successful heartbeat rounds). This avoids
  an extra round-trip for the 95% read workload.
- **Crash recovery**: A recovered node starts as an empty follower. The coordinator
  detects it is behind and sends a full state snapshot, after which normal replication
  resumes. Replication restores within ≤30 seconds.

### Linearizability Guarantee

All operations are serialized through the coordinator. Writes are linearized at
their commit point (majority replication). Reads are linearized at the point the
coordinator confirms its lease is valid and reads from the applied state machine.
The lease mechanism ensures no two coordinators can serve reads simultaneously
(lease_duration < election_timeout_min - max_rtt).

### Client Deduplication

Each client tags write operations with a (clientID, seqNum) pair. The coordinator
maintains a deduplication table. On TIMEOUT + retry, the duplicate is detected
and the cached result is returned, ensuring at-most-once semantics.

## API

| Operation | Input | Output |
|-----------|-------|--------|
| Put(key, value) | 16-byte key, ≤1MiB value | OK, ERROR, TIMEOUT |
| Get(key) | 16-byte key | FOUND(value), NOT_FOUND, ERROR, TIMEOUT |
| Delete(key) | 16-byte key | OK, ERROR, TIMEOUT |

## HTTP API

All operations use `POST` with JSON bodies. Keys are hex-encoded (32 chars).
Values are base64-encoded.

```
POST /api/put     {"key":"...","value":"...","client_id":"...","client_seq":N}
POST /api/get     {"key":"..."}
POST /api/delete  {"key":"...","client_id":"...","client_seq":N}
GET  /api/status
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| HeartbeatInterval | 50ms | Coordinator heartbeat frequency |
| ElectionTimeoutMin | 300ms | Minimum election timeout |
| ElectionTimeoutMax | 500ms | Maximum election timeout |
| LeaseDuration | 150ms | Read lease validity window |
| RPCTimeout | 200ms | Inter-node RPC timeout |
| WriteTimeout | 5s | Client write operation timeout |
| MaxValueSize | 1 MiB | Maximum value size |

## Capacity

- Keys: 128-bit opaque (16 bytes)
- Values: up to 1 MiB, typical mean ~1 KiB
- Nodes: 3 to 5
- Fault tolerance: 1 node crash with majority alive
- Target: ≥1B distinct keys over the lifetime of the cluster
