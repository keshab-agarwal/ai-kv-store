# Distributed Key-Value Store

A distributed key-value store that runs on 3 nodes, provides **causal consistency**, and survives **single node failures**. It implements the interfaces and behavioral contract in [SPEC.md](SPEC.md).

## Overview

- **Keys:** 16-byte fixed size. **Values:** up to 1 MiB.
- **Consistency:** Causal consistency (session guarantees: read your writes, monotonic reads, monotonic writes, writes follow reads). No per-key linearizability; concurrent writes to the same key may be observed in different orders by different clients.
- **Fault tolerance:** Fail-stop. At most one node may be down at a time. Data is replicated so that a successful Put survives the crash of any single node.
- **Persistence:** Each node persists its store to disk; data survives process restart.

## Architecture

### Sharding (key ownership)

- The cluster has exactly **3 nodes**, with IDs 0, 1, 2.
- **Key ownership** is determined by a hash of the key modulo 3: `owner = hash(key) % 3`. All writes for a key are applied on that key’s owner node first.
- Clients can connect to any node. A Put that hits a non-owner node is **forwarded** to the owner. Gets are served from the local store (after replication) or by forwarding to a peer that has the key.

### Replication

- **Put flow (owner):**
  1. Apply the write locally with a new **version vector** (causal metadata).
  2. **Persist** to disk (so the node that processed the write can recover after restart).
  3. **Synchronously** replicate to at least one other node (so the write survives the owner’s crash).
  4. Replicate asynchronously to the remaining node(s).
  5. Return success to the client.

- **Replication message:** Each replicated write carries `(key, value, version vector)`. Replicas apply it into their local store and persist. No consensus protocol (no Raft/Paxos); ordering is causal via version vectors.

### Consistency (causal)

- **Version vectors:** Each node maintains a 3-component logical clock. Every stored value is tagged with a version vector describing its causal position.
- **Multi-version storage:** For each key we keep multiple versions when they are not comparable (concurrent). On **Get**, the server chooses a version that is **visible** to the client’s causal context (version vector sent by the client).
- **Client causal context:** The client library keeps a version vector per connection. On every Get/Put response it merges the returned version into this context and sends the updated context with the next request. So:
  - **Read your writes:** The client’s context includes its own writes, so subsequent reads see them or something causally later.
  - **Monotonic reads:** The context only grows, so the client never sees an older version of a key after a newer one.
  - **Monotonic writes:** Writes from the same client are ordered by the owner’s version vector.
  - **Writes follow reads:** A write carries the client’s context (including what it read), so any observer of that write has a view that includes those reads.

### Fault tolerance and durability

- **Single fail-stop node:** At most one node is down at a time. No Byzantine behavior.
- **Durability:** Before a Put returns success, the owner has persisted the write and replicated it to at least one other node. So if the owner crashes after returning, the write is still on another node. After a node restarts, it loads state from disk; any writes it missed while down are received via later replication from peers (when it dials them for new operations).
- **Availability:** If the owner is down, Put for keys owned by that node returns `ErrNodeDown` until the node is back. Gets can be served by any node that has the key (including replicas).

### Network partition (harness)

- For testing, each node can run an optional **admin HTTP** server (`-admin <port>`). Endpoints:
  - `POST /partition?peer=<addr>` — stop dialing that peer (simulate partition).
  - `POST /heal?peer=<addr>` — resume dialing that peer.
- The data plane (Put/Get/Replicate) checks a per-node blocklist before opening connections; partitioned peers are not contacted.

## How to run

### Build

```bash
go build -o bin/node ./cmd/node
go build -o bin/client ./cmd/client
```

### Start a 3-node cluster

From the repo root:

```bash
./scripts/start-cluster.sh
```

Or manually (each in its own terminal or background):

```bash
./bin/node -id 0 -port 8000 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -data ./data-0
./bin/node -id 1 -port 8001 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -data ./data-1
./bin/node -id 2 -port 8002 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -data ./data-2
```

(Use `-admin <port>` if you need partition/heal for the harness.)

### Use the client

From Go:

```go
c, err := client.New("127.0.0.1:8000")
// ...
err = c.Put(ctx, key, value)
val, err := c.Get(ctx, key)
```

From the CLI:

```bash
./bin/client -addr 127.0.0.1:8000 key                    # generate a key (32 hex chars)
./bin/client -addr 127.0.0.1:8000 put <key-hex> "val"
./bin/client -addr 127.0.0.1:8000 get <key-hex>
```

### Performance

- **Read path:** Local hits are in-memory (sub-millisecond). Remote reads are forwarded to the key owner (one RPC). Persistent connections to peers avoid dial overhead.
- **Write path:** Each write is persisted (append + sync) before ack, so latency depends on storage. For low p99 latency use fast local storage (e.g. SSD) and moderate concurrency; high concurrency and slow sync will increase queueing and p99.

### TensorKV harness

Build the node binary, then from `tensorkv-harness`:

```bash
cd tensorkv-harness
go run ./cmd/harness --kv-store --nodes 3
```

Optionally set `KV_NODE_BIN` to the path of the node binary. The harness runs correctness phases (phantom reads, monotonic reads, causal consistency by session guarantees, durability) and a performance phase.

## Layout

- `interfaces/` — Key, Value, NodeID, Store, errors.
- `internal/version/` — Version vector type and operations.
- `internal/store/` — Causal multi-version store and persistence.
- `internal/node/` — Node process: RPC (Put, Get, Replicate), forwarding, replication, admin (partition/heal).
- `client/` — Client library implementing Store; maintains causal context per connection.
- `cmd/node/` — Main for the node process.
- `cmd/client/` — CLI for Get/Put.
- `tensorkv-harness/` — Evaluation harness; `skeleton/impl.go` adapts the kv-store to the harness Cluster/Store interfaces.
