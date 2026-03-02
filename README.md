# ai-kv-store

A distributed in-memory key-value store implementing the **Photon Quorum** protocol — an original primary-backup replication design with lease-based local reads. Written in Go.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Build](#build)
- [Running a Cluster](#running-a-cluster)
- [API Smoke Test](#api-smoke-test)
- [YCSB Benchmark](#ycsb-benchmark)
- [Correctness Testing](#correctness-testing)
- [TLA+ Model Checking](#tla-model-checking)
- [Full Evaluation Suite](#full-evaluation-suite)
- [Redis Baseline Comparison](#redis-baseline-comparison)
- [Client Library](#client-library)
- [Configuration Reference](#configuration-reference)
- [Performance Characteristics](#performance-characteristics)
- [Interpreting Failures](#interpreting-failures)

---

## Architecture

### Protocol: Photon Quorum

*Inspired by wave-particle duality and quantum state collapse.*

Writes are "photon pulses" emitted by the primary that travel to replicas at network speed. A write "collapses" into a definite committed state only when a quorum of replicas has absorbed (acknowledged) it. The primary maintains a "coherence window" (lease) — analogous to quantum decoherence time — during which reads can be served locally without any network round-trips.

**Key properties:**

- 3 replicas per shard (RF=3): 1 primary + 2 secondaries
- Writes commit on majority quorum (2 of 3), persisted to WAL with group commit
- Reads served from primary's in-memory state within a 40ms lease window (0 RTTs)
- Lease invariant ensures no two primaries can serve reads simultaneously:
  `lease_duration < min_election_timeout - max_rtt`
- Elections via randomized epoch increments (150–300ms); epoch monotonically increases
- Deduplication via `(clientID, seq)` pairs for at-most-once write semantics

**Persistence:** Append-only WAL with 500µs group commit batching and periodic snapshots. A crashed node recovers by loading its WAL/snapshot and catching up from the primary.

**Latency breakdown:**

| Path | Latency | Notes |
|------|---------|-------|
| Read | ~0.1ms | In-memory, within lease — zero network |
| Write | ~15–20ms | WAL `fdatasync` + 1 RTT to replica quorum |

---

## Project Structure

```
ai-kv-store/
├── kvstore/               KV store node implementation
│   ├── types.go           Status codes
│   ├── store.go           In-memory state machine
│   ├── wal.go             Write-ahead log (group commit)
│   ├── node.go            Photon Quorum protocol (election + replication)
│   ├── api.go             HTTP handler setup
│   └── cmd/kvnode/        Node binary
├── client/                Embeddable client library
│   └── client.go
├── ycsb/                  YCSB benchmark harness
│   ├── binding.go         DB interface
│   ├── zipfian.go         Zipfian distribution
│   ├── workload.go        Workload generator
│   ├── reporter.go        Statistics reporter
│   ├── cmd/ycsbrun/       Benchmark binary
│   ├── workloads/         .properties files
│   └── scripts/           plot_latency.py
├── correctness/           Correctness test harness
│   ├── history.go         History recording (JSONL)
│   ├── orchestrator.go    Node lifecycle management
│   ├── workload.go        Concurrent workload generator
│   ├── fault.go           Fault injection
│   ├── checker/           Porcupine linearizability checker
│   └── cmd/               harness and checker binaries
├── redis/                 Redis baseline benchmark
├── tlaplus/               TLA+ formal specification
│   ├── KVStore.tla
│   ├── KVStore.cfg
│   ├── KVStoreBug.cfg
│   └── tla2tools.jar
└── evaluation/            Automated evaluation harness
    └── evaluate.sh
```

---

## Prerequisites

- **Go 1.21+**
- **Java 8+** (for TLA+ model checking only)
- **Docker** (for Redis baseline comparison only)

---

## Build

Build all binaries at once:

```bash
go build -o bin/kvnode   ./kvstore/cmd/kvnode/
go build -o bin/ycsbrun  ./ycsb/cmd/ycsbrun/
go build -o bin/harness  ./correctness/cmd/harness/
go build -o bin/checker  ./correctness/cmd/checker/
```

---

## Running a Cluster

Start a 3-node cluster locally (1 shard, RF=3):

```bash
# Node 1 (becomes primary after election)
./bin/kvnode -id=1 -rpc=":17001" -client=":16001" \
  -peers="2=localhost:17002,3=localhost:17003" -data="./data-1"

# Node 2
./bin/kvnode -id=2 -rpc=":17002" -client=":16002" \
  -peers="1=localhost:17001,3=localhost:17003" -data="./data-2"

# Node 3
./bin/kvnode -id=3 -rpc=":17003" -client=":16003" \
  -peers="1=localhost:17001,2=localhost:17002" -data="./data-3"

# Wait ~500ms for leader election, then check status:
curl http://localhost:16001/api/status
```

---

## API Smoke Test

Keys must be 32 hex characters (16 bytes). Values are base64-encoded.

```bash
# Put a value
curl -s -X POST http://localhost:16001/api/put \
  -H "Content-Type: application/json" \
  -d '{"key":"deadbeefdeadbeefdeadbeefdeadbeef","value":"aGVsbG8=","client_id":"c1","client_seq":1}'

# Get the value
curl -s -X POST http://localhost:16001/api/get \
  -H "Content-Type: application/json" \
  -d '{"key":"deadbeefdeadbeefdeadbeefdeadbeef"}'

# Delete
curl -s -X POST http://localhost:16001/api/delete \
  -H "Content-Type: application/json" \
  -d '{"key":"deadbeefdeadbeefdeadbeefdeadbeef","client_id":"c1","client_seq":2}'
```

---

## YCSB Benchmark

The default workload is 95% read / 4% update / 1% delete with a Zipfian key distribution.

```bash
# Start the cluster first (see above), then run a benchmark:
./bin/ycsbrun \
  -nodes="localhost:16001,localhost:16002,localhost:16003" \
  -recordcount=10000 \
  -operationcount=50000 \
  -threads=8 \
  -phase=both \
  -outdir=ycsb-results
```

Scale to larger datasets:

```bash
# 1M keys
./bin/ycsbrun -nodes="localhost:16001,localhost:16002,localhost:16003" \
  -recordcount=1000000 -operationcount=1000000 -threads=16 -outdir=ycsb-1m

# 100M keys
./bin/ycsbrun -nodes="localhost:16001,localhost:16002,localhost:16003" \
  -recordcount=100000000 -operationcount=100000000 -threads=32 -outdir=ycsb-100m
```

Expected results (3-node local, 8 threads): `READ p99 < 1ms`, `UPDATE p99 < 30ms`, throughput ~9,000 ops/sec.

---

## Correctness Testing

The correctness harness records a full operation history and verifies it against the Porcupine linearizability checker.

### Porcupine self-test (no cluster needed)

```bash
./bin/checker -selftest
# Expected: SELF-TEST PASS
```

### No-fault run (15s)

```bash
./bin/harness \
  -bin=./bin/kvnode \
  -nodes=3 \
  -duration=15s \
  -workers=4 \
  -checktimeout=120s \
  -outdir=harness-nofault
# Expected: PASS (linearizable=true)
```

### Fault injection run (1-node crash + recovery, seed=42)

```bash
./bin/harness \
  -bin=./bin/kvnode \
  -nodes=3 \
  -duration=15s \
  -workers=4 \
  -faults \
  -seed=42 \
  -checktimeout=120s \
  -outdir=harness-fault
# Expected: PASS — fault injection at 25–75% of duration, recovery within 5–15s
```

### Deterministic reproduction

```bash
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=15s -workers=4 \
  -faults -seed=42 -checktimeout=120s -outdir=harness-repro
```

### Run checker on a saved history file

```bash
./bin/checker -history=harness-fault/history.jsonl -timeout=120s
```

### Node lifecycle (from `correctness/orchestrator.go`)

```go
// Start
exec.Command(binPath, "-id=N", "-rpc=:PORT", "-client=:PORT", "-peers=...", "-data=DIR")

// Graceful stop (SIGTERM)
cmd.Process.Signal(syscall.SIGTERM)

// Hard crash (SIGKILL)
cmd.Process.Kill()

// Recover: restart the same command (data directory is preserved)
```

---

## TLA+ Model Checking

```bash
cd tlaplus

# Normal mode — should find no errors:
java -jar tla2tools.jar -config KVStore.cfg KVStore.tla -workers 4

# Bug mode — should find a counterexample violating LinearizabilityInvariant:
java -jar tla2tools.jar -config KVStoreBug.cfg KVStore.tla -workers 4
```

---

## Full Evaluation Suite

```bash
chmod +x evaluation/evaluate.sh
./evaluation/evaluate.sh my-run
# Results written to: evaluation/runs/my-run/scores.json
```

---

## Redis Baseline Comparison

```bash
# Start Redis
docker compose -f redis/docker-compose.yml up -d

# Build and run Redis benchmark
go build -o bin/redisbench ./redis/cmd/redisbench/
./bin/redisbench -addrs=localhost:6379 -recordcount=10000 -operationcount=50000 \
  -threads=8 -phase=both -outdir=redis-results

# Compare KV store vs Redis
python3 redis/scripts/compare.py \
  ycsb-results/run_report.json \
  redis-results/run_report.json \
  comparison/
```

---

## Client Library

Import the embeddable Go client:

```go
import "ai-kv-store/client"

c := client.New(client.Config{
    Nodes:   []string{"localhost:16001", "localhost:16002", "localhost:16003"},
    Timeout: 5 * time.Second,
})

// Get: key must be 16 bytes
status, value, err := c.Get(keyBytes)

// Put: key = 16 bytes, value = up to 1 MiB
status, err := c.Put(keyBytes, valueBytes)

// Delete
status, err := c.Delete(keyBytes)
```

The client automatically routes requests, retries on transient failures, and discovers the current primary.

---

## Configuration Reference

| Binary | Flag | Description |
|--------|------|-------------|
| `kvnode` | `-id` | Node ID (integer) |
| `kvnode` | `-rpc` | Internal RPC listen address (e.g., `:17001`) |
| `kvnode` | `-client` | Client HTTP listen address (e.g., `:16001`) |
| `kvnode` | `-peers` | Comma-separated peer map (`id=host:port,...`) |
| `kvnode` | `-data` | Data directory for WAL and snapshots |
| `kvnode` | `-shards` | Number of shards |
| `ycsbrun` | `-nodes` | Comma-separated client addresses |
| `ycsbrun` | `-recordcount` | Number of keys to load |
| `ycsbrun` | `-operationcount` | Number of operations to run |
| `ycsbrun` | `-threads` | Client thread count |
| `ycsbrun` | `-outdir` | Output directory for results |
| `harness` | `-bin` | Path to `kvnode` binary |
| `harness` | `-nodes` | Number of nodes to start |
| `harness` | `-duration` | Test duration (e.g., `15s`) |
| `harness` | `-workers` | Concurrent worker count |
| `harness` | `-faults` | Enable fault injection |
| `harness` | `-seed` | RNG seed for deterministic fault scheduling |
| `harness` | `-checktimeout` | Timeout for Porcupine checker |
| `harness` | `-outdir` | Output directory for history and logs |
| `checker` | `-history` | Path to a `history.jsonl` file |
| `checker` | `-timeout` | Checker timeout |
| `checker` | `-selftest` | Run built-in self-test (no cluster needed) |

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Read p50 | ~0.07ms (lease: in-memory, 0 RTT) |
| Read p99 | ~0.35ms |
| Write p50 | ~15ms (WAL fsync + 1 RTT quorum) |
| Write p99 | ~28ms |
| Throughput | ~9,000 ops/sec (8 threads, 3-node local) |
| Fault recovery | ≤5s for new leader election, ≤30s for full re-replication |

---

## Interpreting Failures

### Porcupine FAIL

If the checker reports a non-linearizable history:

1. Open `harness-fault/history.jsonl` and locate the failing operations using `call_time_ns` and `return_time_ns` to identify the overlapping window.
2. Look for a read returning a value that was never written, or a stale value returned after a later write committed.
3. Check the node logs at `harness-fault/node-*.log` for unexpected leader changes during the failure window.

### TIMEOUT operations

The harness uses at-most-once semantics for writes via `(clientID, seq)` deduplication. On timeout, writes are retried up to 3 times with the same sequence number. If all retries time out, the operation is excluded from the Porcupine check (conservative: treated as not committed). This is safe because the persisted WAL is the ground truth.
