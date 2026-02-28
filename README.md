# Distributed In-Memory Key-Value Store

A linearizable distributed KV store with YCSB benchmarking, Porcupine-based correctness checking, and a TLA+ formal specification.

## Quick Start

```bash
# Build everything
go build -o bin/kvnode   ./kvstore/cmd/kvnode/
go build -o bin/ycsbrun  ./ycsb/cmd/ycsbrun/
go build -o bin/harness  ./correctness/cmd/harness/
go build -o bin/checker  ./correctness/cmd/checker/
```

### 1. Run a 3-node cluster

```bash
./bin/kvnode -id=1 -rpc=:7001 -client=:6001 -peers="2=localhost:7002,3=localhost:7003" &
./bin/kvnode -id=2 -rpc=:7002 -client=:6002 -peers="1=localhost:7001,3=localhost:7003" &
./bin/kvnode -id=3 -rpc=:7003 -client=:6003 -peers="1=localhost:7001,2=localhost:7002" &
```

Wait ~1 second for a coordinator to be elected, then:

```bash
# Check cluster status
curl http://localhost:6001/api/status

# Put a key (key = hex-encoded 16 bytes, value = base64-encoded)
curl -X POST http://localhost:6001/api/put \
  -d '{"key":"00112233445566778899aabbccddeeff","value":"aGVsbG8gd29ybGQ=","client_id":"me","client_seq":1}'

# Get it back
curl -X POST http://localhost:6001/api/get \
  -d '{"key":"00112233445566778899aabbccddeeff"}'

# Delete it
curl -X POST http://localhost:6001/api/delete \
  -d '{"key":"00112233445566778899aabbccddeeff","client_id":"me","client_seq":2}'

# Stop the cluster
kill %1 %2 %3
```

### 2. Run the YCSB benchmark

```bash
# Start cluster first (see above), then:

# Load 100K keys + run 95/4/1 read/update/delete workload
./bin/ycsbrun \
  -nodes=localhost:6001,localhost:6002,localhost:6003 \
  -recordcount=100000 \
  -operationcount=100000 \
  -threads=8

# Results are written to ycsb-results/
# - run_report.json      → throughput + p50/p95/p99/p999 latencies
# - run_latencies.csv    → per-operation latency data

# Plot latency histograms + CDFs (requires matplotlib)
pip install matplotlib numpy
python3 ycsb/scripts/plot_latency.py ycsb-results/run_latencies.csv
```

Scaling guide — just change `-recordcount` and `-operationcount`:

| Scale | recordcount | operationcount | Suggested threads |
|-------|-------------|----------------|-------------------|
| 1M    | 1000000     | 1000000        | 8                 |
| 10M   | 10000000    | 10000000       | 16                |
| 100M  | 100000000   | 100000000      | 32                |

### 3. Run the correctness harness

The harness automatically starts/stops a cluster, generates traffic, and verifies linearizability with Porcupine. No manual cluster setup needed.

```bash
# Validate the checker pipeline first (no cluster needed)
./bin/checker -selftest

# Run without faults (30s, 3 nodes, 10 concurrent workers)
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10

# Run WITH fault injection (crash + recover 1 node mid-test)
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10 -faults

# Reproduce an exact run using a seed
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10 -faults -seed=42

# Run the checker on a previously saved history
./bin/checker -history=test-run-20260228-120000/history.jsonl
```

Output goes to a timestamped directory containing:

| File | Contents |
|------|----------|
| `history.jsonl` | Full operation history (JSONL) |
| `summary.json` | Pass/fail + operation counters |
| `checker_result.txt` | Detailed Porcupine checker output |
| `logs/node*.log` | Per-node logs |
| `linearizability_violation.html` | Interactive visualization (on failure only) |

See [correctness/README.md](correctness/README.md) for the debugging guide, history format, and TIMEOUT handling strategy.

### 4. Run the TLA+ model checker

```bash
cd tlaplus/

# Download TLC (one time)
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

# Normal mode — should find no errors
java -jar tla2tools.jar -config KVStore.cfg KVStore.tla

# Bug mode — should produce a counterexample
java -jar tla2tools.jar -config KVStoreBug.cfg KVStore.tla
```

See [tlaplus/README.md](tlaplus/README.md) for how to read counterexamples and tune the model.

---

## Project Structure

| Folder | Task | What it does |
|--------|------|-------------|
| `kvstore/` | 1 | Distributed KV store: consensus, replication, HTTP API |
| `ycsb/` | 2 | YCSB-compatible benchmark with Zipfian key selection |
| `correctness/` | 3 | Correctness harness + Porcupine linearizability checking |
| `tlaplus/` | 4 | TLA+ formal specification with TLC model checking |

## Design Summary

The store uses a **coordinator-based quorum replication** protocol designed from scratch (not a named algorithm):

- **Election**: Randomized timeouts (300-500ms). Candidates request votes; majority wins. Votes granted only if the candidate's log is at least as up-to-date.
- **Writes**: Coordinator appends to its log, replicates to followers, commits on majority acknowledgment.
- **Reads**: Coordinator serves reads from local state under a lease renewed by heartbeat acknowledgments. Lease is shorter than the minimum election timeout, preventing stale reads from a deposed coordinator.
- **Recovery**: A crashed node restarts as an empty follower. The coordinator sends a state snapshot to catch it up.
- **Deduplication**: Write operations carry `(client_id, client_seq)`. The coordinator caches results so client retries after TIMEOUT are idempotent.

Full design details in [SPEC.md](SPEC.md).

## Go Client Library

```go
import (
    "ai-kv-store/kvstore"
    "ai-kv-store/kvstore/client"
)

c := client.New([]string{"localhost:8001", "localhost:8002", "localhost:8003"})

var key kvstore.Key
copy(key[:], someBytes)

result := c.Put(key, []byte("hello"))     // result.Status == kvstore.StatusOK
result  = c.Get(key)                       // result.Status == kvstore.StatusFound, result.Value
result  = c.Delete(key)                    // result.Status == kvstore.StatusOK
```

The client automatically retries across nodes if one is down.

## Requirements

- Go 1.20+
- Python 3 + matplotlib/numpy (optional, for latency plots)
- Java 11+ (optional, for TLC model checker)
