# TensorKV Harness — Implementation Specification

This document is the ground-truth reference for anyone (human or LLM) implementing
a distributed KV store that is evaluated by this harness.

---

## 1. What You Must Implement

Copy `skeleton/impl.go` into your own package and fill in the eight stub methods:

| Method | Interface |
|---|---|
| `Put(ctx, key, value) error` | `interfaces.Store` |
| `Get(ctx, key) (Value, error)` | `interfaces.Store` |
| `Start(n int) error` | `interfaces.Cluster` |
| `Connect(nodeID) (Store, error)` | `interfaces.Cluster` |
| `NodeIDs() []NodeID` | `interfaces.Cluster` |
| `KillNode(nodeID) error` | `interfaces.Cluster` |
| `RestartNode(nodeID) error` | `interfaces.Cluster` |
| `PartitionNodes(a, b NodeID) error` | `interfaces.Cluster` |
| `HealPartition(a, b NodeID) error` | `interfaces.Cluster` |
| `Shutdown() error` | `interfaces.Cluster` |

Type definitions live in `interfaces/store.go`:

```go
type Key   [16]byte
type NodeID uint32
type Value  []byte   // valid range: 1 MB – 4 MB

var ErrKeyNotFound = errors.New("key not found")
var ErrValueTooSmall = errors.New("value smaller than 1MB")
var ErrValueTooLarge = errors.New("value larger than 4MB")
var ErrNodeDown     = errors.New("node unavailable")
```

---

## 2. Invariants

The harness verifies four invariants. Each is checked independently; failing one
does not suppress the others.

### 2.1 No Phantom Reads

**Definition:** Every value returned by a successful `Get` must have been written
by a successful `Put` for the same key at some earlier point in history.

**What triggers it:** Your storage layer returns data that was never written — this
indicates memory corruption, incorrect initialisation, or a missing key-space check.

**Harness error pattern:**
```
phantom read: client <N> read hash <H> for key <K> at time <T>,
but this hash was never written for this key
```

### 2.2 Monotonic Reads (per client, per key)

**Definition:** For a given client C and key K, if C reads version V1 at time T1
and then reads version V2 at time T2 > T1, then V2 must have been written at least
as recently as V1. A client must never observe time going backwards.

**What triggers it:** A client reads a stale replica after having already seen a
fresher one (e.g., load-balancing across replicas with inconsistent replication lag,
or reading from a node that missed some writes).

**Harness error pattern:**
```
monotonic read violation: client <N> read key <K>,
got hash <H2> (written at <T2>) after previously reading hash <H1> (written at <T1>)
— went backwards in time
```

### 2.3 Causal Consistency (per key, across all clients)

**Definition:** For each key, the full history of Put and Get operations must be
explainable by some total serial execution that is consistent with real time. If
`Put(V)` completes (returns nil) before `Get(K)` starts (is called), then the Get
must return V or a later version — never a value that predates V. This is
per-key linearizability.

The harness uses the [Porcupine](https://github.com/anishathalye/porcupine)
linearizability checker, which exhaustively searches all valid serial orderings.

**What triggers it:** A client reads a value from before a write that had already
completed system-wide — classic stale read in a replication protocol with weak
read consistency (e.g., reading from a follower that has not yet received the
latest write, or a split-brain situation).

**Harness error pattern:**
```
causal consistency violation on key <K> (HTML visualization: /tmp/porcupine-<K>-*.html)
  <N> operations on key <K> (chronological order):
    [client  0] Put hash=<H>  OK  latency=...
    [client  1] Get -> hash=<H2>  latency=...  ← ANOMALY: expected hash=<H> (last committed Put), got hash=<H2> (stale or phantom)
```

### 2.4 Durability (single-node failure)

**Definition:** After `Put` returns `nil`, the written value must survive the crash
of any single node. Both surviving nodes and the restarted node must return that
value (or a causally later one) on subsequent Gets.

**What triggers it:** A single-replica implementation (no replication), or a
primary-backup design where the backup is not written before the primary acks the
client.

**Harness error patterns:**
```
durability violation: key <K> was successfully Put with hash <H> at <T>,
but after node <N> crash, Get from node <M> returned error: <E>

durability violation: key <K> was successfully Put with hash <H> at <T>,
but after node <N> crash, Get from node <M> returned unexpected hash <H2>
```

---

## 3. Scoring Formula

```
score = phase1 + phase2 + phase3 + phase4

phase1 = 0.25  if HappyPath:       all of phantom-reads, monotonic-reads, causal-consistency pass
phase2 = 0.25  if CrashDuringLoad: all of phantom-reads, monotonic-reads, causal-consistency, durability pass
phase3 = 0.25  if NetworkPartition: all of phantom-reads, monotonic-reads, causal-consistency pass
phase4 = min(0.25,  throughput_ops_per_sec / 1000.0 * 0.25)
```

- **Maximum score: 1.0** — all correctness phases pass and throughput ≥ 1000 ops/s.
- All four phases are always run; no early exit. The report shows the running total
  after each phase so an LLM evolver can see exactly where points are lost.
- A score of 0.0 means Phase 1 failed (or the cluster failed to start).

### Score interpretation

| Score range | Meaning |
|---|---|
| 1.00 | Perfect: all invariants pass, throughput ≥ 1000 ops/s |
| [0.75, 1.00) | All correctness phases pass; throughput below target |
| [0.50, 0.75) | Two correctness phases pass; one is failing |
| [0.25, 0.50) | Only HappyPath passes; crash/partition handling broken |
| [0.00, 0.25) | HappyPath failing; basic correctness not met |

---

## 4. Evaluation Phases

| Phase | Scenario | Invariants checked |
|---|---|---|
| 1 — HappyPath | 30 s, 8 clients, no faults | phantom-reads, monotonic-reads, causal-consistency |
| 2 — CrashDuringLoad | Node 1 killed at 30%, restarted at 60% of 30 s | phantom-reads, monotonic-reads, causal-consistency, **durability** |
| 3 — NetworkPartition | Node 0 ↔ node 1 partitioned at 30%, healed at 60% | phantom-reads, monotonic-reads, causal-consistency |
| 4 — Performance | 60 s, 16 clients, 10 000 keys, 90/10 R/W, no faults | throughput (ops/s) |

**Workload parameters (Phases 1-3):**
- 8 concurrent clients, 1 000 keys, 90% reads / 10% writes
- Zipfian key distribution (θ = 0.99) — hot-key workload
- Value size: 1 MB (fixed)
- 3 s ramp-up (excluded from invariant checks and metrics)

**Benchmark parameters (Phase 4):**
- 16 concurrent clients, 10 000 keys, 90% reads / 10% writes
- Uniform key distribution
- Value size: 1 MB
- 5 s ramp-up, then 60 s measurement window

---

## 5. Getting Started

### Step 1 — Copy the skeleton

```
cp tensorkv-harness/skeleton/impl.go  myimpl/impl.go
```

Edit the `package` declaration at the top to match your package name.

### Step 2 — Implement the eight methods

A minimal correct implementation that passes all phases (but not the performance
benchmark at high throughput) is a **primary-backup** design:

1. `Start(n)`: launch n node goroutines (or processes), each with its own log and
   storage. Node 0 is the primary; nodes 1..n-1 are backups.
2. `Put`: forward to the primary, which writes to its log, synchronously replicates
   to all backups, then acks the client.
3. `Get`: read from any node (for causal consistency, read from the primary or
   implement read-your-writes via session tokens).
4. `KillNode`: stop the process; surviving nodes elect a new primary if needed.
5. `RestartNode`: replay the write-ahead log to recover state.
6. `PartitionNodes`: drop inter-node messages (block TCP connection or use a
   per-node blocklist in your message router).

### Step 3 — Wire your Cluster into the evaluator

```go
// In your main.go:
import (
    "fmt"
    "github.com/tensorkv/harness/evaluator"
    "mymodule/myimpl"
)

func main() {
    cluster := myimpl.NewCluster()
    score, report := evaluator.Evaluate(cluster, 3)
    fmt.Print(report)
    fmt.Printf("Final score: %.4f\n", score)
}
```

Or use the harness CLI (pass your implementation via a plugin mechanism):

```
go run ./cmd/harness --nodes=3
```

### Step 4 — Iterate on the [FAIL] lines

Each failure line has the form:

```
[FAIL] <Phase>/<invariant>: Invariant: <one-sentence rule>. Violation: <specific evidence>
```

Fix the most upstream failure first (HappyPath before CrashDuringLoad; phantom reads
before causal consistency — a phantom is a prerequisite violation for the causal
checker).

---

## 6. Key Design Constraints

| Constraint | Value |
|---|---|
| Minimum value size | 1 MB (`interfaces.MinValueSize`) |
| Maximum value size | 4 MB (`interfaces.MaxValueSize`) |
| Node IDs | Contiguous uint32, 0 .. n-1 |
| `Get` on unwritten key | Must return `interfaces.ErrKeyNotFound` |
| `Put`/`Get` on killed node | Must return `interfaces.ErrNodeDown` |
| Durability guarantee | Survive crash of **any single** node |
| Context cancellation | Must propagate `ctx.Err()` from `Put` and `Get` |
