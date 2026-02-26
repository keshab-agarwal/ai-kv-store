# Distributed Key-Value Store Specification

## What This System Is

A distributed key-value store that runs on 3 nodes, provides causal consistency, and survives single node failures.

---

## System Parameters

| Parameter | Value |
|-----------|-------|
| Nodes | 3 (fixed, known at startup) |
| Key | `[16]byte` (128-bit), randomly generated |
| Value | `[]byte`, 1 KB average, 1 MB max |
| Consistency model | Causal consistency |
| Fault tolerance | Any single node may fail at any time |
| Persistence | Data must survive process restart |

---

## Interfaces

Your implementation must satisfy these Go interfaces. 

```go
package interfaces

import (
    "context"
    "errors"
)

type Key [16]byte
type Value []byte
type NodeID uint32

var (
    ErrKeyNotFound   = errors.New("key not found")
    ErrValueTooLarge = errors.New("value larger than 1MB")
    ErrNodeDown      = errors.New("node unavailable")
)

type Store interface {
    Put(ctx context.Context, key Key, value Value) error
    Get(ctx context.Context, key Key) (Value, error)
}

```

---

## Behavioral Contract

### Put(key, value) → error

Stores a value for the given key. Overwrites any existing value.

**Preconditions:**
- `size of value <= 1 MiB`, otherwise return the appropriate error

**Postconditions:**
- The value is durable (survives restart of the node that processed it)
- The value will eventually be visible to all clients on all nodes

### Get(key) → (value, error)

Retrieves the value for the given key.

**Postconditions:**
- The returned value was written by a prior successful Put for this key (no phantom reads)
- The returned value is byte-for-byte identical to what was written (no corruption)
- If the key was never written, returns `ErrKeyNotFound`

---

## Causal Consistency Definition

The system provides causal consistency as a system-wide property. Clients do not need to manage sessions, tokens, or clocks. The system handles all causal tracking internally.

**Definition:** If operation A causally precedes operation B, then any node that has seen B must also have seen A. The causal precedence relation is:

- **Session order:** If a client performs operation A, then operation B, then A causally precedes B.
- **Reads-from:** If a Put writes value V, and a subsequent Get returns V, then the Put causally precedes the Get.
- **Transitivity:** If A causally precedes B, and B causally precedes C, then A causally precedes C.

### The four session guarantees (all must hold):

**1. Read your writes:** If a client writes a value, its subsequent reads of the same key will return that value or a causally later one.

**2. Monotonic reads:** If a client reads a value V1 from a key, its subsequent reads of the same key will never return a value causally older than V1.

**3. Monotonic writes:** If a client performs write A then write B, every node that has applied B has also applied A.

**4. Writes follow reads:** If a client reads key X and obtains value V, then writes key Y, then any client that reads the new value of Y must also be able to read V (or a causally later version of X).

### Example

```
Client 1:  Put(x, "hello")    // write W1
Client 1:  Put(y, "world")    // write W2, causally after W1

Client 2:  Get(y) → "world"   // Client 2 observes W2
Client 2:  Get(x) → ???       // must return "hello" or newer, never ErrKeyNotFound
```

W2 causally depends on W1 (same client, sequential). Client 2 observed W2. Therefore Client 2 must also observe W1.

### What causal consistency does NOT require

- No total order on concurrent writes. If two clients independently write to the same key without any causal relationship between them, different clients may observe them in different orders.
- No real-time ordering. If Client 1 writes X and Client 2 writes Y a millisecond later, but there is no causal link between them, the system may order them in either direction.

---

## Fault Tolerance

**Fault model:** Fail-stop. A failed node halts completely — no corrupted data, no partial responses. At most one node fails at a time.

**Requirements under failure:**

1. If a key's data has been replicated to at least one other node before failure, that data must be readable from a surviving node.

2. Keys whose responsible node is still alive must remain fully available (reads and writes) regardless of which other node has failed.

3. When a failed node restarts, it must recover its persisted data and must not serve values that violate causal ordering.

4. The system is not required to maintain write availability for all keys during a failure. Keys affected by the failure may return `ErrNodeDown` for writes until the failed node recovers.

---

## Concurrency

Multiple goroutines will call Put and Get simultaneously on any number of Store connections.

- No data races (`go test -race` must pass)
- Concurrent operations on different keys may execute in parallel
- The system must not deadlock under any combination of concurrent operations and failures

---

## Performance Targets

The system will be benchmarked against Redis Cluster on the same workload. These are directional targets, not hard pass/fail criteria.

**Workload:** 90% reads, 10% writes. Zipfian key distribution (theta=0.99). 1000 keys. 512-byte values. 16 concurrent clients.

| Metric | Target |
|--------|--------|
| Read latency p50 | < 1 ms |
| Read latency p99 | < 5 ms |
| Write latency p50 | < 2 ms |
| Write latency p99 | < 10 ms |
| Read throughput | > 50,000 ops/sec |
| Write throughput | > 20,000 ops/sec |

---

## Deliverables

A distributed key-value store that can be started, connected to, and queried by external clients.

A command or binary to start each node in the cluster, given a node ID, port, and the addresses of the other nodes
A client library that implements the Store interface above, allowing callers to connect to any node and perform Put/Get operations
A way to start a full 3-node cluster locally for testing

---

## Constraints

- Language: Go
- No external databases (no Redis, etcd, PostgreSQL, SQLite)
- No external consensus libraries (no Raft, Paxos implementations)
- The cluster is always exactly 3 nodes — no dynamic membership
- Single-key operations only — no transactions, no multi-key atomicity, no scans