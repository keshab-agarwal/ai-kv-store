# Workload Contract: Distributed In-Memory Key-Value Store

## API
- **Get(key)** → FOUND(value) | NOT_FOUND | ERROR | TIMEOUT
- **Put(key, value)** → OK | ERROR | TIMEOUT
- **Delete(key)** → OK | ERROR | TIMEOUT

Single-key operations only. No scans, no range queries, no multi-key transactions.

## Key Characteristics
- **Key size**: 128-bit (16 bytes), opaque random bytes
- **Key distribution**: Random (uniformly distributed in the 128-bit space)
- **Access pattern**: Zipfian (highly skewed — a small fraction of keys receive most traffic)

## Value Characteristics
- **Maximum value size**: 1 MiB
- **Typical (mean) value size**: ~1 KiB
- **Value content**: Opaque bytes

## Workload Mix
| Operation | Proportion |
|-----------|-----------|
| Get       | 95%       |
| Put       | 4%        |
| Delete    | 1%        |

This is an extreme read-heavy workload. The system must be optimized for read latency above all else.

## Capacity
- **Minimum key capacity**: 1 billion distinct keys over the lifetime of the cluster
- **Working set**: determined by Zipfian distribution (hot set is much smaller than total)

## Deployment
- **Topology**: Single cluster, single datacenter, single availability zone
- **Network**: Intra-datacenter latency < 1ms, bandwidth > 10 Gbps
- **Node count**: 3 nodes (minimum for majority quorum with 1-fault tolerance)
- **Shard count**: Static, configured at cluster startup (no resharding). Shards provide intra-node parallelism (independent lock domains, WAL streams, leader elections) — with 3 nodes and RF=2+witness, every node participates in every shard in some role.

## Replication
- **Replication factor**: 2 data replicas + 1 lightweight witness per shard
  - **Primary**: Holds full data, serves reads and writes, replicates to backup
  - **Backup**: Holds full data, can serve reads (lease-based), takes over on primary failure
  - **Witness**: Stores only WAL metadata (operation log entries without full values). Participates in write quorum votes but never serves data reads. Enables majority quorum (2 of 3) with lower storage and replication overhead than a third full replica.
- **Write quorum**: Primary + 1 ACK (from backup or witness) = commit. The witness ACK is cheaper than a full replica ACK since it persists less data.
- **Read path**: Served from primary (or backup under lease). The witness is never on the read path.
- **Rationale**: For a 95% read workload, the write path should be as lean as possible. RF=2+witness achieves the same fault tolerance as RF=3 (majority of 3 participants) while reducing write amplification by ~33% on the witness node.

## Consistency
- **Linearizability** (Herlihy-Wing)
- A history H is linearizable if there exists an extension H' of H and a legal sequential history S such that:
  - L1: complete(H') is equivalent to S
  - L2: Real-time order in H is preserved in S (if op A completes before op B starts in H, then A precedes B in S)

## Fault Tolerance
- **Failure model**: Fail-stop (crash, no Byzantine faults)
- **Tolerance**: Exactly 1 node failure at a time
- **Quorum safety**: With 3 participants per shard (primary + backup + witness), any 1 failure leaves a majority of 2. The surviving majority can continue serving reads and writes.
- **Recovery**: When a failed node returns, it catches up from peers (WAL replay or state transfer) and resumes its role. Re-replication completes within ≤ 30 seconds.
- **Witness promotion**: If the backup fails, the witness can be promoted to a full backup by receiving a state transfer from the primary. If the primary fails, the backup becomes the new primary and the witness continues as witness (or a recovered node becomes the new backup).

## Persistence
- **Durability**: All committed writes must survive single-node crash + restart and full cluster restart
- **Mechanism**: Implementer's choice (WAL, snapshots, hybrid)
- **Constraint**: Persistence must not dominate the latency budget — common-case write latency must remain in the low-millisecond range

## Performance Targets
- **Primary goal**: Minimize read latency (p50, p95, p99)
- **Secondary goal**: Maximize read throughput (ops/sec)
- **Write latency**: Low-millisecond range (acceptable given durability requirements)
- **Network round-trips**: Minimize for both reads and writes

## Implementation Constraints
- **Language**: Implementer's choice (Go recommended for compatibility with Porcupine checker)
- **Consensus / replication protocol**: Implementer's choice — use whatever protocol best meets the performance and correctness requirements (known protocols like Raft, Paxos, chain replication, or novel designs are all acceptable)
- **Client library**: Must be built as part of the implementation, exposing Get/Put/Delete API
- **Concurrency model**: Implementer's choice, must be documented and justified
