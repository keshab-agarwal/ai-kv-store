# Replication Protocol Optimization

## Quorum Mechanics (RF=2 + Witness)
- 3 participants per shard: primary (full data), backup (full data), witness (WAL metadata only).
- Write quorum = 2 of 3 participants (primary + 1 ACK from backup or witness).
- The witness ACK is cheaper than a full backup ACK — it persists only a compact WAL entry (op type + key + metadata, no full value copy). This reduces write amplification by ~33% on the witness node.
- Read quorum = 1 (reads served from primary, or backup under lease). The witness never serves reads.
- Fast-path: if both backup and witness ACK within a tight deadline (e.g., 500µs in single DC), all 3 participants have durability — strongest guarantee with no extra latency.

## Chain Replication
- Alternative to broadcast replication: leader → follower1 → follower2 (chain).
- Writes flow head-to-tail, reads served from tail (guaranteed up-to-date).
- Lower leader network bandwidth (sends to 1 node instead of N-1).
- Disadvantage: latency = sum of inter-node RTTs along the chain. In single DC (~0.1ms per hop), 2-hop chain adds ~0.2ms.
- Hybrid: use broadcast for commits, chain for bulk replication (re-replication after recovery).

## Speculative Execution
- Leader speculatively applies the write before quorum ACK.
- If the write is rolled back (leader loses election before commit), undo from the WAL.
- Benefit: read-after-write on the leader sees the new value immediately.
- Risk: must track speculative vs. committed state separately.

## Fast-Path Optimization (All Replicas Alive)
- When all replicas are alive and healthy, skip the full consensus protocol.
- Direct write: leader writes to local store + WAL, sends to followers, commits on first ACK.
- This reduces the commit latency to ~max(WAL_flush, 1_RTT).
- Fallback: when a replica is down, switch to the full protocol with proper quorum handling.
- Protocol switching must be safe — ensure no writes are lost during the transition.

## Batched Consensus
- Instead of one consensus round per write, batch multiple writes into a single log entry.
- The batch gets one sequence number and one quorum round.
- All operations in the batch are committed atomically.
- This is compatible with group commit — the WAL batch becomes the consensus batch.

## Leader Lease Optimization
- The leader holds a time-bounded lease (e.g., 5s in single DC).
- During the lease, the leader is guaranteed to be the only leader (no split-brain).
- Reads on the leader don't need any consensus round — just read from local state.
- Lease renewal: piggyback on heartbeats or replication messages.
- On lease expiry without renewal: leader steps down, forcing re-election.

## Witness Role Details
- The witness stores only WAL metadata entries: log index, op type, key hash, commit timestamp. No full values.
- Storage footprint: ~64 bytes per entry vs ~1KiB+ for a full replica entry. Orders of magnitude less I/O on writes.
- The witness participates in leader election votes — it knows which log entries are committed.
- Witness promotion: if the backup fails permanently, the witness can be promoted to a full backup by receiving a state snapshot from the primary. This is a background operation and does not block normal commits.
- Witness ACK latency: dominated by a small sequential write (~64 bytes), typically completing in <100µs on SSD with fdatasync, making it faster than a full replica ACK (~1KiB+ write).

## Adaptive Commit Protocol
- Monitor network conditions and adjust the commit protocol:
  - Low latency + all alive → fast path (leader + 1 ACK)
  - Suspected failure → switch to full quorum
  - Confirmed failure → enter degraded mode (2-of-2 quorum)
- Use a state machine with hysteresis to avoid flapping between modes.
