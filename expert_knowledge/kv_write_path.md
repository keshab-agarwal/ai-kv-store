# KV Store Write Path Optimization

## Group Commit (WAL Batching)
- Buffer multiple write operations and flush them to the WAL in a single fsync.
- Typical strategy: accumulate writes for up to 1ms or until N writes are buffered, then flush.
- All buffered operations share a single fsync cost, amortizing the ~1ms disk flush latency.
- In Go, use a channel or ring buffer where writers submit entries and a dedicated flusher goroutine batches and syncs.
- Trade-off: higher batch size = better throughput but higher tail latency for individual writes.

## WAL Design
- Single-writer WAL: one goroutine owns the WAL file, eliminating lock contention on writes.
- Format: length-prefixed entries with CRC32 checksums. Each entry: [length:4][crc:4][data:N].
- Pre-allocate WAL segments (e.g., 64MB files) to avoid filesystem metadata operations during writes.
- Use `O_DIRECT` + aligned buffers to bypass the page cache (reduces memory pressure, more predictable latency).
- WAL truncation: after a snapshot, truncate the WAL up to the snapshot's log index.

## Pipelining Replication
- Don't wait for the previous replication batch to be acknowledged before sending the next.
- Pipeline: send batch N, immediately start accumulating batch N+1. When batch N is ack'd, start sending N+1.
- This overlaps network latency with new write accumulation.
- In single-datacenter (<1ms RTT), pipelining reduces effective replication latency from 2*RTT to ~1*RTT.

## Write-Behind Caching
- Apply writes to the in-memory hash table immediately upon leader commit (before WAL fsync if the replication protocol ensures durability through quorum WAL writes).
- The read path sees the latest committed value without waiting for local disk sync.
- Risk: if the leader crashes before local sync, the value is still durable on quorum peers.

## Compaction Strategies
- Periodic background compaction merges WAL entries for the same key.
- For a workload with 4% Put and 1% Delete, compaction is infrequent but important for space reclamation.
- Use a copy-on-write approach: build new compacted segments while the old ones are still readable.
- Schedule compaction during low-traffic periods (if detectable) or rate-limit to avoid impacting latency.

## Avoiding Write Amplification
- For an in-memory store, the main write amplification is WAL + replication.
- Minimize serialization overhead: use a fixed-size header with raw byte payloads rather than encoding values.
- Batch replication messages: send one network message per batch of WAL entries rather than one per entry.

## Delete Optimization
- Tombstone-based deletes: mark the key as deleted in-memory, write a delete record to WAL.
- Tombstones are compacted away during background compaction.
- For Porcupine correctness, ensure the delete tombstone has a commit timestamp and is linearizable.
