# Persistence Strategies for KV Stores

## WAL Design Patterns

### Single-Writer WAL
- One dedicated goroutine owns the WAL file descriptor. All writes are funneled through it via a channel.
- Eliminates locking on the write path — the channel provides natural serialization.
- The flusher goroutine: reads from channel, appends to buffer, triggers fsync on batch completion.
- Pattern: `walCh chan walEntry` → flusher goroutine → `pwrite()` + `fdatasync()`.

### Group Commit
- Multiple concurrent writes are grouped into a single fsync batch.
- Implementation: writers submit entries to a pending queue. The flusher drains the queue, writes all entries in one `pwrite()`, calls `fdatasync()`, then wakes all waiting writers.
- Optimal batch size: 16-128 entries or 1ms timeout, whichever comes first.
- Latency trade-off: individual write latency = queue wait time + write time + fsync time. But amortized fsync cost is divided by batch size.
- In Go, use `sync.Cond` for the wake-up notification.

### WAL Entry Format
```
[entry_length: uint32][crc32: uint32][log_index: uint64][op_type: uint8][key: []byte][value: []byte]
```
- Fixed header: 13 bytes. Total overhead per entry: 17 bytes (header + CRC).
- CRC32 covers the entire entry (log_index + op_type + key + value) for corruption detection.
- Length prefix enables sequential reading during recovery.

### WAL Segment Management
- Split WAL into fixed-size segments (64MB or 128MB).
- Pre-allocate segments with `fallocate()` to avoid filesystem fragmentation.
- Active segment: append-only writes. Sealed segments: immutable, candidates for truncation.
- Truncation: after a snapshot captures all entries up to index N, delete segments with max_index <= N.

## Snapshot Strategies

### Copy-on-Write Snapshots
- Use `fork()` to create a child process that writes the snapshot while the parent continues serving.
- The OS uses copy-on-write for memory pages — only modified pages are duplicated.
- In Go, `fork()` is problematic (goroutine scheduling). Alternative: use `os/exec` to spawn a snapshot helper process that receives the data via shared memory or pipe.

### Fork-Based Snapshots (Redis-style)
- `BGSAVE`: fork the process, child writes RDB file, parent continues serving.
- Memory overhead: depends on write rate during snapshot. For 4% writes, overhead is minimal.
- Duration: depends on dataset size. For 1B keys × 1KiB values = ~1TB. Full snapshot is expensive.
- Incremental snapshots: only write entries since the last snapshot. Requires tracking dirty pages or using the WAL as a delta log.

### Snapshot + WAL Hybrid
- Periodic snapshots (every 5 minutes or every 1M entries) capture the full state.
- WAL captures all writes between snapshots.
- Recovery: load latest snapshot, then replay WAL entries from the snapshot's log index forward.
- This bounds recovery time to: snapshot_load_time + replay_time_for_recent_entries.

## Recovery Protocols

### Single-Node Recovery
1. Load the latest snapshot (if any).
2. Open the WAL, find entries after the snapshot's log index.
3. Replay WAL entries in order, applying each to the in-memory store.
4. Resume normal operation.
- Recovery time is bounded by WAL size since last snapshot. With 5-minute snapshots and moderate write rate, this is typically <1 second.

### Peer-Assisted Recovery
- After crash-recovery, the node's local data may be behind the cluster.
- Fast catch-up: request missing WAL entries from the leader (entries between the node's last log index and the leader's current index).
- If the gap is too large (node was down for a long time), do a full state transfer from a peer.
- State transfer: snapshot from peer + WAL tail. Stream it over the network.

### Consistency During Recovery
- The recovering node must not serve reads until it has caught up to at least the committed index.
- Implementation: the node enters "recovering" state, refuses client requests, pulls missing entries from peers, applies them, then transitions to "follower" state.
- Lease interaction: the recovering node must not hold any read leases during recovery.

## Performance-Critical Decisions

### fdatasync vs fsync
- `fdatasync()`: flushes data but not metadata (faster, ~0.5ms on SSD).
- `fsync()`: flushes data and metadata (slower, ~1ms on SSD).
- Use `fdatasync()` for WAL writes (metadata changes are minimal — only file size).
- Use `fsync()` for snapshot files (ensure complete metadata).

### Direct I/O
- `O_DIRECT`: bypass the page cache, DMA directly to/from user-space buffers.
- Requires aligned buffers (typically 4096-byte alignment).
- Benefit: no page cache pollution, more predictable latency.
- Cost: must manage buffering yourself. In Go, use `memalign` via CGo or `syscall.Mmap`.

### Persistent Memory (PMEM)
- If available, use PMEM for the WAL. Writes are durable after `CLFLUSH` + `SFENCE` (~100ns).
- Eliminates the fsync bottleneck entirely.
- In Go, access via memory-mapped files on a DAX-enabled filesystem.
- Falls back gracefully to SSD if PMEM is not available.
