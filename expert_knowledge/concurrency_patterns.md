# Concurrency Patterns for KV Stores

## Epoch-Based Reclamation
- Divide time into epochs. Each reader announces which epoch it's in (atomic load/store).
- Writers defer object deallocation until all readers have exited the epoch in which the object was logically deleted.
- In Go: maintain a global epoch counter and per-goroutine epoch announcement. Increment the global epoch periodically (e.g., every 1ms). A retired object is safe to free when all goroutines have advanced past its retirement epoch.
- Advantage over RWMutex: readers never block, no cache-line bouncing on the mutex.

## Read-Copy-Update (RCU)
- For read-dominated workloads (95% reads), RCU is ideal.
- Read path: load the current pointer atomically, use it without locking.
- Write path: copy the data structure (or relevant portion), modify the copy, atomically swap the pointer.
- Grace period: old version is freed after all in-progress readers are done.
- In Go, implement with `atomic.Pointer[T]` (Go 1.19+) and a quiescent state tracker.

## Per-Shard Striping
- Each shard has its own lock (or lock-free structure). Operations on different shards never contend.
- Within a shard, use a striped lock (e.g., 256 lock stripes for 128-bit key space, selected by key hash).
- This reduces lock contention from O(total_ops) to O(ops_per_stripe).
- For Zipfian workloads, hot keys may concentrate on a few stripes — consider adaptive striping or a dedicated hot-key cache.

## io_uring Event Loop
- Linux io_uring provides kernel-bypassed async I/O for both network and disk.
- Submit SQEs (submission queue entries) for: accept, read, write, fsync.
- Completion events arrive on the CQ (completion queue) — poll in a tight loop.
- In Go, using io_uring requires CGo or a pure-Go library (e.g., `iceber/iouring-go`).
- Benefit: eliminates syscall overhead for high-throughput I/O operations.
- Trade-off: added complexity, CGo overhead may negate benefits for small operations.

## Coroutine-Based Concurrency
- Go goroutines are lightweight green threads — use them liberally.
- Pattern: one goroutine per client connection (reader), one per shard (applier), one for WAL flushing.
- Avoid goroutine-per-operation for write path (use channels to batch).
- Use `runtime.GOMAXPROCS` to match the number of physical cores.
- Consider `runtime.LockOSThread()` for the WAL flusher and network poller to reduce context switching.

## Lock-Free Hash Table
- Implementation using atomic CAS (compare-and-swap):
  1. Each bucket is an atomic pointer to a chain of entries.
  2. Insert: create new entry, CAS it as the new head of the chain.
  3. Lookup: traverse the chain (no locking needed — entries are never modified, only replaced).
  4. Delete: mark entry as deleted (tombstone), physical removal during compaction.
- Robin Hood hashing with atomic operations for open-addressing variant.
- In Go, use `sync/atomic` package with `unsafe.Pointer` for atomic pointer operations.

## Avoiding False Sharing
- Ensure frequently accessed fields are on different cache lines (64 bytes apart).
- In Go, use padding: `_ [64]byte` between fields that different goroutines access.
- Critical spots: per-shard counters, per-connection state, global epoch counter.

## Backpressure and Flow Control
- If the write pipeline (WAL + replication) is saturated, apply backpressure to clients.
- Use bounded channels: when the channel is full, the client goroutine blocks.
- Monitor queue depths and expose them as metrics.
- Adaptive: reduce batch timeout under load (flush more frequently, smaller batches) to keep latency bounded.

## Work Stealing
- If shard load is unbalanced (Zipfian causes hot shards), use work-stealing across shard goroutines.
- Alternatively, sub-shard the hot shard: split the key space of a hot shard into micro-shards processed by multiple goroutines.
- Monitor per-shard latency and trigger rebalancing when a shard's p99 exceeds 2x the median.
