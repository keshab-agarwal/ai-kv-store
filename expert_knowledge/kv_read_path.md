# KV Store Read Path Optimization

## Zero-Copy Reads
- Serve reads directly from the in-memory hash table without copying the value to an intermediate buffer.
- In Go, return a `[]byte` slice backed by the same underlying array as the stored value. The caller must not mutate it (document this contract) or copy-on-read only when the caller needs ownership.
- Avoid `append()` on returned slices — it may trigger reallocation and copy.
- For network serialization, write directly from the stored slice to the socket buffer (avoid marshaling into a protobuf `bytes` field which forces a copy).

## Lock-Free Hash Tables
- Use a concurrent hash table with sharded/striped locks (one lock per bucket group) rather than a global RWMutex.
- For a 95% read workload, consider `sync.Map` (optimized for read-heavy workloads with stable keys) or a custom implementation with atomic pointer swaps.
- Epoch-based reclamation: readers enter an epoch, writers defer cleanup until all readers in the previous epoch have exited. This allows reads without holding any lock.
- Alternative: RCU (read-copy-update) pattern — readers access the current version locklessly, writers create a new version and atomically swap the pointer.

## NUMA Awareness
- Pin shard goroutines to specific CPU cores using `runtime.LockOSThread()` + OS-level CPU affinity (`sched_setaffinity` on Linux).
- Allocate shard memory on the local NUMA node. In Go, this requires CGo or careful goroutine scheduling.
- Keep hot data (frequently accessed keys under Zipfian) in L1/L2 cache by co-locating the hash bucket, key, and value metadata in cache-line-aligned structures.

## Lease-Based Reads
- The shard leader grants time-bounded leases to followers. During a valid lease, followers can serve reads locally without contacting the leader.
- Lease duration trade-off: longer leases reduce leader load but increase staleness window during leader failover. For single-datacenter (<1ms RTT), 1-5 second leases are appropriate.
- On leader change, all outstanding leases must expire before the new leader can serve writes (or the new leader must explicitly revoke leases).
- Optimization: for the common case (no failures), the leader can serve reads from its own state without any consensus round-trip.

## Read-Your-Writes
- Track the last committed log index per client session.
- On read, if the serving node's applied index is >= the client's last write index, serve locally.
- Otherwise, forward to the leader or wait for replication to catch up.
- This is critical for correctness when using lease-based reads.

## Cache-Line Optimization
- Hash table entries should be 64 bytes (one cache line) or a multiple thereof.
- Store key hash (8 bytes), key pointer (8 bytes), value pointer (8 bytes), value length (4 bytes), and metadata (4 bytes) in the first cache line.
- For Zipfian workloads, the hot set is small — ensure it fits in L2 cache.
- Use open addressing with linear probing for better cache behavior than chaining.

## Batch Read Optimization
- If the client library supports multi-get, batch multiple reads into a single network round-trip.
- On the server side, process batched reads without acquiring the lock multiple times — lock once, read all, unlock.
