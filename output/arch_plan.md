# Architecture Plan: High-Performance Distributed In-Memory Key-Value Store

## 1. Sharding

### Hash Function Choice
*   **Choice:** A cryptographically strong, non-cryptographic hash function like **MurmurHash3 (128-bit variant)** or **xxHash (64-bit variant, then combine two for 128-bit)**.
*   **Justification:**
    *   **128-bit Keys:** The keys are 128-bit random bytes. A 128-bit hash function ensures a uniform distribution across the hash space, which is crucial for evenly distributing keys across shards.
    *   **Performance:** MurmurHash3 and xxHash are known for their excellent performance and good distribution properties, which are vital for a high-throughput system.
    *   **Collision Resistance:** While not strictly cryptographic, these hashes offer sufficient collision resistance for sharding purposes, minimizing the chance of uneven shard distribution due to hash collisions.

### Shard Count Parameterization
*   **Choice:** The shard count (`N_SHARDS`) will be a configurable parameter, typically a power of 2 (e.g., 256, 512, 1024).
*   **Justification:**
    *   **Intra-node Parallelism:** Shards provide independent units of concurrency (separate lock domains, WAL streams, leader elections). A higher shard count allows for finer-grained parallelism within each node, maximizing CPU utilization.
    *   **Load Balancing:** A sufficiently large number of shards helps distribute the Zipfian hot keys more evenly across the physical nodes, reducing the likelihood of a single node becoming a bottleneck due to a "hot shard."
    *   **Static Sharding:** The contract specifies static sharding. A configurable `N_SHARDS` allows tuning for different cluster sizes and workloads without requiring dynamic resharding logic, simplifying the design.
    *   **Calculation:** `shard_id = hash(key) % N_SHARDS`.

### Placement Strategy Across 3 Nodes
*   **Choice:** Each shard will have a Primary, a Backup, and a Witness. These three roles for a given shard will be distributed across the three physical nodes in a round-robin or deterministic fashion to ensure high availability and load balancing.
*   **Justification:**
    *   **Fault Tolerance:** With 3 nodes and RF=2+witness, each node must participate in every shard in some role to maintain 1-fault tolerance. If a node fails, the remaining two nodes form a majority for all shards.
    *   **Load Balancing:** By distributing primary, backup, and witness roles across nodes, we ensure that no single node is solely responsible for serving all primary reads or handling all write replication for a large set of shards. For example:
        *   Node 1: Primary for Shard 0, Backup for Shard 1, Witness for Shard 2
        *   Node 2: Witness for Shard 0, Primary for Shard 1, Backup for Shard 2
        *   Node 3: Backup for Shard 0, Witness for Shard 1, Primary for Shard 2
    *   **Read Optimization:** This strategy ensures that read load (which goes to primaries) is distributed across all nodes.
    *   **Witness Role:** The witness, being lightweight, can reside on any node without significantly impacting its performance, further aiding load distribution.

## 2. Replication Protocol: Raft-like Protocol with Witness Optimization

*   **Choice:** A **leader-based Raft-like consensus protocol** is the optimal choice, adapted to incorporate the RF=2 data replicas + 1 lightweight witness model.
*   **Justification:**
    *   **Linearizability:** Raft inherently provides linearizability, which is a core requirement.
    *   **Read Optimization:** Leader-based protocols allow for single-round-trip reads from the leader (primary), which is critical for the 95% read workload.
    *   **Simplicity & Understandability:** Raft is known for its relative simplicity compared to Paxos, making implementation and reasoning about correctness easier.
    *   **Fault Tolerance:** Raft's quorum-based approach naturally supports 1-fault tolerance with 3 participants.

### Leader Election
*   **Mechanism:** Standard Raft leader election (heartbeats, randomized timeouts, requestVote RPCs).
*   **Justification:** Ensures a single primary (leader) for each shard, simplifying write coordination and providing a consistent point for reads.

### Log Replication
*   **Mechanism:**
    1.  Client sends `Put/Delete` request to the shard's Primary.
    2.  Primary appends the operation (key, value, operation type, term, index) to its local WAL.
    3.  Primary sends `AppendEntries` RPCs to the Backup and Witness.
        *   **To Backup:** Includes full log entry (key, value, operation type).
        *   **To Witness:** Includes only log entry metadata (key, operation type, term, index) – **crucially, NOT the full value**.
    4.  Backup and Witness persist the entry to their respective WALs and respond with success.
*   **Justification:**
    *   **Durability:** Ensures all operations are persisted before being committed.
    *   **Witness Optimization:** By sending only metadata to the witness, we significantly reduce network bandwidth, I/O, and storage requirements for the witness node. This is a key optimization for the RF=2+witness model, especially with potentially large values.
    *   **Asynchronous Replication:** While the primary waits for ACKs, the actual network transfer can be asynchronous, improving throughput.

### Commit Rules
*   **Mechanism:** An entry is considered **committed** when the Primary has successfully replicated it to a **majority of participants**. In our RF=2+witness model (3 participants total), this means the Primary plus **one successful ACK** from either the Backup or the Witness.
*   **Justification:**
    *   **Linearizability:** This majority-based commit rule ensures that once an operation is committed, it is durable and visible to subsequent reads, even if the Primary fails.
    *   **Performance:** Requiring only one additional ACK (from either Backup or Witness) makes the write path efficient. The witness ACK is particularly fast due to its lightweight nature.
    *   **Fault Tolerance:** Even if one participant fails, the remaining two can form a majority to commit new entries.

### Read Path Optimization
*   **Mechanism:**
    1.  Client sends `Get(key)` request to the shard's Primary.
    2.  Primary directly serves the read from its in-memory data store.
    3.  Primary responds with `FOUND(value)` or `NOT_FOUND`.
*   **Justification:**
    *   **Single Network Round-Trip:** This is the absolute fastest read path, crucial for the 95% read workload. No quorum reads are required, as the Primary is guaranteed to have the most up-to-date committed state (due to linearizability and the commit rules).
    *   **In-Memory Access:** Data is served directly from RAM, minimizing latency.
    *   **Backup Reads (Lease-based):** In scenarios where the Primary is overloaded or for specific read-scaling needs, the Backup can serve reads if it holds a valid "read lease" from the Primary, guaranteeing its data is sufficiently fresh. This adds complexity but can further offload the Primary. For the initial design, direct Primary reads are sufficient and simpler.

### Witness Participation in Quorum
*   **Mechanism:**
    *   **Write Quorum:** The Witness participates in the write quorum by receiving `AppendEntries` RPCs (containing only metadata), persisting them to its WAL, and sending an ACK to the Primary.
    *   **No Data Storage:** The Witness explicitly *does not* store the full key-value data in its in-memory store or on disk (beyond WAL metadata).
    *   **No Read Path:** The Witness never serves `Get` requests.
*   **Justification:**
    *   **Reduced Write Amplification:** By not storing full values, the Witness significantly reduces the storage and network overhead associated with replication, especially for large values. This makes the RF=2+witness model more efficient than RF=3 full replicas.
    *   **Fault Tolerance:** It still contributes to the majority quorum, ensuring 1-fault tolerance for writes and leader elections.
    *   **Cost-Effectiveness:** A witness node can be provisioned with less memory and disk than a full replica, reducing operational costs.

## 3. Storage Engine

*   **Choice:** A **Concurrent Hash Table (e.g., a Cuckoo Hash Table or a highly optimized `std::unordered_map`-like structure in C++/Go's `map`)** for the in-memory store, backed by a **Log-Structured Merge-tree (LSM-tree)** for the on-disk WAL and snapshots.
*   **Justification:**
    *   **In-Memory Hash Table for Hot Data:**
        *   **O(1) Average Time Complexity:** Hash tables provide near-constant time complexity for `Get`, `Put`, and `Delete` operations, which is paramount for the read-heavy, low-latency requirements.
        *   **Random Key Access:** Ideal for 128-bit random keys and Zipfian distribution, as it provides fast access regardless of key order.
        *   **Memory Efficiency:** Modern concurrent hash tables are highly optimized for memory usage.
    *   **LSM-tree for Persistence (WAL & Snapshots):**
        *   **Write Optimization:** LSM-trees are optimized for write-heavy workloads (sequential writes to disk), which is beneficial for the WAL. They buffer writes in memory (memtable) and flush them to immutable sorted string tables (SSTables) on disk.
        *   **Durability:** Provides a robust mechanism for persisting data.
        *   **Snapshots:** SSTables naturally form snapshots of the data at different points in time, simplifying recovery and background compaction.
        *   **Background Compaction:** Handles merging and cleaning up old data in the background, preventing read performance degradation over time.

## 4. Persistence

### Write-Ahead Log (WAL) Design
*   **Mechanism:**
    *   Each shard will have its own dedicated WAL.
    *   Entries are appended sequentially to the WAL file(s) on disk.
    *   Each WAL entry will contain: `(operation_type, key, value_size, value_data, timestamp, term, index)`.
    *   **Witness WAL:** The Witness's WAL will store `(operation_type, key, value_size=0, value_data=NULL, timestamp, term, index)`. It will only store metadata, not the actual value bytes.
    *   **Durability:** `fsync()` or `fdatasync()` will be called periodically (e.g., every N entries or every M milliseconds) to ensure data is flushed to stable storage.
*   **Justification:**
    *   **Durability:** Guarantees that all committed operations survive node crashes.
    *   **Recovery:** Upon restart, a node can rebuild its in-memory state by replaying its WAL.
    *   **Replication Source:** The WAL serves as the authoritative source for replicating state to new or recovering replicas.
    *   **Shard-Specific WALs:** Reduces contention and allows for independent recovery of shards.

### Snapshot Design
*   **Mechanism:**
    *   Periodically, each shard's Primary will take a snapshot of its current in-memory state.
    *   This involves iterating through the in-memory hash table and writing all key-value pairs to a new, immutable snapshot file on disk (e.g., an SSTable).
    *   The snapshot process should be non-blocking, allowing the store to continue serving requests. This can be achieved by using copy-on-write techniques or by taking a consistent view of the in-memory state.
    *   Once a snapshot is complete and persisted, older WAL segments preceding the snapshot can be safely truncated.
*   **Justification:**
    *   **Faster Recovery:** Snapshots significantly reduce the amount of WAL replay needed during recovery, speeding up node restarts.
    *   **State Transfer:** Snapshots are ideal for transferring the current state to a new or recovering replica, rather than replaying the entire WAL history.
    *   **Bounded WAL Size:** Prevents the WAL from growing indefinitely.

### Group Commit Strategy
*   **Mechanism:**
    *   Instead of `fsync()`ing every single write, the Primary will batch multiple pending WAL entries and `fsync()` them to disk together.
    *   A dedicated background goroutine/thread will be responsible for this, flushing either after a certain number of entries have accumulated or after a small timeout (e.g., 1-10ms).
*   **Justification:**
    *   **Reduced Disk I/O:** `fsync()` is an expensive operation. Grouping commits significantly reduces the number of disk flushes, improving overall write throughput and reducing average write latency.
    *   **Low-Millisecond Write Latency:** This strategy helps achieve the low-millisecond write latency target by amortizing the cost of `fsync()` across multiple operations.
    *   **Durability Trade-off:** There's a small window of data loss (entries in memory but not yet `fsync()`ed) if the node crashes between `fsync()` calls, but this is an acceptable trade-off for performance given the low-millisecond target and the single-datacenter deployment.

## 5. Concurrency Model

*   **Choice:** **Go's Goroutines and Channels** for inter-shard communication and asynchronous operations, combined with **fine-grained locking (mutexes) or lock-free data structures** for intra-shard concurrency.
*   **Justification:**
    *   **Goroutines & Channels:**
        *   **Lightweight Concurrency:** Goroutines are extremely lightweight, allowing for thousands or millions of concurrent operations without the overhead of traditional threads. This is ideal for handling many concurrent client requests and internal replication tasks.
        *   **Asynchronous Operations:** Channels provide a safe and idiomatic way to communicate between goroutines, facilitating asynchronous replication, background compaction, and other tasks without complex callback hell.
        *   **Scalability:** Go's runtime scheduler efficiently maps goroutines to OS threads, leveraging multi-core CPUs effectively.
    *   **Intra-Shard Concurrency:**
        *   **Shard-level Isolation:** Each shard operates largely independently. Operations within a shard can be serialized using a single mutex for the in-memory hash table, or more advanced lock-free data structures if profiling shows contention.
        *   **Key-level Locking (Optional):** For extremely high contention on specific hot keys within a shard, a finer-grained locking mechanism (e.g., a striping of mutexes based on key hash) could be considered, but a single shard-level lock is often sufficient for most in-memory hash table operations.
        *   **WAL Appending:** Appending to the WAL is typically a single-writer operation per shard, which can be protected by a mutex.

### How Concurrent Ops on Same Key are Serialized
*   **Mechanism:**
    1.  All `Put`, `Delete`, and `Get` requests for a given key are first routed to the Primary of the corresponding shard.
    2.  Within that shard's Primary, all operations are processed through a single logical pipeline.
    3.  For `Put` and `Delete` operations, they are appended to the shard's WAL and then applied to the in-memory store. The Raft-like protocol ensures that only one operation for a given log index is committed.
    4.  For `Get` operations, they directly query the in-memory store.
    5.  A **shard-level mutex** protects access to the in-memory hash table and the WAL append logic. This ensures that operations on the same key (and different keys within the same shard) are serialized at the point of application to the in-memory state and WAL.
*   **Justification:**
    *   **Linearizability:** Serializing operations at the shard primary ensures that the real-time order of operations is preserved, satisfying the linearizability requirement.
    *   **Simplicity:** A shard-level mutex is simpler to implement and reason about than complex lock-free structures, especially given the low-latency network and in-memory nature. Performance bottlenecks would be identified through profiling.

## 6. Network Layer

*   **Transport Protocol:**
    *   **Choice:** **TCP (Transmission Control Protocol)**.
    *   **Justification:**
        *   **Reliability:** TCP provides reliable, ordered, and error-checked delivery of data, which is essential for replication and ensuring data integrity.
        *   **Flow Control & Congestion Control:** Built-in mechanisms prevent network saturation and ensure fair usage of bandwidth.
        *   **Single Datacenter:** The <1ms latency within the datacenter minimizes the overhead typically associated with TCP's handshake and retransmissions.
*   **Serialization Format:**
    *   **Choice:** **Protocol Buffers (Protobuf)** or **FlatBuffers**.
    *   **Justification:**
        *   **Efficiency:** Both are highly efficient binary serialization formats, minimizing message size and parsing overhead. This is crucial for high-throughput replication and client-server communication.
        *   **Schema Evolution:** Both support schema evolution, allowing for backward and forward compatibility as the API or internal data structures change.
        *   **Language Agnostic:** Supports multiple programming languages, facilitating client library development in various languages if needed.
        *   **Protobuf vs. FlatBuffers:** Protobuf is generally easier to use for most cases. FlatBuffers offers zero-copy deserialization, which can be beneficial for very large values, but adds complexity. For 1KiB mean values, Protobuf is likely sufficient and simpler.
*   **Connection Management:**
    *   **Choice:** **Persistent TCP connections** between all nodes in the cluster (full mesh for internal communication) and between clients and cluster nodes.
    *   **Justification:**
        *   **Reduced Latency:** Eliminates the overhead of establishing new TCP connections for every request, which is critical for low-latency operations.
        *   **Resource Efficiency:** Reusing connections is more efficient in terms of CPU and memory resources than constantly opening and closing sockets.
        *   **Connection Pooling:** Clients will maintain a pool of connections to each cluster node to handle concurrent requests efficiently.

## 7. Client Library

*   **Routing Strategy:**
    *   **Mechanism:**
        1.  Client computes `hash(key) % N_SHARDS` to determine the target shard ID.
        2.  Client maintains a **shard map** (Shard ID -> Primary Node Address).
        3.  Client directs the `Get/Put/Delete` request to the Primary node for that shard.
*   **Justification:**
    *   **Direct Routing:** Eliminates the need for an intermediate proxy or load balancer for every request, reducing latency.
    *   **Client-Side Intelligence:** Offloads routing logic from the server, allowing servers to focus on data operations.

*   **Shard Discovery:**
    *   **Mechanism:**
        1.  Client connects to a well-known **bootstrap endpoint** (e.g., a configuration service or one of the cluster nodes).
        2.  The bootstrap endpoint provides the client with the initial shard map (Shard ID -> Primary Node Address).
        3.  The client periodically polls the cluster (or receives push updates) to refresh its shard map, especially after leader elections or node failures.
*   **Justification:**
    *   **Dynamic Updates:** Allows the client to adapt to changes in cluster topology (e.g., a new primary after a failover).
    *   **Decentralization:** Once the shard map is obtained, clients can directly communicate with the relevant nodes.

*   **Retry Logic:**
    *   **Mechanism:**
        1.  If a request to a Primary fails (e.g., connection error, timeout, "not leader" error), the client will:
            *   **Refresh its shard map** (to get the new Primary for that shard).
            *   **Retry the request** to the newly discovered Primary.
        2.  Implement **exponential backoff with jitter** for retries to avoid overwhelming the cluster during transient failures.
        3.  Limit the total number of retries and overall timeout for a request.
*   **Justification:**
    *   **Resilience:** Ensures the client can recover from transient network issues, node failures, and leader changes without immediately failing the operation.
    *   **User Experience:** Improves the perceived reliability of the system.

*   **Timeout Handling:**
    *   **Mechanism:**
        1.  Each client request will have a configurable **end-to-end timeout**.
        2.  Network operations (connection, send, receive) will also have shorter, internal timeouts.
        3.  If any timeout is exceeded, the client will abort the operation and return a `TIMEOUT` error.
*   **Justification:**
    *   **Prevent Hung Operations:** Prevents client requests from hanging indefinitely, improving application responsiveness.
    *   **Resource Management:** Releases client-side resources associated with timed-out requests.
    *   **Clear Error Semantics:** Provides clear feedback to the application about the operation's status.

This comprehensive architecture plan addresses all the requirements and constraints outlined in the workload contract, with a strong emphasis on optimizing for read latency and throughput while maintaining linearizability and fault tolerance.
