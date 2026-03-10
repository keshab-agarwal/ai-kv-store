# Network Optimization for KV Stores

## Zero-Copy Networking
- Avoid copying data between user-space buffers and the kernel's socket buffer.
- In Go, `net.Conn.Write(buf)` already uses sendmsg under the hood, but the data still passes through the kernel.
- For true zero-copy: use `splice()` or `sendfile()` via CGo to transfer data from a file (WAL) directly to a socket.
- For in-memory data: `writev()` (scatter-gather I/O) to send header + value from separate buffers without concatenating.

## Connection Pooling
- The client library should maintain a pool of persistent TCP connections to each node.
- Pool size: 1-4 connections per node per client instance (more adds diminishing returns).
- Multiplexing: send multiple requests on the same connection with request IDs for out-of-order responses.
- Connection health: periodic heartbeat pings, automatic reconnection on failure.
- In Go, use `sync.Pool` or a custom pool with `chan *net.TCPConn`.

## Request Batching
- Client-side: accumulate requests for a short window (100-500µs) and send as a batch.
- Server-side: process the batch in one lock acquisition, respond with a batch.
- Trade-off: batching adds latency (up to the batch window) but significantly improves throughput.
- Adaptive batching: increase batch window under high load, decrease under low load.
- For 95% reads, batch reads separately from writes (reads can be processed without consensus).

## Kernel Bypass (DPDK/XDP)
- DPDK: bypass the kernel network stack entirely for maximum throughput. Requires dedicated NICs and huge pages. Complex to integrate with Go.
- XDP (eXpress Data Path): eBPF programs attached to the NIC driver. Lower overhead than full DPDK, works with standard sockets.
- AF_XDP: user-space socket that receives packets from XDP. Can be used with Go via CGo.
- For a single-datacenter KV store, kernel bypass reduces per-packet overhead from ~5µs to ~1µs.
- Recommendation: start without kernel bypass, add it only if network processing is the proven bottleneck.

## Serialization Format Comparison
- **Protocol Buffers (protobuf)**: Schema-based, compact, but requires marshaling/unmarshaling (allocation + copy). Good interop. ~500ns per small message.
- **FlatBuffers**: Zero-copy deserialization (access fields directly from the buffer). Schema-based. No allocation on read. ~50ns access time. Larger wire size than protobuf.
- **MessagePack**: Schema-less, compact binary format. ~200ns per small message. Good for dynamic data.
- **Custom binary format**: Fixed-size header + raw bytes payload. Zero allocation, zero copy. ~10ns to parse header. Best performance but no interop.
- **Recommendation for this workload**: Custom binary format for the internal protocol (replication, client-server). Use a 16-byte fixed header: [request_id:8][op_type:1][key_len:2][value_len:4][flags:1]. Key and value follow as raw bytes.

## TCP Tuning
- Disable Nagle's algorithm: `TCP_NODELAY = 1`. Critical for latency-sensitive protocols.
- TCP keepalive: enable with short intervals (10s) for connection health detection.
- Socket buffer sizes: increase `SO_RCVBUF` and `SO_SNDBUF` for replication connections (1-4MB).
- In single DC with <1ms RTT, the bandwidth-delay product is small — default buffer sizes are usually sufficient for client connections.

## Request Pipelining
- Send multiple requests without waiting for responses (HTTP/2 style pipelining).
- The server processes requests in order and sends responses in order (or out-of-order with request IDs).
- This hides the round-trip latency for sequential operations.
- In Go, use separate read and write goroutines per connection.

## UDP for Reads
- For simple Get operations, consider UDP for lower per-request overhead (no connection setup, no TCP head-of-line blocking).
- Challenge: UDP doesn't guarantee delivery — implement application-level ACKs with timeout+retry.
- Best for: read-heavy workloads where the value fits in a single UDP datagram (<=1400 bytes for safety).
- Not suitable for writes (need reliability guarantees for the commit protocol).

## gRPC vs Raw TCP
- gRPC: built on HTTP/2, has multiplexing, streaming, and code generation. Higher overhead (~50µs per call) due to HTTP/2 framing and protobuf serialization.
- Raw TCP: minimal framing overhead, custom serialization. Lower latency (~5-10µs per call).
- Hybrid: use raw TCP for the hot path (Get/Put/Delete), gRPC for control plane (cluster management, health checks).
