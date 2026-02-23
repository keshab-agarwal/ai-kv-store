// Package skeleton is the starting point for implementing a distributed KV store
// that passes the TensorKV harness evaluation.
//
// # Invariants the harness checks
//
//  1. NO PHANTOM READS
//     Every value returned by Get must have been Put for that key at some earlier
//     point in history. Your storage layer must never fabricate or corrupt data.
//
//  2. MONOTONIC READS (per client, per key)
//     If client C reads key K and observes version V1, any subsequent read of K
//     by C must return V1 or a version written AFTER V1. Never go backwards.
//
//  3. CAUSAL CONSISTENCY (per key, across all clients)
//     The history of operations on each key must be explainable by some total
//     serial execution consistent with real time. If Put(V) completes before
//     Get(K) starts, the Get must return V or a later version — not a stale one.
//     The harness uses Porcupine (exhaustive linearizability checker) to verify.
//
//  4. DURABILITY (single-node failure)
//     After Put returns nil, the written value must survive the crash of any
//     single node. Surviving nodes and the restarted node must all return that
//     value (or a causally later one) on subsequent Gets.
//
// # Two pieces to write
//
// THIS FILE is the harness adapter. It does NOT contain the KV logic. It:
//   - launches server processes (Start, RestartNode, Shutdown)
//   - kills server processes (KillNode)
//   - dials gRPC connections to those processes (Connect → nodeStore.Put/Get)
//   - tells processes to block/unblock peer traffic (PartitionNodes, HealPartition)
//
// THE SERVER BINARY (e.g. cmd/kvserver/main.go) is separate code you also write.
// It implements: storage engine, replication protocol (Raft / primary-backup /
// chain replication), write-ahead log, and an admin HTTP endpoint for partition
// injection. Start() launches this binary; the harness never calls it directly.
//
// # Recommended server binary interface
//
//	./kvserver \
//	    --id    <nodeID>           \   # integer, 0-based
//	    --port  <grpcPort>         \   # data-plane gRPC port
//	    --admin <httpAdminPort>    \   # control-plane HTTP port (for PartitionNodes)
//	    --data  <dir>              \   # durable storage directory (WAL, snapshots)
//	    --peers <addr,addr,...>        # gRPC addresses of all OTHER nodes
//
// The server exposes two gRPC methods matching your .proto:
//
//	rpc Put(PutRequest)  returns (PutResponse);
//	rpc Get(GetRequest)  returns (GetResponse);
//
// And two HTTP admin endpoints:
//
//	POST /partition?peer=<addr>   — start dropping traffic to/from peer
//	POST /heal?peer=<addr>        — stop dropping traffic to/from peer
package skeleton

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"

	"github.com/tensorkv/harness/interfaces"
)

// ---------------------------------------------------------------------------
// nodeStore — gRPC client adapter (implements interfaces.Store)
// ---------------------------------------------------------------------------

// nodeStore routes Put/Get RPCs to a single node's gRPC server.
//
// Once you have run protoc to generate Go stubs from your .proto file, replace
// the placeholder addr field with the generated client:
//
//	import (
//	    kvpb "mymodule/gen/tensorkv/v1"
//	    "google.golang.org/grpc"
//	    "google.golang.org/grpc/credentials/insecure"
//	)
//
//	type nodeStore struct {
//	    conn   *grpc.ClientConn
//	    client kvpb.TensorKVClient
//	}
type nodeStore struct {
	addr   string // "localhost:<grpcPort>" — placeholder until you add the gRPC client
	nodeID interfaces.NodeID
}

// Put sends a Put RPC to the node.
//
// Before returning nil, the server must have replicated the write to a quorum
// so that it survives any single-node crash.
//
// TODO: replace the stub body with:
//
//	_, err := s.client.Put(ctx, &kvpb.PutRequest{Key: key[:], Value: value})
//	return grpcToHarnessErr(err)
func (s *nodeStore) Put(ctx context.Context, key interfaces.Key, value interfaces.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(value) < interfaces.MinValueSize {
		return interfaces.ErrValueTooSmall
	}
	if len(value) > interfaces.MaxValueSize {
		return interfaces.ErrValueTooLarge
	}
	// TODO: call s.client.Put, then return grpcToHarnessErr(err).
	return errors.New("not implemented")
}

// Get sends a Get RPC to the node.
//
// Must return interfaces.ErrKeyNotFound if the key was never written.
// Must return interfaces.ErrNodeDown if the node is unavailable.
//
// TODO: replace the stub body with:
//
//	resp, err := s.client.Get(ctx, &kvpb.GetRequest{Key: key[:]})
//	if err != nil { return nil, grpcToHarnessErr(err) }
//	return resp.Value, nil
func (s *nodeStore) Get(ctx context.Context, key interfaces.Key) (interfaces.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// TODO: call s.client.Get, then map errors with grpcToHarnessErr.
	return nil, errors.New("not implemented")
}

// grpcToHarnessErr maps gRPC status codes to harness sentinel errors.
// Copy this into your implementation once you import the grpc/status package.
//
//	import (
//	    "google.golang.org/grpc/codes"
//	    "google.golang.org/grpc/status"
//	)
//
//	func grpcToHarnessErr(err error) error {
//	    if err == nil { return nil }
//	    switch status.Code(err) {
//	    case codes.NotFound:    return interfaces.ErrKeyNotFound
//	    case codes.Unavailable: return interfaces.ErrNodeDown
//	    default:                return err
//	    }
//	}

// ---------------------------------------------------------------------------
// Cluster — node lifecycle + fault injection (implements interfaces.Cluster)
// ---------------------------------------------------------------------------

// Cluster launches and controls n server processes via os/exec.
// Each entry in cmds corresponds to one node process; ports holds the gRPC
// listen port for the same node by index.
//
// The harness calls methods in this order:
//
//	Start(n) → Connect(id) × many → [KillNode / RestartNode / PartitionNodes / HealPartition] → Shutdown
type Cluster struct {
	n          int
	ports      []int                // gRPC port for node i
	adminPorts []int                // admin HTTP port for node i (partition control)
	cmds       []*exec.Cmd          // server process for node i
	nodeIDs    []interfaces.NodeID
}

// NewCluster creates an uninitialised Cluster. Call Start before anything else.
func NewCluster() *Cluster {
	return &Cluster{}
}

// Start launches n server processes with IDs 0..n-1 and waits until they are
// all ready to serve requests.
//
// Each process gets its own gRPC port, admin port, and data directory so that
// processes are fully isolated on disk and on the network.
//
// TODO: fill in the exec.Command call with the correct binary path and flags,
// then call waitReady (or equivalent) for each node.
func (c *Cluster) Start(n int) error {
	if n <= 0 {
		return errors.New("node count must be > 0")
	}
	c.n = n
	c.ports = make([]int, n)
	c.adminPorts = make([]int, n)
	c.cmds = make([]*exec.Cmd, n)
	c.nodeIDs = make([]interfaces.NodeID, n)

	// Allocate ports before starting any process so each node can be told the
	// full peer list on its command line.
	for i := range n {
		p, err := freePort()
		if err != nil {
			return fmt.Errorf("node %d: allocate gRPC port: %w", i, err)
		}
		a, err := freePort()
		if err != nil {
			return fmt.Errorf("node %d: allocate admin port: %w", i, err)
		}
		c.ports[i] = p
		c.adminPorts[i] = a
		c.nodeIDs[i] = interfaces.NodeID(i)
	}

	for i := range n {
		// TODO: build peer list (all gRPC addresses except self), then:
		//
		//   peers := peerList(c.ports, i) // e.g. "localhost:9001,localhost:9002"
		//   c.cmds[i] = exec.Command("./kvserver",
		//       "--id",    strconv.Itoa(i),
		//       "--port",  strconv.Itoa(c.ports[i]),
		//       "--admin", strconv.Itoa(c.adminPorts[i]),
		//       "--data",  fmt.Sprintf("/tmp/tensorkv-node-%d", i),
		//       "--peers", peers,
		//   )
		//   c.cmds[i].Stdout = os.Stderr  // pipe server logs for debugging
		//   c.cmds[i].Stderr = os.Stderr
		//   if err := c.cmds[i].Start(); err != nil {
		//       return fmt.Errorf("node %d: start process: %w", i, err)
		//   }
		_ = i
	}

	// TODO: wait until every node is ready to accept gRPC connections.
	//
	//   for i := 0; i < n; i++ {
	//       if err := waitReady(c.ports[i], 10*time.Second); err != nil {
	//           return fmt.Errorf("node %d: health check failed: %w", i, err)
	//       }
	//   }
	return errors.New("not implemented")
}

// Connect dials a gRPC connection to nodeID and returns a Store that routes
// all Put/Get calls through that node.
//
// TODO: replace the stub with a real gRPC dial:
//
//	conn, err := grpc.NewClient(
//	    fmt.Sprintf("localhost:%d", c.ports[idx]),
//	    grpc.WithTransportCredentials(insecure.NewCredentials()),
//	)
//	if err != nil { return nil, err }
//	return &nodeStore{conn: conn, client: kvpb.NewTensorKVClient(conn)}, nil
func (c *Cluster) Connect(nodeID interfaces.NodeID) (interfaces.Store, error) {
	idx := int(nodeID)
	if idx < 0 || idx >= len(c.ports) {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	// TODO: dial gRPC and return a nodeStore with a real client.
	addr := fmt.Sprintf("localhost:%d", c.ports[idx])
	return &nodeStore{addr: addr, nodeID: nodeID}, nil
}

// NodeIDs returns the IDs of all nodes in the cluster.
func (c *Cluster) NodeIDs() []interfaces.NodeID {
	ids := make([]interfaces.NodeID, len(c.nodeIDs))
	copy(ids, c.nodeIDs)
	return ids
}

// KillNode sends SIGKILL to the node process. The abrupt exit causes all
// in-flight gRPC calls to return codes.Unavailable, which grpcToHarnessErr
// maps to interfaces.ErrNodeDown.
//
// The node's data directory is preserved so RestartNode can replay the WAL.
func (c *Cluster) KillNode(nodeID interfaces.NodeID) error {
	idx := int(nodeID)
	if idx < 0 || idx >= len(c.cmds) {
		return fmt.Errorf("node %d not found", nodeID)
	}
	cmd := c.cmds[idx]
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("node %d: process not running", nodeID)
	}
	return cmd.Process.Kill()
}

// RestartNode re-launches the server process for nodeID. The server must replay
// its write-ahead log (or fetch a snapshot from a live peer) before the method
// returns, so that subsequent Gets return pre-crash writes.
//
// TODO: re-issue the same exec.Command as in Start for this node, then wait
// for the health check to pass before returning.
//
//	c.cmds[idx] = exec.Command("./kvserver", ...same flags as in Start...)
//	if err := c.cmds[idx].Start(); err != nil { return err }
//	return waitReady(c.ports[idx], 15*time.Second)
func (c *Cluster) RestartNode(nodeID interfaces.NodeID) error {
	// TODO: re-launch the server process and wait for recovery.
	return errors.New("not implemented")
}

// PartitionNodes tells nodes a and b to drop traffic to each other.
//
// The recommended implementation is an admin HTTP endpoint on each server:
//
//	POST http://localhost:<adminPort>/partition?peer=<grpcAddr>
//
// Your server's replication goroutines must check a per-peer blocklist before
// dialling or forwarding messages. Call the endpoint on both nodes so the
// partition is bidirectional.
//
// Alternative: iptables (requires root) or a proxy you control.
//
// TODO: send the admin request to both nodes.
//
//	addrA := fmt.Sprintf("localhost:%d", c.ports[int(a)])
//	addrB := fmt.Sprintf("localhost:%d", c.ports[int(b)])
//	if err := adminPost(c.adminPorts[int(a)], "/partition", addrB); err != nil { return err }
//	return adminPost(c.adminPorts[int(b)], "/partition", addrA)
func (c *Cluster) PartitionNodes(a, b interfaces.NodeID) error {
	// TODO: call admin endpoint on node a and node b.
	return errors.New("not implemented")
}

// HealPartition removes the partition between a and b.
//
// TODO: send the admin request to both nodes.
//
//	addrA := fmt.Sprintf("localhost:%d", c.ports[int(a)])
//	addrB := fmt.Sprintf("localhost:%d", c.ports[int(b)])
//	if err := adminPost(c.adminPorts[int(a)], "/heal", addrB); err != nil { return err }
//	return adminPost(c.adminPorts[int(b)], "/heal", addrA)
func (c *Cluster) HealPartition(a, b interfaces.NodeID) error {
	// TODO: call admin endpoint on node a and node b.
	return errors.New("not implemented")
}

// Shutdown kills all running node processes and waits for them to exit.
func (c *Cluster) Shutdown() error {
	for _, cmd := range c.cmds {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill() //nolint:errcheck
			cmd.Wait()         //nolint:errcheck
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// freePort asks the OS for an available TCP port on localhost.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// adminPost is a placeholder showing how to call the admin HTTP endpoint.
// Replace with a real net/http call once your server exposes /partition and /heal.
//
//	func adminPost(adminPort int, path, peer string) error {
//	    url := fmt.Sprintf("http://localhost:%d%s?peer=%s", adminPort, path, peer)
//	    resp, err := http.Post(url, "", nil)
//	    if err != nil { return err }
//	    resp.Body.Close()
//	    if resp.StatusCode != http.StatusOK {
//	        return fmt.Errorf("admin %s: status %d", path, resp.StatusCode)
//	    }
//	    return nil
//	}
