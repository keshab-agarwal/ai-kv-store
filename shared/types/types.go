// Package types defines shared types for the distributed KV store.
package types

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Key is a 128-bit opaque key (16 bytes).
type Key [16]byte

func (k Key) String() string { return hex.EncodeToString(k[:]) }

func KeyFromHex(s string) (Key, error) {
	var k Key
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		return k, fmt.Errorf("invalid hex key: %q", s)
	}
	copy(k[:], b)
	return k, nil
}

func RandomKey() Key {
	var k Key
	rand.Read(k[:])
	return k
}

// OpType represents operation types.
type OpType uint8

const (
	OpGet    OpType = 1
	OpPut    OpType = 2
	OpDelete OpType = 3
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Status represents operation result status.
type Status uint8

const (
	StatusOK       Status = 0
	StatusFound    Status = 1
	StatusNotFound Status = 2
	StatusError    Status = 3
	StatusTimeout  Status = 4
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusFound:
		return "found"
	case StatusNotFound:
		return "not_found"
	case StatusError:
		return "error"
	case StatusTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Result represents an operation result.
type Result struct {
	Status Status
	Value  []byte
	Error  string
}

// MsgType identifies the type of wire message.
type MsgType uint8

const (
	MsgGetReq    MsgType = 1
	MsgGetResp   MsgType = 2
	MsgPutReq    MsgType = 3
	MsgPutResp   MsgType = 4
	MsgDelReq    MsgType = 5
	MsgDelResp   MsgType = 6
	MsgReplReq   MsgType = 10
	MsgReplResp  MsgType = 11
	MsgHeartbeat MsgType = 20
	MsgHeartAck  MsgType = 21
	MsgVoteReq   MsgType = 22
	MsgVoteResp  MsgType = 23
	MsgSyncReq   MsgType = 30
	MsgSyncResp  MsgType = 31
	MsgForward   MsgType = 40
	MsgFwdResp   MsgType = 41
	MsgStatusReq  MsgType = 50
	MsgStatusResp MsgType = 51
)

// NodeRole represents the role of a node.
type NodeRole uint8

const (
	RoleFollower  NodeRole = 0
	RoleCandidate NodeRole = 1
	RoleCoordinator NodeRole = 2
)

func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleCoordinator:
		return "coordinator"
	default:
		return "unknown"
	}
}

// LogEntry represents a replicated log entry.
type LogEntry struct {
	Epoch    uint64
	Index    uint64
	Op       OpType
	Key      Key
	Value    []byte
	ClientID string
	ClientSeq uint64
}

// ClusterConfig holds cluster configuration.
type ClusterConfig struct {
	NodeID             int      `json:"node_id"`
	Peers              []string `json:"peers"` // host:port for each node (index = node id)
	HeartbeatMs        int      `json:"heartbeat_ms"`
	ElectionMinMs      int      `json:"election_min_ms"`
	ElectionMaxMs      int      `json:"election_max_ms"`
	LeaseMs            int      `json:"lease_ms"`
	RPCTimeoutMs       int      `json:"rpc_timeout_ms"`
	WriteTimeoutMs     int      `json:"write_timeout_ms"`
	MaxValueSize       int      `json:"max_value_size"`
	DataDir            string   `json:"data_dir"`
	ClientPort         int      `json:"client_port"`
}

func DefaultConfig() ClusterConfig {
	return ClusterConfig{
		HeartbeatMs:    50,
		ElectionMinMs:  300,
		ElectionMaxMs:  500,
		LeaseMs:        150,
		RPCTimeoutMs:   200,
		WriteTimeoutMs: 5000,
		MaxValueSize:   1 << 20, // 1 MiB
	}
}
