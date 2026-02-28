package kvstore

import (
	"encoding/hex"
	"fmt"
	"time"
)

type Key [16]byte

func (k Key) Hex() string { return hex.EncodeToString(k[:]) }

func KeyFromHex(s string) (Key, error) {
	var k Key
	b, err := hex.DecodeString(s)
	if err != nil {
		return k, fmt.Errorf("invalid key hex: %q", s)
	}
	if len(b) != 16 {
		return k, fmt.Errorf("key must be 16 bytes, got %d", len(b))
	}
	copy(k[:], b)
	return k, nil
}

type OpType uint8

const (
	OpNoop   OpType = 0
	OpPut    OpType = 1
	OpDelete OpType = 2
)

func (o OpType) String() string {
	switch o {
	case OpNoop:
		return "Noop"
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	default:
		return "Unknown"
	}
}

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
		return "OK"
	case StatusFound:
		return "FOUND"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusError:
		return "ERROR"
	case StatusTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

type Role uint8

const (
	Follower    Role = 0
	Candidate   Role = 1
	Coordinator Role = 2
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Coordinator:
		return "Coordinator"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Index     int64
	Epoch     uint64
	Op        OpType
	Key       Key
	Value     []byte
	ClientID  string
	ClientSeq uint64
}

type OpResult struct {
	Status Status
	Value  []byte
}

// --- RPC message types ---

type VoteRequest struct {
	Epoch        uint64
	CandidateID  uint64
	LastLogIndex int64
	LastLogEpoch uint64
}

type VoteResponse struct {
	Epoch   uint64
	Granted bool
}

type AppendRequest struct {
	Epoch         uint64
	CoordinatorID uint64
	PrevLogIndex  int64
	PrevLogEpoch  uint64
	Entries       []LogEntry
	CommitIndex   int64
}

type AppendResponse struct {
	Epoch      uint64
	Success    bool
	MatchIndex int64
}

type ForwardRequest struct {
	Op        OpType
	Key       Key
	Value     []byte
	ClientID  string
	ClientSeq uint64
	IsRead    bool
}

type ForwardResponse struct {
	Result OpResult
}

type SnapshotRequest struct {
	Epoch         uint64
	CoordinatorID uint64
	LastIndex     int64
	LastEpoch     uint64
	Data          map[Key][]byte
}

type SnapshotResponse struct {
	Epoch   uint64
	Success bool
}

// --- Configuration ---

type PeerInfo struct {
	ID      uint64
	RPCAddr string
}

type Config struct {
	NodeID             uint64
	RPCAddr            string
	ClientAddr         string
	Peers              []PeerInfo
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	LeaseDuration      time.Duration
	RPCTimeout         time.Duration
	WriteTimeout       time.Duration
	MaxValueSize       int
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		LeaseDuration:      150 * time.Millisecond,
		RPCTimeout:         200 * time.Millisecond,
		WriteTimeout:       5 * time.Second,
		MaxValueSize:       1 << 20,
	}
}
