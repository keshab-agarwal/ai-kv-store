package replication

// NodeRole represents the role of a node in the cluster.
type NodeRole string

const (
	Primary  NodeRole = "primary"
	Backup   NodeRole = "backup"
	Witness  NodeRole = "witness"
	Crashed  NodeRole = "crashed"
)

// LogEntry represents an entry in the node's log.
type LogEntry struct {
	Epoch   int
	Key     string
	Value   string
	OpType  string // "put" or "delete"
	Index   int
}

// Node represents a node in the cluster.
type Node struct {
	ID          string
	Role        NodeRole
	Epoch       int
	Log         []LogEntry
	CommitIndex int
	VotedFor    string
	KVState     map[string]string
	LeaseValid  bool
}

// WALEntry represents a write-ahead log entry for durability.
type WALEntry struct {
	Epoch  int
	Key    string
	Value  string
	OpType string
	Index  int
}