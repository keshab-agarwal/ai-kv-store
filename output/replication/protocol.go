package replication

import (
	"errors"
	"sync"
)

// Cluster represents the entire cluster of nodes.
type Cluster struct {
	Nodes map[string]*Node
	Mutex sync.Mutex
}

// NewCluster initializes a new cluster with the given nodes.
func NewCluster(nodeIDs []string) *Cluster {
	cluster := &Cluster{Nodes: make(map[string]*Node)}
	for _, id := range nodeIDs {
		cluster.Nodes[id] = &Node{
			ID:      id,
			Role:    Backup,
			Epoch:   1,
			Log:     []LogEntry{},
			KVState: make(map[string]string),
		}
	}
	// Set the initial primary, backup, and witness roles
	if len(nodeIDs) >= 3 {
		cluster.Nodes[nodeIDs[0]].Role = Primary
		cluster.Nodes[nodeIDs[1]].Role = Backup
		cluster.Nodes[nodeIDs[2]].Role = Witness
		cluster.Nodes[nodeIDs[0]].LeaseValid = true
	}
	return cluster
}

// StartElection implements the TLA+ StartElection action.
func (c *Cluster) StartElection(nodeID string) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	node, exists := c.Nodes[nodeID]
	if !exists || node.Role == Primary || node.Role == Crashed {
		return errors.New("invalid node for election")
	}

	// Check if there is no current primary
	for _, n := range c.Nodes {
		if n.Role == Primary && n.LeaseValid {
			return errors.New("primary already exists")
		}
	}

	// Increment epoch and start election
	node.Epoch++
	node.VotedFor = nodeID
	node.Role = Primary
	node.LeaseValid = true

	// Append a no-op entry to the log
	noopEntry := LogEntry{
		Epoch:  node.Epoch,
		Key:    "",
		Value:  "",
		OpType: "noop",
		Index:  len(node.Log) + 1,
	}
	node.Log = append(node.Log, noopEntry)

	return nil
}

// ClientPut implements the TLA+ ClientPut action.
func (c *Cluster) ClientPut(nodeID, key, value string) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	node, exists := c.Nodes[nodeID]
	if !exists || node.Role != Primary {
		return errors.New("node is not primary")
	}

	// Find a backup node
	var backupNode *Node
	for _, n := range c.Nodes {
		if n.Role == Backup && n.Role != Crashed {
			backupNode = n
			break
		}
	}

	if backupNode == nil {
		return errors.New("no backup node available")
	}

	// Create log entry
	entry := LogEntry{
		Epoch:  node.Epoch,
		Key:    key,
		Value:  value,
		OpType: "put",
		Index:  len(node.Log) + 1,
	}

	// Append to primary and backup logs
	node.Log = append(node.Log, entry)
	backupNode.Log = append(backupNode.Log, entry)

	// Update commit index and state
	node.CommitIndex = entry.Index
	node.KVState[key] = value

	return nil
}

// ClientDelete implements the TLA+ ClientDelete action.
func (c *Cluster) ClientDelete(nodeID, key string) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	node, exists := c.Nodes[nodeID]
	if !exists || node.Role != Primary {
		return errors.New("node is not primary")
	}

	// Find a backup node
	var backupNode *Node
	for _, n := range c.Nodes {
		if n.Role == Backup && n.Role != Crashed {
			backupNode = n
			break
		}
	}

	if backupNode == nil {
		return errors.New("no backup node available")
	}

	// Create log entry
	entry := LogEntry{
		Epoch:  node.Epoch,
		Key:    key,
		Value:  "",
		OpType: "delete",
		Index:  len(node.Log) + 1,
	}

	// Append to primary and backup logs
	node.Log = append(node.Log, entry)
	backupNode.Log = append(backupNode.Log, entry)

	// Update commit index and state
	node.CommitIndex = entry.Index
	delete(node.KVState, key)

	return nil
}

// BackupApply implements the TLA+ BackupApply action.
func (c *Cluster) BackupApply(nodeID string) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	node, exists := c.Nodes[nodeID]
	if !exists || node.Role != Backup {
		return errors.New("node is not a backup")
	}

	// Find the primary node
	var primaryNode *Node
	for _, n := range c.Nodes {
		if n.Role == Primary && n.Role != Crashed {
			primaryNode = n
			break
		}
	}

	if primaryNode == nil {
		return errors.New("no primary node available")
	}

	// Apply log entries up to the primary's commit index
	if len(node.Log) >= primaryNode.CommitIndex {
		node.CommitIndex = primaryNode.CommitIndex
		node.KVState = applyLog(node.Log, node.CommitIndex)
	}

	return nil
}

// applyLog applies log entries up to a given index to the state.
func applyLog(log []LogEntry, upTo int) map[string]string {
	state := make(map[string]string)
	for i := 0; i < upTo && i < len(log); i++ {
		e := log[i]
		if e.OpType == "put" {
			state[e.Key] = e.Value
		} else if e.OpType == "delete" {
			delete(state, e.Key)
		}
	}
	return state
}