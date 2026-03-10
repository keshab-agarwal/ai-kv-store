package replication

import (
	"testing"
)

func TestClusterFormation(t *testing.T) {
	cluster := NewCluster([]string{"node1", "node2", "node3"})

	if len(cluster.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(cluster.Nodes))
	}

	primaryCount := 0
	backupCount := 0
	witnessCount := 0

	for _, node := range cluster.Nodes {
		switch node.Role {
		case Primary:
			primaryCount++
		case Backup:
			backupCount++
		case Witness:
			witnessCount++
		}
	}

	if primaryCount != 1 || backupCount != 1 || witnessCount != 1 {
		t.Fatalf("expected 1 primary, 1 backup, 1 witness, got %d primary, %d backup, %d witness", primaryCount, backupCount, witnessCount)
	}
}

func TestLeaderElection(t *testing.T) {
	cluster := NewCluster([]string{"node1", "node2", "node3"})

	// Simulate the primary node crashing
	cluster.Nodes["node1"].Role = Crashed
	cluster.Nodes["node1"].LeaseValid = false

	err := cluster.StartElection("node2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cluster.Nodes["node2"].Role != Primary {
		t.Fatalf("node2 should be primary after election")
	}
}

func TestClientPutGetDelete(t *testing.T) {
	cluster := NewCluster([]string{"node1", "node2", "node3"})

	err := cluster.ClientPut("node1", "key1", "value1")
	if err != nil {
		t.Fatalf("unexpected error on put: %v", err)
	}

	if cluster.Nodes["node1"].KVState["key1"] != "value1" {
		t.Fatalf("expected key1 to be value1, got %s", cluster.Nodes["node1"].KVState["key1"])
	}

	err = cluster.ClientDelete("node1", "key1")
	if err != nil {
		t.Fatalf("unexpected error on delete: %v", err)
	}

	if _, exists := cluster.Nodes["node1"].KVState["key1"]; exists {
		t.Fatalf("expected key1 to be deleted")
	}
}

func TestLogAgreement(t *testing.T) {
	cluster := NewCluster([]string{"node1", "node2", "node3"})

	err := cluster.ClientPut("node1", "key1", "value1")
	if err != nil {
		t.Fatalf("unexpected error on put: %v", err)
	}

	err = cluster.BackupApply("node2")
	if err != nil {
		t.Fatalf("unexpected error on backup apply: %v", err)
	}

	if cluster.Nodes["node2"].KVState["key1"] != "value1" {
		t.Fatalf("expected key1 to be value1 on backup, got %s", cluster.Nodes["node2"].KVState["key1"])
	}
}