package cluster

import "hash/fnv"

// Shard represents a shard in the cluster.
type Shard struct {
    ID int
}

// HashKeyToShard maps a key to a shard ID.
func HashKeyToShard(key string, numShards int) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32()) % numShards
}