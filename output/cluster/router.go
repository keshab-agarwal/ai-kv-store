package cluster

import (
    "kvstore/kv"
)

// Router routes requests to the appropriate shard.
type Router struct {
    shards []Shard
    clients map[int]*kv.Client
}

// NewRouter creates a new Router.
func NewRouter(numShards int, clients map[int]*kv.Client) *Router {
    shards := make([]Shard, numShards)
    for i := 0; i < numShards; i++ {
        shards[i] = Shard{ID: i}
    }
    return &Router{shards: shards, clients: clients}
}

// Route routes the key to the appropriate shard client.
func (r *Router) Route(key string) *kv.Client {
    shardID := HashKeyToShard(key, len(r.shards))
    return r.clients[shardID]
}