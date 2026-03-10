package cluster

import (
    "kvstore/kv"
    "testing"
)

func TestCrossShardOperations(t *testing.T) {
    numShards := 3
    clients := make(map[int]*kv.Client)
    for i := 0; i < numShards; i++ {
        clients[i] = kv.NewClient("")
    }

    router := NewRouter(numShards, clients)

    // Test Put
    key := "testKey"
    value := "testValue"
    client := router.Route(key)
    client.Put(key, value)

    // Test Get
    got := client.Get(key)
    if got != value {
        t.Errorf("expected %s, got %s", value, got)
    }

    // Test Delete
    client.Delete(key)
    got = client.Get(key)
    if got != "" {
        t.Errorf("expected empty string, got %s", got)
    }
}