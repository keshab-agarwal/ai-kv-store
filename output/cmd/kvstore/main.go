package main

import (
    "fmt"
    "kvstore/cluster"
    "kvstore/kv"
    "log"
    "net/http"
)

func main() {
    numShards := 3
    clients := make(map[int]*kv.Client)
    for i := 0; i < numShards; i++ {
        clients[i] = kv.NewClient(fmt.Sprintf(":808%d", i))
    }

    router := cluster.NewRouter(numShards, clients)

    http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        value := r.URL.Query().Get("value")
        client := router.Route(key)
        client.Put(key, value)
        fmt.Fprintf(w, "Put %s=%s", key, value)
    })

    http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        client := router.Route(key)
        value := client.Get(key)
        fmt.Fprintf(w, "Get %s=%s", key, value)
    })

    http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        client := router.Route(key)
        client.Delete(key)
        fmt.Fprintf(w, "Deleted %s", key)
    })

    log.Fatal(http.ListenAndServe(":8080", nil))
}