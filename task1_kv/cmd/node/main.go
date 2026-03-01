package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"base/task1_kv/internal/kv"
)

func main() {
	var (
		nodeID = flag.Int("node-id", 0, "node id")
		addr   = flag.String("addr", "http://127.0.0.1:9001", "advertised node address")
		listen = flag.String("listen", "127.0.0.1:9001", "listen address")
		peers  = flag.String("peers", "", "comma separated peers: id=http://host:port")
	)
	flag.Parse()

	peerMap, err := kv.ParsePeers(*peers)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}
	if _, ok := peerMap[*nodeID]; !ok {
		peerMap[*nodeID] = *addr
	}

	node := kv.NewNode(kv.Config{
		NodeID:            *nodeID,
		Addr:              *addr,
		Peers:             peerMap,
		ElectionTimeoutLo: 650 * time.Millisecond,
		ElectionTimeoutHi: 1050 * time.Millisecond,
		HeartbeatInterval: 120 * time.Millisecond,
		HTTPTimeout:       500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.StartBackground(ctx)

	srv := &http.Server{Addr: *listen, Handler: node, ReadHeaderTimeout: 2 * time.Second}

	go func() {
		log.Printf("node %d listening on %s", *nodeID, *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	ctxStop, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	_ = srv.Shutdown(ctxStop)
}
