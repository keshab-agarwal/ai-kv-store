// kvnode is the distributed key-value store node binary.
//
// Usage:
//
//	kvnode -id=1 -rpc=":17001" -client=":16001" -peers="2=localhost:17002,3=localhost:17003"
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"ai-kv-store/kvstore"
)

func main() {
	id := flag.Int("id", 0, "Node ID (positive integer)")
	rpcAddr := flag.String("rpc", ":17001", "RPC listen address")
	clientAddr := flag.String("client", ":16001", "Client HTTP listen address")
	peersStr := flag.String("peers", "", "Peers in format: id=host:port,id=host:port")
	flag.Parse()

	if *id <= 0 {
		fmt.Fprintf(os.Stderr, "Error: -id must be a positive integer\n")
		os.Exit(1)
	}

	peers, err := parsePeers(*peersStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing peers: %v\n", err)
		os.Exit(1)
	}

	node := kvstore.NewNode(*id, *rpcAddr, *clientAddr, peers)
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	log.Printf("Node %d started (rpc=%s, client=%s, peers=%d)", *id, *rpcAddr, *clientAddr, len(peers))

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("Node %d shutting down...", *id)
	node.Stop()
}

func parsePeers(s string) (map[int]string, error) {
	peers := make(map[int]string)
	if s == "" {
		return peers, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			return nil, fmt.Errorf("invalid peer format: %q (expected id=host:port)", part)
		}
		idStr := part[:eqIdx]
		addr := part[eqIdx+1:]
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q: %v", idStr, err)
		}
		peers[id] = addr
	}
	return peers, nil
}
