package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"ai-kv-store/shared/types"
	"ai-kv-store/task1_kv/server"
)

func main() {
	id := flag.Int("id", 0, "Node ID (0-based index into peers list)")
	port := flag.Int("port", 9000, "Listen port (used if peers not specified)")
	peersStr := flag.String("peers", "", "Comma-separated list of host:port for all nodes (index = node id)")
	dataDir := flag.String("data-dir", "", "Data directory (unused, for future persistence)")
	flag.Parse()

	if *peersStr == "" {
		fmt.Fprintf(os.Stderr, "Error: --peers is required\n")
		flag.Usage()
		os.Exit(1)
	}

	peers := strings.Split(*peersStr, ",")
	for i := range peers {
		peers[i] = strings.TrimSpace(peers[i])
	}

	if *id < 0 || *id >= len(peers) {
		fmt.Fprintf(os.Stderr, "Error: --id must be between 0 and %d\n", len(peers)-1)
		os.Exit(1)
	}

	_ = *port // port is derived from peers list

	config := types.DefaultConfig()
	config.NodeID = *id
	config.Peers = peers
	config.DataDir = *dataDir

	node := server.NewNode(config)
	if err := node.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting node: %v\n", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigC

	fmt.Fprintf(os.Stderr, "{\"msg\":\"shutting down\",\"signal\":\"%v\"}\n", sig)
	node.Stop()
}
