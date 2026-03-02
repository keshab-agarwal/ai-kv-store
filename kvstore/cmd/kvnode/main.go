package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"ai-kv-store/kvstore"
)

func main() {
	// --- Flag parsing ---
	idFlag := flag.Int("id", 0, "node ID (required, must be > 0)")
	rpcFlag := flag.String("rpc", ":17001", "RPC listen address (inter-node)")
	clientFlag := flag.String("client", ":16001", "client HTTP listen address")
	dataFlag := flag.String("data", "", "data directory (default: ./data-<id>)")
	peersFlag := flag.String("peers", "", `comma-separated "id=rpcaddr" pairs, e.g. "2=localhost:17002,3=localhost:17003"`)
	shardsFlag := flag.Int("shards", 1, "number of shards (reserved for future use)")

	flag.Parse()

	if *idFlag == 0 {
		fmt.Fprintln(os.Stderr, "error: -id flag is required and must be non-zero")
		flag.Usage()
		os.Exit(1)
	}

	nodeID := *idFlag
	dataDir := *dataFlag
	if dataDir == "" {
		dataDir = fmt.Sprintf("./data-%d", nodeID)
	}

	// Parse peers: "2=localhost:17002,3=localhost:17003"
	peers := make(map[int]string)
	if *peersFlag != "" {
		for _, part := range strings.Split(*peersFlag, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				fmt.Fprintf(os.Stderr, "error: invalid peer spec %q (expected id=addr)\n", part)
				os.Exit(1)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(kv[0]))
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: invalid peer ID %q: %v\n", kv[0], err)
				os.Exit(1)
			}
			peers[pid] = strings.TrimSpace(kv[1])
		}
	}

	// Normalize addresses: ":PORT" -> "localhost:PORT" for redirect announcements.
	advertiseClient := *clientFlag
	if strings.HasPrefix(advertiseClient, ":") {
		advertiseClient = "localhost" + advertiseClient
	}
	advertiseRPC := *rpcFlag
	if strings.HasPrefix(advertiseRPC, ":") {
		advertiseRPC = "localhost" + advertiseRPC
	}
	_ = advertiseRPC // used implicitly via peer RPC addresses

	log.Printf("starting node id=%d rpc=%s client=%s advertise=%s data=%s shards=%d peers=%v",
		nodeID, *rpcFlag, *clientFlag, advertiseClient, dataDir, *shardsFlag, peers)

	// --- Create and start node ---
	node, err := kvstore.NewNode(nodeID, *rpcFlag, advertiseClient, dataDir, peers, *shardsFlag)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}
	node.Start()
	log.Printf("node %d started", nodeID)

	// --- Start RPC server ---
	rpcServer := &http.Server{
		Addr:    *rpcFlag,
		Handler: kvstore.SetupRPCServer(node),
	}
	go func() {
		log.Printf("node %d: RPC server listening on %s", nodeID, *rpcFlag)
		if err := rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("rpc server error: %v", err)
		}
	}()

	// --- Start client server ---
	clientServer := &http.Server{
		Addr:    *clientFlag,
		Handler: kvstore.SetupClientServer(node),
	}
	go func() {
		log.Printf("node %d: client server listening on %s", nodeID, *clientFlag)
		if err := clientServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("client server error: %v", err)
		}
	}()

	// --- Wait for shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("node %d: received signal %v, shutting down...", nodeID, sig)

	// Graceful shutdown of HTTP servers.
	shutdownCtx := context.Background()
	if err := clientServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("node %d: client server shutdown error: %v", nodeID, err)
	}
	if err := rpcServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("node %d: rpc server shutdown error: %v", nodeID, err)
	}

	node.Stop()
	log.Printf("node %d: stopped", nodeID)
}
