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
	nodeID := flag.Uint64("id", 0, "Node ID (required)")
	rpcAddr := flag.String("rpc", "", "RPC listen address (e.g. :9001)")
	clientAddr := flag.String("client", "", "Client HTTP listen address (e.g. :8001)")
	peersFlag := flag.String("peers", "", "Comma-separated peers: id=host:port,id=host:port")
	flag.Parse()

	if *nodeID == 0 || *rpcAddr == "" || *clientAddr == "" {
		fmt.Fprintf(os.Stderr, "Usage: kvnode -id=N -rpc=:9001 -client=:8001 -peers=2=localhost:9002,3=localhost:9003\n")
		os.Exit(1)
	}

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid peers: %v\n", err)
		os.Exit(1)
	}

	cfg := kvstore.DefaultConfig()
	cfg.NodeID = *nodeID
	cfg.RPCAddr = *rpcAddr
	cfg.ClientAddr = *clientAddr
	cfg.Peers = peers

	logger := log.New(os.Stderr, fmt.Sprintf("[node%d] ", *nodeID), log.LstdFlags|log.Lmicroseconds)

	node := kvstore.NewNode(cfg, logger)
	if err := node.Start(); err != nil {
		logger.Fatalf("start failed: %v", err)
	}

	api := kvstore.NewAPIServer(node)
	if err := api.Start(*clientAddr); err != nil {
		logger.Fatalf("api start failed: %v", err)
	}

	logger.Printf("node ready: rpc=%s client=%s", *rpcAddr, *clientAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Printf("shutting down")
	api.Stop()
	node.Stop()
}

func parsePeers(s string) ([]kvstore.PeerInfo, error) {
	if s == "" {
		return nil, nil
	}
	var peers []kvstore.PeerInfo
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			return nil, fmt.Errorf("peer format must be id=host:port, got %q", part)
		}
		id, err := strconv.ParseUint(part[:eqIdx], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer id in %q: %w", part, err)
		}
		addr := part[eqIdx+1:]
		peers = append(peers, kvstore.PeerInfo{ID: id, RPCAddr: addr})
	}
	return peers, nil
}
