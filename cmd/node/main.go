package main

import (
	"flag"
	"fmt"
	"kv-store/interfaces"
	"kv-store/internal/node"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	id := flag.Uint("id", 0, "node ID (0, 1, or 2)")
	port := flag.String("port", "8000", "listen port")
	adminPort := flag.String("admin", "", "admin HTTP port for partition/heal (optional)")
	dataDir := flag.String("data", "", "data directory (default: ./data-<id>)")
	peers := flag.String("peers", "localhost:8000,localhost:8001,localhost:8002", "comma-separated peer addresses (node0,node1,node2)")
	flag.Parse()
	if *id > 2 {
		fmt.Fprintf(os.Stderr, "id must be 0, 1, or 2\n")
		os.Exit(1)
	}
	peerAddrs := splitPeers(*peers)
	if len(peerAddrs) != 3 {
		fmt.Fprintf(os.Stderr, "peers must be exactly 3 addresses\n")
		os.Exit(1)
	}
	dir := *dataDir
	if dir == "" {
		dir = "./data-" + strconv.FormatUint(uint64(*id), 10)
	}
	n, err := node.New(interfaces.NodeID(*id), dir, peerAddrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new node: %v\n", err)
		os.Exit(1)
	}
	addr := "127.0.0.1:" + *port
	if err := n.Listen(addr); err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	if *adminPort != "" {
		adminAddr := "127.0.0.1:" + *adminPort
		if err := n.ListenAdmin(adminAddr); err != nil {
			fmt.Fprintf(os.Stderr, "admin listen: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("node %d admin on %s\n", *id, adminAddr)
	}
	fmt.Printf("node %d listening on %s\n", *id, addr)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	_ = n.Close()
}

func splitPeers(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
