package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"ai-kv-store/shared/types"
	"ai-kv-store/task1_kv/client"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: kvcli [--addr host:port] <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  get <hex-key>\n")
	fmt.Fprintf(os.Stderr, "  put <hex-key> <value>\n")
	fmt.Fprintf(os.Stderr, "  delete <hex-key>\n")
	fmt.Fprintf(os.Stderr, "  status\n")
	os.Exit(1)
}

func main() {
	addr := "localhost:9000"
	args := os.Args[1:]

	// Parse --addr flag
	for i := 0; i < len(args); i++ {
		if args[i] == "--addr" && i+1 < len(args) {
			addr = args[i+1]
			args = append(args[:i], args[i+2:]...)
			break
		}
	}

	if len(args) == 0 {
		usage()
	}

	c := client.NewClient(addr, 5*time.Second)
	ctx := context.Background()

	switch args[0] {
	case "get":
		if len(args) != 2 {
			usage()
		}
		key, err := types.KeyFromHex(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid key: %v\n", err)
			os.Exit(1)
		}
		result := c.Get(ctx, key)
		switch result.Status {
		case types.StatusFound:
			fmt.Printf("found: %s\n", base64.StdEncoding.EncodeToString(result.Value))
		case types.StatusNotFound:
			fmt.Println("not_found")
		default:
			fmt.Printf("error: %s\n", result.Error)
			os.Exit(1)
		}

	case "put":
		if len(args) != 3 {
			usage()
		}
		key, err := types.KeyFromHex(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid key: %v\n", err)
			os.Exit(1)
		}
		value := []byte(args[2])
		result := c.Put(ctx, key, value)
		if result.Status == types.StatusOK {
			fmt.Println("ok")
		} else {
			fmt.Printf("error: %s\n", result.Error)
			os.Exit(1)
		}

	case "delete":
		if len(args) != 2 {
			usage()
		}
		key, err := types.KeyFromHex(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid key: %v\n", err)
			os.Exit(1)
		}
		result := c.Delete(ctx, key)
		if result.Status == types.StatusOK {
			fmt.Println("ok")
		} else {
			fmt.Printf("error: %s\n", result.Error)
			os.Exit(1)
		}

	case "status":
		status, err := c.Status(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Node ID:       %d\n", status.NodeID)
		fmt.Printf("Role:          %s\n", status.Role)
		fmt.Printf("Epoch:         %d\n", status.Epoch)
		fmt.Printf("Applied Index: %d\n", status.AppliedIndex)
		fmt.Printf("Commit Index:  %d\n", status.CommitIndex)
		fmt.Printf("Log Length:    %d\n", status.LogLength)
		fmt.Printf("Store Size:    %d\n", status.StoreSize)
		fmt.Printf("Alive Peers:   %v\n", status.AlivePeers)

	default:
		usage()
	}
}
