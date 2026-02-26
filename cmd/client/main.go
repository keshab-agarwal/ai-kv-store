package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"kv-store/client"
	"kv-store/interfaces"
	"os"
)

func main() {
	addr := flag.String("addr", "localhost:8000", "node address")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: client get <key-hex> | put <key-hex> [value] | key\n")
		fmt.Fprintf(os.Stderr, "  key-hex: 32 hex chars (16 bytes). use 'key' to generate one.\n")
		fmt.Fprintf(os.Stderr, "  for put, value is optional; if omitted, read from stdin\n")
		os.Exit(1)
	}
	cmd := args[0]
	if cmd == "key" {
		var k interfaces.Key
		if _, err := rand.Read(k[:]); err != nil {
			fmt.Fprintf(os.Stderr, "rand: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(hex.EncodeToString(k[:]))
		return
	}
	if cmd != "get" && cmd != "put" {
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		os.Exit(1)
	}
	if (cmd == "get" && len(args) != 2) || (cmd == "put" && len(args) != 2 && len(args) != 3) {
		fmt.Fprintf(os.Stderr, "usage: client get <key-hex> | put <key-hex> [value]\n")
		os.Exit(1)
	}
	key, err := parseKey(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "key: %v\n", err)
		os.Exit(1)
	}
	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	ctx := context.Background()
	switch cmd {
	case "get":
		val, err := c.Get(ctx, key)
		if err != nil {
			if err == interfaces.ErrKeyNotFound {
				fmt.Fprintf(os.Stderr, "key not found\n")
			} else {
				fmt.Fprintf(os.Stderr, "get: %v\n", err)
			}
			os.Exit(1)
		}
		os.Stdout.Write(val)
	case "put":
		var value interfaces.Value
		if len(args) == 3 {
			value = interfaces.Value(args[2])
		} else {
			value, err = io.ReadAll(os.Stdin)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
				os.Exit(1)
			}
		}
		if len(value) > interfaces.MaxValueSize {
			fmt.Fprintf(os.Stderr, "value larger than 1 MiB\n")
			os.Exit(1)
		}
		if err := c.Put(ctx, key, value); err != nil {
			fmt.Fprintf(os.Stderr, "put: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ok\n")
	}
}

func parseKey(s string) (interfaces.Key, error) {
	if len(s) != 32 {
		return interfaces.Key{}, fmt.Errorf("key must be 32 hex chars, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return interfaces.Key{}, err
	}
	var k interfaces.Key
	copy(k[:], b)
	return k, nil
}
