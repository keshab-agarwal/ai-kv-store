package server

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"kvstore/client"
	"kvstore/storage"
)

func TestKVServer(t *testing.T) {
	db := storage.NewEngine()
	listener, grpcServer, err := StartServer(db, ":0")
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer grpcServer.Stop()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := client.NewKVClient(conn)

	key := []byte("key")
	value := []byte("value")

	// Test Put
	err = client.Put(context.Background(), key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	gotValue, err := client.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(gotValue) != string(value) {
		t.Fatalf("expected %s, got %s", value, gotValue)
	}

	// Test Delete
	err = client.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Test Get after Delete
	gotValue, err = client.Get(context.Background(), key)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
