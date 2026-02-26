package client

import (
	"context"
	"kv-store/interfaces"
	"kv-store/internal/node"
	"os"
	"strconv"
	"testing"
	"time"
)

func startCluster(t *testing.T, portBase int) (addrs []string, cleanup func()) {
	t.Helper()
	addrs = []string{
		"127.0.0.1:" + strconv.Itoa(portBase),
		"127.0.0.1:" + strconv.Itoa(portBase+1),
		"127.0.0.1:" + strconv.Itoa(portBase+2),
	}
	dir0, _ := os.MkdirTemp("", "kv0")
	dir1, _ := os.MkdirTemp("", "kv1")
	dir2, _ := os.MkdirTemp("", "kv2")
	n0, err := node.New(0, dir0, addrs)
	if err != nil {
		t.Fatal(err)
	}
	n1, err := node.New(1, dir1, addrs)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := node.New(2, dir2, addrs)
	if err != nil {
		t.Fatal(err)
	}
	if err := n0.Listen(addrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := n1.Listen(addrs[1]); err != nil {
		t.Fatal(err)
	}
	if err := n2.Listen(addrs[2]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	cleanup = func() {
		_ = n0.Close()
		_ = n1.Close()
		_ = n2.Close()
		_ = os.RemoveAll(dir0)
		_ = os.RemoveAll(dir1)
		_ = os.RemoveAll(dir2)
	}
	return addrs, cleanup
}

func TestPutGet(t *testing.T) {
	addrs, cleanup := startCluster(t, 18180)
	defer cleanup()
	c, err := New(addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	var key interfaces.Key
	key[0] = 1
	if err := c.Put(ctx, key, interfaces.Value([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q", got)
	}
	// value too large
	err = c.Put(ctx, key, make(interfaces.Value, interfaces.MaxValueSize+1))
	if err != interfaces.ErrValueTooLarge {
		t.Errorf("expected ErrValueTooLarge got %v", err)
	}
}

func TestCausalConsistencyExample(t *testing.T) {
	addrs, cleanup := startCluster(t, 18280)
	defer cleanup()
	c1, _ := New(addrs[0])
	defer c1.Close()
	c2, _ := New(addrs[1])
	defer c2.Close()
	ctx := context.Background()
	var x, y interfaces.Key
	x[0], y[0] = 1, 2
	if err := c1.Put(ctx, x, interfaces.Value([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if err := c1.Put(ctx, y, interfaces.Value([]byte("world"))); err != nil {
		t.Fatal(err)
	}
	gotY, err := c2.Get(ctx, y)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotY) != "world" {
		t.Fatalf("c2 Get(y)=%q", gotY)
	}
	gotX, err := c2.Get(ctx, x)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotX) != "hello" {
		t.Errorf("writes follow reads: c2 must see x=hello after seeing y=world, got %q", gotX)
	}
}

func TestGetNotFound(t *testing.T) {
	addrs, cleanup := startCluster(t, 18380)
	defer cleanup()
	c, _ := New(addrs[0])
	defer c.Close()
	var key interfaces.Key
	key[0] = 99
	_, err := c.Get(context.Background(), key)
	if err != interfaces.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound got %v", err)
	}
}

