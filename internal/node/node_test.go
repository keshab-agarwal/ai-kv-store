package node

import (
	"kv-store/interfaces"
	"kv-store/internal/version"
	"os"
	"testing"
)

func TestPutValueTooLarge(t *testing.T) {
	dir, _ := os.MkdirTemp("", "nodetest")
	defer os.RemoveAll(dir)
	addrs := []string{"127.0.0.1:19190", "127.0.0.1:19191", "127.0.0.1:19192"}
	n, err := New(0, dir, addrs)
	if err != nil {
		t.Fatal(err)
	}
	req := PutReq{
		Key:     interfaces.Key{},
		Value:   make(interfaces.Value, interfaces.MaxValueSize+1),
		ClientVV: version.Vector{},
	}
	var resp PutResp
	_ = n.Put(req, &resp)
	if resp.Err != interfaces.ErrValueTooLarge.Error() {
		t.Errorf("expected ErrValueTooLarge got %q", resp.Err)
	}
}

func TestNewRequiresThreePeers(t *testing.T) {
	dir, _ := os.MkdirTemp("", "nodetest")
	defer os.RemoveAll(dir)
	_, err := New(0, dir, []string{"a", "b"})
	if err == nil {
		t.Error("expected error for 2 peers")
	}
}

