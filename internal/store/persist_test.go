package store

import (
	"kv-store/interfaces"
	"kv-store/internal/version"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistRoundtrip(t *testing.T) {
	dir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "data")
	s := NewCausalStore(0)
	var key interfaces.Key
	key[0] = 1
	s.Put(key, interfaces.Value([]byte("hello")), version.Vector{})
	if err := s.Save(path); err != nil {
		t.Fatal(err)
	}
	s2 := NewCausalStore(0)
	if err := s2.Load(path); err != nil {
		t.Fatal(err)
	}
	got, _, ok := s2.Get(key, version.Vector{})
	if !ok {
		t.Fatal("get not found after load")
	}
	if string(got) != "hello" {
		t.Errorf("got %q", got)
	}
}
