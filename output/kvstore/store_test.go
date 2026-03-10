package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKVStoreCRUD(t *testing.T) {
	s := NewKVStore()

	// Put and Get
	s.Put("key1", []byte("value1"))
	v, ok := s.Get("key1")
	if !ok || string(v) != "value1" {
		t.Fatalf("expected value1, got %s (ok=%v)", v, ok)
	}

	// Get missing key
	_, ok = s.Get("missing")
	if ok {
		t.Fatal("expected not found for missing key")
	}

	// Overwrite
	s.Put("key1", []byte("value2"))
	v, ok = s.Get("key1")
	if !ok || string(v) != "value2" {
		t.Fatalf("expected value2 after overwrite, got %s", v)
	}

	// Delete
	s.Delete("key1")
	_, ok = s.Get("key1")
	if ok {
		t.Fatal("expected not found after delete")
	}

	// Delete idempotent
	s.Delete("key1")

	// Len
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	if s.Len() != 2 {
		t.Fatalf("expected len 2, got %d", s.Len())
	}
}

func TestKVStoreSnapshot(t *testing.T) {
	s := NewKVStore()
	s.Put("k1", []byte("v1"))
	s.Put("k2", []byte("v2"))

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot should have 2 keys, got %d", len(snap))
	}

	s2 := NewKVStore()
	s2.ApplySnapshot(snap)
	v, ok := s2.Get("k1")
	if !ok || string(v) != "v1" {
		t.Fatalf("snapshot apply failed for k1")
	}
}

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, entries, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries on fresh WAL, got %d", len(entries))
	}

	err = w.Append([]WALEntry{
		{Epoch: 1, Index: 1, OpType: OpPut, Key: "k1", Value: []byte("v1")},
		{Epoch: 1, Index: 2, OpType: OpPut, Key: "k2", Value: []byte("v2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = w.Append([]WALEntry{
		{Epoch: 1, Index: 3, OpType: OpDelete, Key: "k1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Reopen and replay
	w2, entries, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries on replay, got %d", len(entries))
	}
	if entries[0].Key != "k1" || entries[2].OpType != OpDelete {
		t.Fatalf("unexpected entry content: %+v", entries)
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, _, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		w.Append([]WALEntry{{Epoch: 1, Index: i, OpType: OpPut, Key: "k", Value: []byte("v")}})
	}

	err = w.Truncate(3)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, entries, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after truncate(3), got %d", len(entries))
	}
	if entries[0].Index != 4 || entries[1].Index != 5 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestWALGroupCommit(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	w, _, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// Submit many entries concurrently to exercise group commit
	errs := make(chan error, 100)
	for i := uint64(1); i <= 100; i++ {
		go func(idx uint64) {
			errs <- w.Append([]WALEntry{{Epoch: 1, Index: idx, OpType: OpPut, Key: "k", Value: []byte("v")}})
		}(i)
	}
	for i := 0; i < 100; i++ {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	w.Close()

	w2, entries, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(entries) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(entries))
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
