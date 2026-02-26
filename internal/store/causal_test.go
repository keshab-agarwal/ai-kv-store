package store

import (
	"kv-store/interfaces"
	"kv-store/internal/version"
	"testing"
)

func TestCausalStorePutGet(t *testing.T) {
	s := NewCausalStore(0)
	var key interfaces.Key
	key[0] = 1
	value := interfaces.Value([]byte("hello"))
	vv := s.Put(key, value, version.Vector{})
	if vv[0] != 1 {
		t.Errorf("expected vv[0]=1 got %v", vv)
	}
	got, gv, ok := s.Get(key, version.Vector{})
	if !ok {
		t.Fatal("get not found")
	}
	if string(got) != "hello" {
		t.Errorf("got %q", got)
	}
	if gv != vv {
		t.Errorf("vv mismatch %v %v", gv, vv)
	}
}

func TestCausalStoreVisibility(t *testing.T) {
	s := NewCausalStore(0)
	var key interfaces.Key
	key[0] = 1
	s.Put(key, interfaces.Value([]byte("v1")), version.Vector{})
	vv2 := s.Put(key, interfaces.Value([]byte("v2")), version.Vector{})
	got, _, ok := s.Get(key, vv2)
	if !ok {
		t.Fatal("get not found")
	}
	if string(got) != "v2" {
		t.Errorf("expected v2 got %q", got)
	}
	got, _, ok = s.Get(key, version.Vector{})
	if !ok {
		t.Fatal("get not found with zero vv")
	}
	if string(got) != "v2" {
		t.Errorf("zero vv should see latest: got %q", got)
	}
}

func TestKeyOwner(t *testing.T) {
	var k1, k2 interfaces.Key
	k1[0] = 0
	k2[0] = 1
	o1 := KeyOwner(k1, 3)
	o2 := KeyOwner(k2, 3)
	if o1 == o2 && k1 == k2 {
		t.Error("different keys should often have different owners")
	}
}
