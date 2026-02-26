package version

import (
	"kv-store/interfaces"
	"testing"
)

func TestVectorLEQ(t *testing.T) {
	v := Vector{0, 0, 0}
	w := Vector{1, 1, 1}
	if !v.LEQ(w) {
		t.Error("v should be LEQ w")
	}
	if w.LEQ(v) {
		t.Error("w should not be LEQ v")
	}
	if !v.LEQ(v) {
		t.Error("v LEQ v")
	}
}

func TestVectorDominates(t *testing.T) {
	v := Vector{1, 1, 1}
	w := Vector{0, 0, 0}
	if !v.Dominates(w) {
		t.Error("v should dominate w")
	}
	if w.Dominates(v) {
		t.Error("w should not dominate v")
	}
	if v.Dominates(v) {
		t.Error("v should not dominate itself")
	}
}

func TestVectorMerge(t *testing.T) {
	v := Vector{1, 0, 2}
	w := Vector{0, 2, 1}
	m := v.Merge(w)
	if m != (Vector{1, 2, 2}) {
		t.Errorf("merge got %v", m)
	}
}

func TestVectorInc(t *testing.T) {
	v := Vector{1, 2, 3}
	v1 := v.Inc(interfaces.NodeID(0))
	if v1[0] != 2 || v1[1] != 2 || v1[2] != 3 {
		t.Errorf("inc got %v", v1)
	}
}
