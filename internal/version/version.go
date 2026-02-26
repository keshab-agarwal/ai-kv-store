package version

import "kv-store/interfaces"

type Vector [3]uint64

func (v Vector) Dominates(w Vector) bool {
	ge := true
	for i := 0; i < 3; i++ {
		if v[i] < w[i] {
			ge = false
			break
		}
	}
	return ge && v != w
}

func (v Vector) LEQ(w Vector) bool {
	for i := 0; i < 3; i++ {
		if v[i] > w[i] {
			return false
		}
	}
	return true
}

func (v Vector) Merge(w Vector) Vector {
	var out Vector
	for i := 0; i < 3; i++ {
		if w[i] > v[i] {
			out[i] = w[i]
		} else {
			out[i] = v[i]
		}
	}
	return out
}

func (v Vector) Inc(node interfaces.NodeID) Vector {
	if node >= 3 {
		return v
	}
	out := v
	out[node]++
	return out
}
