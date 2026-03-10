package correctness

import (
	"github.com/anishathalye/porcupine"
)

type KVInput struct {
	Op    int
	Key   int
	Value int
}

type KVOutput struct {
	Value int
}

const (
	OpPut = iota
	OpGet
)

// CheckLinearizability checks if the given history is linearizable.
func CheckLinearizability(history []porcupine.Operation) bool {
	model := porcupine.Model{
		Init: func() interface{} { return make(map[int]int) },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(map[int]int)
			in := input.(KVInput)
			out := output.(KVOutput)

			switch in.Op {
			case OpPut:
				st[in.Key] = in.Value
				return true, st
			case OpGet:
				return st[in.Key] == out.Value, st
			}
			return false, st
		},
		Equal: func(state1, state2 interface{}) bool {
			st1 := state1.(map[int]int)
			st2 := state2.(map[int]int)
			if len(st1) != len(st2) {
				return false
			}
			for k, v := range st1 {
				if st2[k] != v {
					return false
				}
			}
			return true
		},
	}
	return porcupine.CheckOperations(model, history)
}
