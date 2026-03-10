package correctness

import (
	"testing"
	"github.com/anishathalye/porcupine"
)

func TestLinearizability(t *testing.T) {
	goodHistory := []porcupine.Operation{
		{Input: KVInput{Op: OpPut, Key: 1, Value: 1}, Output: KVOutput{Value: 0}},
		{Input: KVInput{Op: OpGet, Key: 1}, Output: KVOutput{Value: 1}},
	}

	badHistory := []porcupine.Operation{
		{Input: KVInput{Op: OpPut, Key: 1, Value: 1}, Output: KVOutput{Value: 0}},
		{Input: KVInput{Op: OpGet, Key: 1}, Output: KVOutput{Value: 0}},
	}

	if !CheckLinearizability(goodHistory) {
		t.Error("expected good history to be linearizable")
	}

	if CheckLinearizability(badHistory) {
		t.Error("expected bad history to not be linearizable")
	}
}
