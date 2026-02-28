package checker

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// KVState represents the state of the KV store for Porcupine's sequential specification.
// It's a map from hex-encoded key -> hex-encoded value. Empty string means key absent.
type KVState map[string]string

type KVInput struct {
	OpType string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
}

type KVOutput struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
}

func KVInit() any {
	return make(KVState)
}

// KVStep applies an operation to the KV state and checks if the output is legal.
func KVStep(stateIface any, inputIface any, outputIface any) (bool, any) {
	state := copyState(stateIface.(KVState))
	input := inputIface.(KVInput)
	output := outputIface.(KVOutput)

	switch input.OpType {
	case "Put":
		state[input.Key] = input.Value
		// TIMEOUT is accepted: if placed here in linearization, write takes effect
		// but client couldn't confirm. Porcupine explores all valid placements.
		return output.Status == "OK" || output.Status == "TIMEOUT", state

	case "Delete":
		delete(state, input.Key)
		return output.Status == "OK" || output.Status == "TIMEOUT", state

	case "Get":
		if output.Status == "TIMEOUT" {
			return true, state
		}
		val, exists := state[input.Key]
		switch output.Status {
		case "FOUND":
			return exists && val == output.Value, state
		case "NOT_FOUND":
			return !exists, state
		default:
			return false, state
		}

	default:
		return false, state
	}
}

func KVEqual(s1, s2 any) bool {
	a := s1.(KVState)
	b := s2.(KVState)
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func KVDescribeState(s any) string {
	state := s.(KVState)
	data, _ := json.Marshal(state)
	if len(data) > 200 {
		return fmt.Sprintf("{%d keys...}", len(state))
	}
	return string(data)
}

func KVDescribeInput(input any) string {
	in := input.(KVInput)
	if in.Value != "" {
		truncVal := in.Value
		if len(truncVal) > 20 {
			truncVal = truncVal[:20] + "..."
		}
		return fmt.Sprintf("%s(%s, %s)", in.OpType, shortKey(in.Key), truncVal)
	}
	return fmt.Sprintf("%s(%s)", in.OpType, shortKey(in.Key))
}

func KVDescribeOutput(output any) string {
	out := output.(KVOutput)
	if out.Value != "" {
		truncVal := out.Value
		if len(truncVal) > 20 {
			truncVal = truncVal[:20] + "..."
		}
		return fmt.Sprintf("%s(%s)", out.Status, truncVal)
	}
	return out.Status
}

func copyState(s KVState) KVState {
	cp := make(KVState, len(s))
	for k, v := range s {
		cp[k] = v
	}
	return cp
}

func shortKey(hexKey string) string {
	if len(hexKey) > 8 {
		return hexKey[:8] + ".."
	}
	return hexKey
}

// ValidateValue checks that an output value was actually written at some point.
// This is an additional safety property: reads never return values never written.
func ValidateValue(events []HistoryOp) error {
	writtenValues := make(map[string]map[string]bool) // key -> set of values

	for _, ev := range events {
		if ev.Input.OpType == "Put" {
			k := ev.Input.Key
			if writtenValues[k] == nil {
				writtenValues[k] = make(map[string]bool)
			}
			writtenValues[k][ev.Input.Value] = true
		}
	}

	for _, ev := range events {
		if ev.Input.OpType == "Get" && ev.Output.Status == "FOUND" {
			k := ev.Input.Key
			if writtenValues[k] == nil || !writtenValues[k][ev.Output.Value] {
				kb, _ := hex.DecodeString(k)
				return fmt.Errorf("read returned value never written for key %x", kb)
			}
		}
	}
	return nil
}

type HistoryOp struct {
	ClientID int
	Call     int64
	Return   int64
	Input    KVInput
	Output   KVOutput
}
