package porcupine

import (
	"fmt"
	"os"
	"sort"
	"time"
)

type EventKind int

const (
	CallEvent EventKind = iota
	ReturnEvent
)

type Event struct {
	ClientId int
	Kind     EventKind
	Value    any
	Id       int
	Time     int64
}

type Model struct {
	Init              func() any
	Step              func(state, input, output any) (bool, any)
	Equal             func(state1, state2 any) bool
	DescribeOperation func(input, output any) string
}

type CheckResult int

const (
	Unknown CheckResult = iota
	Ok
	Illegal
)

type op struct {
	id     int
	call   int64
	ret    int64
	input  any
	output any
}

type LinearizationInfo struct {
	Result      CheckResult
	CheckedOps  int
	PendingOps  int
	Explanation string
}

func CheckEventsVerbose(model Model, events []Event, timeout time.Duration) (CheckResult, *LinearizationInfo) {
	deadline := time.Now().Add(timeout)
	sort.Slice(events, func(i, j int) bool {
		if events[i].Time == events[j].Time {
			return events[i].Kind < events[j].Kind
		}
		return events[i].Time < events[j].Time
	})

	calls := map[int]Event{}
	ops := make([]op, 0, len(events)/2)
	pending := 0
	for _, e := range events {
		switch e.Kind {
		case CallEvent:
			calls[e.Id] = e
		case ReturnEvent:
			c, ok := calls[e.Id]
			if !ok {
				continue
			}
			delete(calls, e.Id)
			ops = append(ops, op{id: e.Id, call: c.Time, ret: e.Time, input: c.Value, output: e.Value})
		}
	}
	pending = len(calls)

	if len(ops) == 0 {
		return Ok, &LinearizationInfo{Result: Ok, CheckedOps: 0, PendingOps: pending, Explanation: "no completed operations"}
	}

	// Precedence matrix: if a.ret < b.call then a must appear before b.
	n := len(ops)
	mustBefore := make([][]bool, n)
	prereqs := make([]int, n)
	for i := 0; i < n; i++ {
		mustBefore[i] = make([]bool, n)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if ops[i].ret < ops[j].call {
				mustBefore[i][j] = true
				prereqs[j]++
			}
		}
	}

	used := make([]bool, n)
	lin := make([]int, 0, n)
	memo := map[string]bool{}

	var dfs func(state any, prereq []int) bool
	dfs = func(state any, prereq []int) bool {
		if timeout > 0 && time.Now().After(deadline) {
			return false
		}
		if len(lin) == n {
			return true
		}
		key := memoKey(used, state)
		if memo[key] {
			return false
		}
		memo[key] = true

		for i := 0; i < n; i++ {
			if used[i] || prereq[i] != 0 {
				continue
			}
			ok, ns := model.Step(state, ops[i].input, ops[i].output)
			if !ok {
				continue
			}
			used[i] = true
			lin = append(lin, i)
			nextPrereq := append([]int(nil), prereq...)
			for j := 0; j < n; j++ {
				if mustBefore[i][j] {
					nextPrereq[j]--
				}
			}
			if dfs(ns, nextPrereq) {
				return true
			}
			lin = lin[:len(lin)-1]
			used[i] = false
		}
		return false
	}

	ok := dfs(model.Init(), prereqs)
	if ok {
		return Ok, &LinearizationInfo{Result: Ok, CheckedOps: len(ops), PendingOps: pending, Explanation: "linearization found"}
	}
	if timeout > 0 && time.Now().After(deadline) {
		return Unknown, &LinearizationInfo{Result: Unknown, CheckedOps: len(ops), PendingOps: pending, Explanation: "timeout during search"}
	}
	return Illegal, &LinearizationInfo{Result: Illegal, CheckedOps: len(ops), PendingOps: pending, Explanation: "no legal linearization"}
}

func memoKey(used []bool, state any) string {
	buf := make([]byte, len(used))
	for i := range used {
		if used[i] {
			buf[i] = '1'
		} else {
			buf[i] = '0'
		}
	}
	return string(buf) + "|" + fmt.Sprintf("%v", state)
}

func VisualizePath(_ Model, info *LinearizationInfo, path string) error {
	if info == nil {
		return os.WriteFile(path, []byte("<html><body><h3>No info</h3></body></html>"), 0o644)
	}
	h := fmt.Sprintf("<html><body><h3>Result: %v</h3><p>%s</p><p>checked=%d pending=%d</p></body></html>", info.Result, info.Explanation, info.CheckedOps, info.PendingOps)
	return os.WriteFile(path, []byte(h), 0o644)
}
