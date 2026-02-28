package checker

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"ai-kv-store/correctness"

	"github.com/anishathalye/porcupine"
)

var KVModel = porcupine.Model{
	Init:             KVInit,
	Step:             KVStep,
	Equal:            KVEqual,
	DescribeState:    KVDescribeState,
	DescribeOperation: func(input any, output any) string {
		return KVDescribeInput(input) + " -> " + KVDescribeOutput(output)
	},
}

type CheckResult struct {
	OK            bool
	Info          string
	TotalOps      int
	CheckedOps    int
	SkippedOps    int
	TimeoutOps    int
	ElapsedMs     int64
}

// CheckHistory runs the Porcupine linearizability checker on a recorded history.
//
// TIMEOUT handling strategy: operations that returned TIMEOUT have indeterminate
// outcomes. We set their Return time to math.MaxInt64 so Porcupine treats them
// as "possibly still pending" — the checker will try both including and excluding
// them from the linearization, which is sound.
func CheckHistory(events []correctness.HistoryEvent, timeout time.Duration) CheckResult {
	clientMap := make(map[string]int)
	nextClient := 0

	var ops []porcupine.Operation
	var histOps []HistoryOp
	skipped := 0
	timeouts := 0

	for _, ev := range events {
		if ev.Output == "ERROR" {
			skipped++
			continue
		}

		clientID, ok := clientMap[ev.ClientID]
		if !ok {
			clientID = nextClient
			clientMap[ev.ClientID] = clientID
			nextClient++
		}

		input := KVInput{
			OpType: ev.OpType,
			Key:    ev.Key,
			Value:  ev.InputValue,
		}
		output := KVOutput{
			Status: ev.Output,
			Value:  ev.OutputValue,
		}

		returnTime := ev.ReturnTime
		if ev.Output == "TIMEOUT" {
			returnTime = math.MaxInt64
			timeouts++
		}

		ops = append(ops, porcupine.Operation{
			ClientId: clientID,
			Input:    input,
			Output:   output,
			Call:      ev.CallTime,
			Return:    returnTime,
		})

		histOps = append(histOps, HistoryOp{
			ClientID: clientID,
			Call:     ev.CallTime,
			Return:   returnTime,
			Input:    input,
			Output:   output,
		})
	}

	if len(ops) == 0 {
		return CheckResult{OK: true, Info: "no operations to check"}
	}

	// Additional safety: reads never return values never written
	if err := ValidateValue(histOps); err != nil {
		return CheckResult{
			OK:         false,
			Info:       fmt.Sprintf("safety violation: %v", err),
			TotalOps:   len(events),
			CheckedOps: len(ops),
			SkippedOps: skipped,
			TimeoutOps: timeouts,
		}
	}

	start := time.Now()
	result, info := porcupine.CheckOperationsVerbose(KVModel, ops, timeout)
	elapsed := time.Since(start)

	ok := result == porcupine.Ok
	infoStr := ""
	switch result {
	case porcupine.Ok:
		infoStr = "PASS: history is linearizable"
	case porcupine.Illegal:
		infoStr = "FAIL: history is NOT linearizable"
	case porcupine.Unknown:
		infoStr = "UNKNOWN: checker timed out"
	}

	if !ok {
		visPath := "linearizability_violation.html"
		if err := porcupine.VisualizePath(KVModel, info, visPath); err == nil {
			infoStr += fmt.Sprintf(" (visualization: %s)", visPath)
		}
	}

	return CheckResult{
		OK:         ok,
		Info:       infoStr,
		TotalOps:   len(events),
		CheckedOps: len(ops),
		SkippedOps: skipped,
		TimeoutOps: timeouts,
		ElapsedMs:  elapsed.Milliseconds(),
	}
}

func RunCheckerOnFile(historyPath string, timeout time.Duration) (CheckResult, error) {
	events, err := correctness.LoadHistory(historyPath)
	if err != nil {
		return CheckResult{}, fmt.Errorf("load history: %w", err)
	}
	return CheckHistory(events, timeout), nil
}

func WriteCheckResult(path string, result CheckResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "Linearizability Check Result\n")
	fmt.Fprintf(f, "============================\n")
	fmt.Fprintf(f, "Result:      %s\n", boolToPassFail(result.OK))
	fmt.Fprintf(f, "Info:        %s\n", result.Info)
	fmt.Fprintf(f, "Total ops:   %d\n", result.TotalOps)
	fmt.Fprintf(f, "Checked ops: %d\n", result.CheckedOps)
	fmt.Fprintf(f, "Skipped:     %d (errors)\n", result.SkippedOps)
	fmt.Fprintf(f, "Timeouts:    %d (treated as pending)\n", result.TimeoutOps)
	fmt.Fprintf(f, "Check time:  %dms\n", result.ElapsedMs)
	return nil
}

func boolToPassFail(b bool) string {
	if b {
		return "PASS"
	}
	return "FAIL"
}

func ParseClientID(s string) int {
	s = strings.TrimPrefix(s, "w")
	n, _ := strconv.Atoi(s)
	return n
}
