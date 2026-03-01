package checker

import (
	"fmt"
	"math"
	"os"
	"sort"
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
	OK              bool
	Info            string
	TotalOps        int
	CheckedOps      int
	SkippedOps      int
	TimeoutOps      int
	ElapsedMs       int64
	LinearizedOps   int
	ViolationReport string
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

	cr := CheckResult{
		OK:         ok,
		Info:       infoStr,
		TotalOps:   len(events),
		CheckedOps: len(ops),
		SkippedOps: skipped,
		TimeoutOps: timeouts,
		ElapsedMs:  elapsed.Milliseconds(),
	}

	if !ok {
		visPath := "linearizability_violation.html"
		if err := porcupine.VisualizePath(KVModel, info, visPath); err == nil {
			cr.Info += fmt.Sprintf(" (visualization: %s)", visPath)
		}
		cr.ViolationReport, cr.LinearizedOps = buildViolationReport(ops, info, cr)
	}

	return cr
}

func RunCheckerOnFile(historyPath string, timeout time.Duration) (CheckResult, error) {
	events, err := correctness.LoadHistory(historyPath)
	if err != nil {
		return CheckResult{}, fmt.Errorf("load history: %w", err)
	}
	return CheckHistory(events, timeout), nil
}

func buildViolationReport(ops []porcupine.Operation, info porcupine.LinearizationInfo, cr CheckResult) (string, int) {
	var b strings.Builder

	b.WriteString("LINEARIZABILITY VIOLATION REPORT\n")
	b.WriteString("================================\n\n")

	partials := info.PartialLinearizationsOperations()
	longestLen := 0
	var longestPartial []porcupine.Operation
	for _, partitionPartials := range partials {
		for _, seq := range partitionPartials {
			if len(seq) > longestLen {
				longestLen = len(seq)
				longestPartial = seq
			}
		}
	}

	fmt.Fprintf(&b, "Checked operations:    %d\n", cr.CheckedOps)
	fmt.Fprintf(&b, "Linearizable prefix:   %d ops (%.1f%%)\n", longestLen, pct(longestLen, cr.CheckedOps))
	fmt.Fprintf(&b, "Unlinearizable ops:    %d\n\n", cr.CheckedOps-longestLen)

	linearizedIDs := make(map[string]bool)
	for _, op := range longestPartial {
		in := op.Input.(KVInput)
		out := op.Output.(KVOutput)
		key := fmt.Sprintf("%d|%d|%s|%s|%s", op.Call, op.Return, in.OpType, in.Key, out.Status)
		linearizedIDs[key] = true
	}

	var unlinOps []porcupine.Operation
	for _, op := range ops {
		in := op.Input.(KVInput)
		out := op.Output.(KVOutput)
		key := fmt.Sprintf("%d|%d|%s|%s|%s", op.Call, op.Return, in.OpType, in.Key, out.Status)
		if !linearizedIDs[key] {
			unlinOps = append(unlinOps, op)
		}
	}

	keyConflicts := make(map[string][]porcupine.Operation)
	for _, op := range unlinOps {
		in := op.Input.(KVInput)
		keyConflicts[in.Key] = append(keyConflicts[in.Key], op)
	}

	// --- Per-key violation analysis ---
	b.WriteString("PER-KEY VIOLATION ANALYSIS\n")
	b.WriteString("--------------------------\n")

	type keyInfo struct {
		key   string
		count int
	}
	var keys []keyInfo
	for k, v := range keyConflicts {
		keys = append(keys, keyInfo{k, len(v)})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].count > keys[j].count })

	if len(keys) == 0 {
		b.WriteString("(unable to isolate specific key violations from partial linearization)\n\n")
	} else {
		fmt.Fprintf(&b, "Affected keys: %d\n\n", len(keys))
		limit := 10
		if len(keys) < limit {
			limit = len(keys)
		}
		for i := 0; i < limit; i++ {
			ki := keys[i]
			fmt.Fprintf(&b, "  Key %s — %d unlinearizable ops:\n", shortKey(ki.key), ki.count)
			opLimit := 5
			if len(keyConflicts[ki.key]) < opLimit {
				opLimit = len(keyConflicts[ki.key])
			}
			for j := 0; j < opLimit; j++ {
				op := keyConflicts[ki.key][j]
				in := op.Input.(KVInput)
				out := op.Output.(KVOutput)
				fmt.Fprintf(&b, "    [client=%d] %s -> %s  (call=%d, return=%d)\n",
					op.ClientId, KVDescribeInput(in), KVDescribeOutput(out), op.Call, op.Return)
			}
			if len(keyConflicts[ki.key]) > opLimit {
				fmt.Fprintf(&b, "    ... and %d more\n", len(keyConflicts[ki.key])-opLimit)
			}
			b.WriteString("\n")
		}
		if len(keys) > limit {
			fmt.Fprintf(&b, "  ... and %d more affected keys\n\n", len(keys)-limit)
		}
	}

	// --- Full history of all ops on the most-affected key ---
	if len(keys) > 0 {
		worstKey := keys[0].key
		b.WriteString("FULL HISTORY FOR MOST-AFFECTED KEY\n")
		b.WriteString("-----------------------------------\n")
		fmt.Fprintf(&b, "Key: %s\n\n", worstKey)

		var keyOps []porcupine.Operation
		for _, op := range ops {
			in := op.Input.(KVInput)
			if in.Key == worstKey {
				keyOps = append(keyOps, op)
			}
		}
		sort.Slice(keyOps, func(i, j int) bool { return keyOps[i].Call < keyOps[j].Call })

		fmt.Fprintf(&b, "  %-8s %-8s %-30s %-20s %s\n", "CALL", "RETURN", "OPERATION", "RESULT", "STATUS")
		fmt.Fprintf(&b, "  %-8s %-8s %-30s %-20s %s\n", "--------", "--------", "------------------------------", "--------------------", "------")
		for _, op := range keyOps {
			in := op.Input.(KVInput)
			out := op.Output.(KVOutput)
			idKey := fmt.Sprintf("%d|%d|%s|%s|%s", op.Call, op.Return, in.OpType, in.Key, out.Status)
			status := "linearized"
			if !linearizedIDs[idKey] {
				status = "** VIOLATION **"
			}
			retStr := fmt.Sprintf("%d", op.Return)
			if op.Return == math.MaxInt64 {
				retStr = "PENDING"
			}
			fmt.Fprintf(&b, "  %-8d %-8s %-30s %-20s %s\n",
				op.Call, retStr, KVDescribeInput(in), KVDescribeOutput(out), status)
		}
		b.WriteString("\n")
	}

	// --- Linearized prefix (last N ops) ---
	if longestLen > 0 {
		b.WriteString("LINEARIZED PREFIX (last 20 operations)\n")
		b.WriteString("---------------------------------------\n")
		startIdx := 0
		if longestLen > 20 {
			startIdx = longestLen - 20
		}
		for i := startIdx; i < longestLen; i++ {
			op := longestPartial[i]
			in := op.Input.(KVInput)
			out := op.Output.(KVOutput)
			fmt.Fprintf(&b, "  %4d. [client=%d] %s -> %s\n",
				i+1, op.ClientId, KVDescribeInput(in), KVDescribeOutput(out))
		}
		b.WriteString("\n")
	}

	// --- Operation type breakdown ---
	b.WriteString("OPERATION BREAKDOWN\n")
	b.WriteString("--------------------\n")
	typeCounts := map[string][2]int{} // [total, unlinearizable]
	for _, op := range ops {
		in := op.Input.(KVInput)
		c := typeCounts[in.OpType]
		c[0]++
		typeCounts[in.OpType] = c
	}
	for _, op := range unlinOps {
		in := op.Input.(KVInput)
		c := typeCounts[in.OpType]
		c[1]++
		typeCounts[in.OpType] = c
	}
	for _, opType := range []string{"Get", "Put", "Delete"} {
		c := typeCounts[opType]
		if c[0] > 0 {
			fmt.Fprintf(&b, "  %-8s total=%-6d unlinearizable=%-6d (%.1f%%)\n",
				opType, c[0], c[1], pct(c[1], c[0]))
		}
	}
	b.WriteString("\n")

	return b.String(), longestLen
}

func pct(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom) * 100
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

	if result.ViolationReport != "" {
		fmt.Fprintf(f, "\n%s", result.ViolationReport)
	}

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
