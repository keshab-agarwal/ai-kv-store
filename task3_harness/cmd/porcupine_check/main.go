package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"base/task3_harness/internal/harness"

	"github.com/anishathalye/porcupine"
)

type Input struct {
	Type string
	Key  string
	Val  string
}

type Output struct {
	Status string
	Val    string
}

func model() porcupine.Model {
	return porcupine.Model{
		Init: func() any {
			return map[string]string{}
		},
		Step: func(state, in, out any) (bool, any) {
			s := copyState(state.(map[string]string))
			i := in.(Input)
			o := out.(Output)
			switch i.Type {
			case "put":
				if o.Status != "OK" {
					return false, s
				}
				s[i.Key] = i.Val
				return true, s
			case "delete":
				if o.Status != "OK" {
					return false, s
				}
				delete(s, i.Key)
				return true, s
			case "get":
				v, ok := s[i.Key]
				if !ok {
					return o.Status == "NOT_FOUND", s
				}
				return o.Status == "FOUND" && o.Val == v, s
			default:
				return false, s
			}
		},
		Equal: func(a, b any) bool {
			return equalMap(a.(map[string]string), b.(map[string]string))
		},
		DescribeOperation: func(input, output any) string {
			i := input.(Input)
			o := output.(Output)
			return fmt.Sprintf("%s(%s,%s)->%s/%s", i.Type, i.Key, i.Val, o.Status, o.Val)
		},
	}
}

func main() {
	var (
		history = flag.String("history", "", "history jsonl")
		outDir  = flag.String("out-dir", ".", "output directory")
		self    = flag.Bool("selftest", false, "run built-in pass/fail self-test")
	)
	flag.Parse()

	if *self {
		if err := runSelfTest(*outDir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("SELFTEST PASS")
		return
	}

	if *history == "" {
		fmt.Fprintln(os.Stderr, "-history required")
		os.Exit(1)
	}

	events, err := loadEvents(*history)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	res, info := porcupine.CheckEventsVerbose(model(), events, 30*time.Second)
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if info != nil {
		_ = porcupine.VisualizePath(model(), info, filepath.Join(*outDir, "linearizability.html"))
	}
	if res == porcupine.Ok {
		fmt.Println("PASS linearizable")
		return
	}
	fmt.Println("FAIL not linearizable")
	os.Exit(2)
}

func loadEvents(path string) ([]porcupine.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024), 4<<20)

	var events []porcupine.Event
	for sc.Scan() {
		var r harness.HistoryRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("decode history: %w", err)
		}
		callNs := r.CallTime.UnixNano()
		in := Input{Type: string(r.OpType), Key: r.Key, Val: base64.StdEncoding.EncodeToString(r.InputValue)}
		events = append(events, porcupine.Event{ClientId: r.ClientID, Id: stableID(r.OpID), Kind: porcupine.CallEvent, Value: in, Time: callNs})
		if r.ReturnTime == nil || r.Status == harness.StatusTimeout {
			continue
		}
		out := Output{Status: string(r.Status), Val: base64.StdEncoding.EncodeToString(r.OutputValue)}
		events = append(events, porcupine.Event{ClientId: r.ClientID, Id: stableID(r.OpID), Kind: porcupine.ReturnEvent, Value: out, Time: r.ReturnTime.UnixNano()})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Time < events[j].Time })
	return events, nil
}

func stableID(opID string) int {
	h := 0
	for i := 0; i < len(opID); i++ {
		h = (h*131 + int(opID[i])) & 0x7fffffff
	}
	return h
}

func copyState(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func equalMap(a, b map[string]string) bool {
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

func runSelfTest(outDir string) error {
	goodPath := filepath.Join(outDir, "selftest_good.jsonl")
	badPath := filepath.Join(outDir, "selftest_bad.jsonl")
	if err := writeSelfHistories(goodPath, badPath); err != nil {
		return err
	}
	goodEv, err := loadEvents(goodPath)
	if err != nil {
		return err
	}
	if res, _ := porcupine.CheckEventsVerbose(model(), goodEv, 5*time.Second); res != porcupine.Ok {
		return errors.New("selftest expected good history to pass")
	}
	badEv, err := loadEvents(badPath)
	if err != nil {
		return err
	}
	if res, _ := porcupine.CheckEventsVerbose(model(), badEv, 5*time.Second); res == porcupine.Ok {
		return errors.New("selftest expected bad history to fail")
	}
	return nil
}

func writeSelfHistories(goodPath, badPath string) error {
	now := time.Now().UTC()
	good := []harness.HistoryRecord{
		{OpID: "g1", ClientID: 0, CallTime: now, ReturnTime: tptr(now.Add(1 * time.Millisecond)), OpType: harness.OpPut, Key: "00000000000000000000000000000001", InputValue: []byte("v1"), Status: harness.StatusOK},
		{OpID: "g2", ClientID: 1, CallTime: now.Add(2 * time.Millisecond), ReturnTime: tptr(now.Add(3 * time.Millisecond)), OpType: harness.OpGet, Key: "00000000000000000000000000000001", OutputValue: []byte("v1"), Status: harness.StatusFound},
	}
	bad := []harness.HistoryRecord{
		{OpID: "b1", ClientID: 0, CallTime: now, ReturnTime: tptr(now.Add(1 * time.Millisecond)), OpType: harness.OpPut, Key: "00000000000000000000000000000002", InputValue: []byte("vA"), Status: harness.StatusOK},
		{OpID: "b2", ClientID: 1, CallTime: now.Add(2 * time.Millisecond), ReturnTime: tptr(now.Add(3 * time.Millisecond)), OpType: harness.OpGet, Key: "00000000000000000000000000000002", OutputValue: []byte("vB"), Status: harness.StatusFound},
	}
	if err := writeHistory(goodPath, good); err != nil {
		return err
	}
	if err := writeHistory(badPath, bad); err != nil {
		return err
	}
	return nil
}

func writeHistory(path string, rows []harness.HistoryRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	for _, r := range rows {
		b, _ := json.Marshal(r)
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func tptr(t time.Time) *time.Time { return &t }
