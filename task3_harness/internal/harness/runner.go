package harness

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	RunDir           string
	Seed             int64
	Duration         time.Duration
	Workers          int
	Endpoints        []string
	Keyspace         uint64
	MaxValueBytes    int
	RealisticSizes   bool
	InjectFault      bool
	CrashNode        int
	CrashAfter       time.Duration
	RecoverAfter     time.Duration
	ManageCluster    bool
	StartCmd         string
	StopCmd          string
	CrashCmd         string
	RecoverCmd       string
	CheckerBinary    string
	CheckerExtraArgs []string
}

type Recorder struct {
	mu   sync.Mutex
	file *os.File
	bw   *bufio.Writer
}

func NewRecorder(path string) (*Recorder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Recorder{file: f, bw: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (r *Recorder) Append(rec HistoryRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := r.bw.Write(b); err != nil {
		return err
	}
	if err := r.bw.WriteByte('\n'); err != nil {
		return err
	}
	return nil
}

func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.bw.Flush(); err != nil {
		_ = r.file.Close()
		return err
	}
	return r.file.Close()
}

func Run(cfg Config) error {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 20 * time.Second
	}
	if cfg.Keyspace == 0 {
		cfg.Keyspace = 1_000_000
	}
	if cfg.MaxValueBytes <= 0 {
		cfg.MaxValueBytes = 1 << 20
	}
	if cfg.CrashAfter <= 0 {
		cfg.CrashAfter = cfg.Duration / 3
	}
	if cfg.RecoverAfter <= 0 {
		cfg.RecoverAfter = 10 * time.Second
	}
	if err := os.MkdirAll(cfg.RunDir, 0o755); err != nil {
		return err
	}

	orch := CommandOrchestrator{
		StartCmd:   cfg.StartCmd,
		StopCmd:    cfg.StopCmd,
		CrashCmd:   cfg.CrashCmd,
		RecoverCmd: cfg.RecoverCmd,
	}
	if cfg.ManageCluster {
		if err := orch.StartCluster(); err != nil {
			return err
		}
		defer func() { _ = orch.StopCluster() }()
	}

	historyPath := filepath.Join(cfg.RunDir, "history.jsonl")
	rec, err := NewRecorder(historyPath)
	if err != nil {
		return err
	}
	defer func() { _ = rec.Close() }()

	harnessLog, err := os.Create(filepath.Join(cfg.RunDir, "harness.log"))
	if err != nil {
		return err
	}
	defer harnessLog.Close()
	logf := func(s string, args ...any) {
		_, _ = fmt.Fprintf(harnessLog, time.Now().Format(time.RFC3339Nano)+" "+s+"\n", args...)
	}

	rng := mrand.New(mrand.NewSource(cfg.Seed))
	client := NewKVClient(cfg.Endpoints, cfg.Seed+17)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	if cfg.InjectFault {
		crashNode := cfg.CrashNode
		if crashNode < 0 {
			crashNode = 1 + rng.Intn(maxInt(1, len(cfg.Endpoints)-1))
		}
		go func() {
			t := time.NewTimer(cfg.CrashAfter)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				logf("fault: crash node=%d", crashNode)
				_ = orch.CrashNode(crashNode)
			}
			t2 := time.NewTimer(cfg.RecoverAfter)
			select {
			case <-ctx.Done():
				return
			case <-t2.C:
				logf("fault: recover node=%d", crashNode)
				_ = orch.RecoverNode(crashNode)
			}
		}()
	}

	var ops, gets, puts, dels, timeouts, errors int64
	var opSeq atomic.Uint64
	wg := sync.WaitGroup{}
	for w := 0; w < cfg.Workers; w++ {
		workerID := w
		localRng := mrand.New(mrand.NewSource(cfg.Seed + int64(workerID)*1009))
		zipf := mrand.NewZipf(localRng, 1.05, 8, cfg.Keyspace-1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				opID := fmt.Sprintf("op-%d", opSeq.Add(1))
				call := time.Now().UTC()
				p := localRng.Float64()
				opType := OpGet
				if p < 0.95 {
					opType = OpGet
				} else if p < 0.99 {
					opType = OpPut
				} else {
					opType = OpDelete
				}
				knum := zipf.Uint64()
				key := encodeUint128(knum)
				opCtx, cancelOp := context.WithTimeout(ctx, 1200*time.Millisecond)
				recEntry := HistoryRecord{OpID: opID, ClientID: workerID, CallTime: call, OpType: opType, Key: key}
				switch opType {
				case OpGet:
					status, out, errMsg := client.Get(opCtx, key)
					if status == StatusFound {
						recEntry.OutputValue = out
					}
					recEntry.Status = status
					recEntry.Error = errMsg
					atomic.AddInt64(&gets, 1)
				case OpPut:
					v := makeValue(localRng, cfg.MaxValueBytes, cfg.RealisticSizes)
					recEntry.InputValue = v
					status, errMsg := client.Put(opCtx, key, v)
					recEntry.Status = status
					recEntry.Error = errMsg
					atomic.AddInt64(&puts, 1)
				case OpDelete:
					status, errMsg := client.Delete(opCtx, key)
					recEntry.Status = status
					recEntry.Error = errMsg
					atomic.AddInt64(&dels, 1)
				}
				cancelOp()

				if recEntry.Status == StatusTimeout {
					atomic.AddInt64(&timeouts, 1)
					// TIMEOUT strategy: leave return_time absent so operation is pending in history.
				} else {
					rt := time.Now().UTC()
					recEntry.ReturnTime = &rt
				}
				if recEntry.Status == StatusError {
					atomic.AddInt64(&errors, 1)
				}
				_ = rec.Append(recEntry)
				atomic.AddInt64(&ops, 1)
			}
		}()
	}
	wg.Wait()
	if err := rec.Close(); err != nil {
		return err
	}

	summary := RunSummary{
		Seed:       cfg.Seed,
		Duration:   cfg.Duration,
		Workers:    cfg.Workers,
		Operations: ops,
		Gets:       gets,
		Puts:       puts,
		Deletes:    dels,
		Timeouts:   timeouts,
		Errors:     errors,
	}

	checkerOutPath := filepath.Join(cfg.RunDir, "checker.out")
	checkerCmd := exec.Command(cfg.CheckerBinary, append([]string{"-history", historyPath, "-out-dir", cfg.RunDir}, cfg.CheckerExtraArgs...)...)
	b, err := checkerCmd.CombinedOutput()
	_ = os.WriteFile(checkerOutPath, b, 0o644)
	if err != nil {
		summary.CheckerResult = "FAIL"
		summary.CheckerNotes = strings.TrimSpace(string(b))
	} else {
		summary.CheckerResult = "PASS"
		summary.CheckerNotes = strings.TrimSpace(string(b))
	}

	summaryBytes, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(filepath.Join(cfg.RunDir, "summary.json"), summaryBytes, 0o644); err != nil {
		return err
	}
	logf("summary: %s", string(summaryBytes))
	copyNodeLogs(cfg.RunDir)
	return nil
}

func copyNodeLogs(runDir string) {
	_ = os.MkdirAll(filepath.Join(runDir, "node_logs"), 0o755)
	_ = exec.Command("zsh", "-lc", "cp -f .run/cluster/logs/node-*.log "+filepath.Join(runDir, "node_logs")+" 2>/dev/null || true").Run()
}

func makeValue(r *mrand.Rand, max int, realistic bool) []byte {
	n := 1024
	if realistic {
		x := math.Exp(r.NormFloat64()*0.9 + math.Log(1024))
		if x < 1 {
			x = 1
		}
		if x > float64(max) {
			x = float64(max)
		}
		n = int(x)
	} else {
		n = 32 + r.Intn(minInt(max, 4096))
	}
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return buf
}

func encodeUint128(u uint64) string {
	b := make([]byte, 16)
	for i := 0; i < 8; i++ {
		b[15-i] = byte(u >> (8 * i))
	}
	return hex.EncodeToString(b)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func PercentilesFromHistory(path string) (p95, p99 float64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	lat := make([]float64, 0, 1_000)
	for sc.Scan() {
		var r HistoryRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		if r.ReturnTime == nil {
			continue
		}
		lat = append(lat, r.ReturnTime.Sub(r.CallTime).Seconds()*1000)
	}
	if err := sc.Err(); err != nil {
		return 0, 0, err
	}
	if len(lat) == 0 {
		return 0, 0, nil
	}
	sort.Float64s(lat)
	p95 = lat[int(0.95*float64(len(lat)-1))]
	p99 = lat[int(0.99*float64(len(lat)-1))]
	return p95, p99, nil
}
