package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/ycsb"
)

// -----------------------------------------------------------------------
// Report types
// -----------------------------------------------------------------------

type OpStats struct {
	Count  int64   `json:"count"`
	P50ms  float64 `json:"p50_ms"`
	P95ms  float64 `json:"p95_ms"`
	P99ms  float64 `json:"p99_ms"`
	P999ms float64 `json:"p999_ms"`
	AvgMs  float64 `json:"avg_ms"`
	Errors int64   `json:"errors"`
}

type RunReport struct {
	ThroughputOpsSec float64            `json:"throughput_ops_sec"`
	TotalOps         int64              `json:"total_ops"`
	DurationSec      float64            `json:"duration_sec"`
	PerOperation     map[string]OpStats `json:"per_operation"`
}

type LoadReport struct {
	TotalRecords int64   `json:"total_records"`
	DurationSec  float64 `json:"duration_sec"`
	ThroughputRs float64 `json:"throughput_records_sec"`
	Errors       int64   `json:"errors"`
}

// -----------------------------------------------------------------------
// Latency record (written to CSV)
// -----------------------------------------------------------------------

type latencyRecord struct {
	timestampNs int64
	operation   string
	latencyUs   int64
	status      string // "OK" or "ERR"
}

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------

func main() {
	nodes := flag.String("nodes", "localhost:16001,localhost:16002,localhost:16003", "comma-separated node addresses")
	recordCount := flag.Int64("recordcount", 10000, "number of records to load")
	operationCount := flag.Int64("operationcount", 50000, "number of run-phase operations")
	threads := flag.Int("threads", 8, "concurrent worker count")
	phase := flag.String("phase", "both", `"load", "run", or "both"`)
	outdir := flag.String("outdir", ".", "output directory for results")
	flag.Parse()

	if err := os.MkdirAll(*outdir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create outdir: %v\n", err)
		os.Exit(1)
	}

	client := ycsb.NewClient(*nodes)

	switch *phase {
	case "load":
		runLoad(client, *recordCount, *threads, *outdir)
	case "run":
		runRun(client, *recordCount, *operationCount, *threads, *outdir)
	case "both":
		runLoad(client, *recordCount, *threads, *outdir)
		runRun(client, *recordCount, *operationCount, *threads, *outdir)
	default:
		fmt.Fprintf(os.Stderr, "unknown phase %q\n", *phase)
		os.Exit(1)
	}
}

// -----------------------------------------------------------------------
// Load phase
// -----------------------------------------------------------------------

func runLoad(client *ycsb.Client, recordCount int64, threads int, outdir string) {
	fmt.Printf("=== LOAD PHASE: %d records, %d threads ===\n", recordCount, threads)

	var (
		nextID   int64
		mu       sync.Mutex
		records  []latencyRecord
		errCount int64
	)

	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				id := atomic.AddInt64(&nextID, 1) - 1
				if id >= recordCount {
					return
				}
				key := ycsb.KeyFromID(id)
				val := ycsb.RandomValue(rng)
				t0 := time.Now()
				err := client.Put(key, val)
				dur := time.Since(t0)
				status := "OK"
				if err != nil {
					status = "ERR"
					atomic.AddInt64(&errCount, 1)
				}
				rec := latencyRecord{
					timestampNs: t0.UnixNano(),
					operation:   "INSERT",
					latencyUs:   dur.Microseconds(),
					status:      status,
				}
				mu.Lock()
				records = append(records, rec)
				mu.Unlock()
			}
		}(int64(t) + 42)
	}
	wg.Wait()
	elapsed := time.Since(start)

	report := LoadReport{
		TotalRecords: recordCount,
		DurationSec:  elapsed.Seconds(),
		ThroughputRs: float64(recordCount) / elapsed.Seconds(),
		Errors:       errCount,
	}
	writeJSON(outdir+"/load_report.json", report)
	writeCSV(outdir+"/load_latencies.csv", records)

	fmt.Printf("Load done: %.2f sec, %.1f rec/s, %d errors\n",
		report.DurationSec, report.ThroughputRs, report.Errors)
}

// -----------------------------------------------------------------------
// Run phase
// -----------------------------------------------------------------------

func runRun(client *ycsb.Client, recordCount, operationCount int64, threads int, outdir string) {
	fmt.Printf("=== RUN PHASE: %d ops, %d threads, workload 95/4/1 ===\n", operationCount, threads)

	cfg := ycsb.WorkloadConfig{
		RecordCount:    recordCount,
		OperationCount: operationCount,
	}
	wl := ycsb.NewWorkload(cfg, 12345)

	var (
		opsIssued int64
		mu        sync.Mutex
		records   []latencyRecord
	)

	// Per-op latency collectors (unsorted slices; sorted later for percentiles).
	type collector struct {
		latencies []int64 // microseconds
		errors    int64
	}
	opCollectors := map[string]*collector{
		"READ":   {},
		"UPDATE": {},
		"DELETE": {},
	}

	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				idx := atomic.AddInt64(&opsIssued, 1) - 1
				if idx >= operationCount {
					return
				}
				op, key := wl.NextOp()
				var status string
				t0 := time.Now()
				switch op {
				case ycsb.OpRead:
					_, _, err := client.Get(key)
					if err != nil {
						status = "ERR"
					} else {
						status = "OK"
					}
				case ycsb.OpUpdate:
					val := ycsb.RandomValue(rng)
					err := client.Put(key, val)
					if err != nil {
						status = "ERR"
					} else {
						status = "OK"
					}
				case ycsb.OpDelete:
					err := client.Delete(key)
					if err != nil {
						status = "ERR"
					} else {
						status = "OK"
					}
				}
				dur := time.Since(t0)
				latUs := dur.Microseconds()
				opName := op.String()

				rec := latencyRecord{
					timestampNs: t0.UnixNano(),
					operation:   opName,
					latencyUs:   latUs,
					status:      status,
				}
				mu.Lock()
				records = append(records, rec)
				c := opCollectors[opName]
				c.latencies = append(c.latencies, latUs)
				if status == "ERR" {
					c.errors++
				}
				mu.Unlock()
			}
		}(int64(t) + 99)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Build report.
	perOp := make(map[string]OpStats)
	for name, c := range opCollectors {
		sort.Slice(c.latencies, func(i, j int) bool { return c.latencies[i] < c.latencies[j] })
		perOp[name] = OpStats{
			Count:  int64(len(c.latencies)),
			P50ms:  percentileMs(c.latencies, 0.50),
			P95ms:  percentileMs(c.latencies, 0.95),
			P99ms:  percentileMs(c.latencies, 0.99),
			P999ms: percentileMs(c.latencies, 0.999),
			AvgMs:  avgMs(c.latencies),
			Errors: c.errors,
		}
	}

	report := RunReport{
		ThroughputOpsSec: float64(operationCount) / elapsed.Seconds(),
		TotalOps:         operationCount,
		DurationSec:      math.Round(elapsed.Seconds()*1000) / 1000,
		PerOperation:     perOp,
	}
	writeJSON(outdir+"/run_report.json", report)
	writeCSV(outdir+"/run_latencies.csv", records)

	fmt.Printf("Run done: %.2f sec, %.1f ops/s\n", report.DurationSec, report.ThroughputOpsSec)
	for _, name := range []string{"READ", "UPDATE", "DELETE"} {
		s := perOp[name]
		fmt.Printf("  %s: count=%d avg=%.2fms p50=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms errors=%d\n",
			name, s.Count, s.AvgMs, s.P50ms, s.P95ms, s.P99ms, s.P999ms, s.Errors)
	}
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func percentileMs(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / 1000.0
}

func avgMs(vals []int64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum int64
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals)) / 1000.0
}

func writeJSON(path string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "json marshal error: %v\n", err)
		return
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
	}
}

func writeCSV(path string, records []latencyRecord) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", path, err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "timestamp_ns,operation,latency_us,status")
	for _, r := range records {
		fmt.Fprintf(f, "%d,%s,%d,%s\n", r.timestampNs, r.operation, r.latencyUs, r.status)
	}
}
