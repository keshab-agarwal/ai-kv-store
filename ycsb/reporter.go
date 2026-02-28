package ycsb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

type Reporter struct {
	mu        sync.Mutex
	startTime time.Time
	latencies map[OpKind][]time.Duration
	errors    map[OpKind]int64
}

func NewReporter() *Reporter {
	return &Reporter{
		startTime: time.Now(),
		latencies: make(map[OpKind][]time.Duration),
		errors:    make(map[OpKind]int64),
	}
}

func (r *Reporter) Record(op OpKind, latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latencies[op] = append(r.latencies[op], latency)
}

func (r *Reporter) RecordError(op OpKind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors[op]++
}

type LatencyStats struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min_ms"`
	Max    float64 `json:"max_ms"`
	Mean   float64 `json:"mean_ms"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	P999   float64 `json:"p999_ms"`
	Errors int64   `json:"errors"`
}

type Report struct {
	TotalOps    int                  `json:"total_ops"`
	ElapsedSec  float64              `json:"elapsed_sec"`
	Throughput  float64              `json:"throughput_ops_sec"`
	PerOp       map[string]LatencyStats `json:"per_operation"`
}

func (r *Reporter) Finalize() Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := time.Since(r.startTime)
	totalOps := 0
	perOp := make(map[string]LatencyStats)

	for op, lats := range r.latencies {
		totalOps += len(lats)
		stats := computeStats(lats)
		stats.Errors = r.errors[op]
		perOp[opKindName(op)] = stats
	}

	return Report{
		TotalOps:   totalOps,
		ElapsedSec: elapsed.Seconds(),
		Throughput: float64(totalOps) / elapsed.Seconds(),
		PerOp:      perOp,
	}
}

func (r *Reporter) PrintReport(w io.Writer) {
	report := r.Finalize()
	fmt.Fprintf(w, "\n=== YCSB Benchmark Results ===\n")
	fmt.Fprintf(w, "Total operations: %d\n", report.TotalOps)
	fmt.Fprintf(w, "Elapsed time:     %.2f sec\n", report.ElapsedSec)
	fmt.Fprintf(w, "Throughput:        %.0f ops/sec\n\n", report.Throughput)

	for name, stats := range report.PerOp {
		fmt.Fprintf(w, "[%s] count=%d errors=%d\n", name, stats.Count, stats.Errors)
		fmt.Fprintf(w, "  min=%.3fms mean=%.3fms max=%.3fms\n", stats.Min, stats.Mean, stats.Max)
		fmt.Fprintf(w, "  p50=%.3fms p95=%.3fms p99=%.3fms p999=%.3fms\n\n", stats.P50, stats.P95, stats.P99, stats.P999)
	}
}

func (r *Reporter) WriteJSON(path string) error {
	report := r.Finalize()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (r *Reporter) WriteLatencyCSV(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "operation,latency_us")
	for op, lats := range r.latencies {
		name := opKindName(op)
		for _, l := range lats {
			fmt.Fprintf(f, "%s,%d\n", name, l.Microseconds())
		}
	}
	return nil
}

func computeStats(lats []time.Duration) LatencyStats {
	if len(lats) == 0 {
		return LatencyStats{}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	var sum time.Duration
	for _, l := range lats {
		sum += l
	}

	n := len(lats)
	return LatencyStats{
		Count: n,
		Min:   float64(lats[0].Microseconds()) / 1000,
		Max:   float64(lats[n-1].Microseconds()) / 1000,
		Mean:  float64(sum.Microseconds()) / float64(n) / 1000,
		P50:   float64(lats[percentileIdx(n, 0.50)].Microseconds()) / 1000,
		P95:   float64(lats[percentileIdx(n, 0.95)].Microseconds()) / 1000,
		P99:   float64(lats[percentileIdx(n, 0.99)].Microseconds()) / 1000,
		P999:  float64(lats[percentileIdx(n, 0.999)].Microseconds()) / 1000,
	}
}

func percentileIdx(n int, p float64) int {
	idx := int(float64(n) * p)
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func opKindName(op OpKind) string {
	switch op {
	case OpRead:
		return "READ"
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDel:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}
