package ycsb

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const (
	OpRead   OpType = "READ"
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)

type latencyRecord struct {
	Op      OpType
	Latency time.Duration
}

type Reporter struct {
	mu        sync.Mutex
	records   []latencyRecord
	startTime time.Time
	endTime   time.Time
	opCount   atomic.Int64
	errCount  atomic.Int64
}

func NewReporter() *Reporter {
	return &Reporter{
		records: make([]latencyRecord, 0, 1_000_000),
	}
}

func (r *Reporter) Start() { r.startTime = time.Now() }
func (r *Reporter) Stop()  { r.endTime = time.Now() }

func (r *Reporter) Record(op OpType, latency time.Duration, err error) {
	r.opCount.Add(1)
	if err != nil {
		r.errCount.Add(1)
	}
	r.mu.Lock()
	r.records = append(r.records, latencyRecord{Op: op, Latency: latency})
	r.mu.Unlock()
}

type opStats struct {
	Count  int     `json:"count"`
	MinMs  float64 `json:"min_ms"`
	MaxMs  float64 `json:"max_ms"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p999_ms"`
}

type report struct {
	ThroughputOpsSec float64            `json:"throughput_ops_sec"`
	ElapsedSec       float64            `json:"elapsed_sec"`
	TotalOps         int64              `json:"total_ops"`
	Errors           int64              `json:"errors"`
	PerOperation     map[string]opStats `json:"per_operation"`
}

func (r *Reporter) buildReport() report {
	elapsed := r.endTime.Sub(r.startTime)
	totalOps := r.opCount.Load()

	perOp := make(map[OpType][]float64)
	for _, rec := range r.records {
		ms := float64(rec.Latency.Microseconds()) / 1000.0
		perOp[rec.Op] = append(perOp[rec.Op], ms)
	}

	stats := make(map[string]opStats)
	for op, latencies := range perOp {
		sort.Float64s(latencies)
		n := len(latencies)
		if n == 0 {
			continue
		}
		var sum float64
		for _, v := range latencies {
			sum += v
		}
		stats[string(op)] = opStats{
			Count:  n,
			MinMs:  latencies[0],
			MaxMs:  latencies[n-1],
			MeanMs: sum / float64(n),
			P50Ms:  pct(latencies, 0.50),
			P95Ms:  pct(latencies, 0.95),
			P99Ms:  pct(latencies, 0.99),
			P999Ms: pct(latencies, 0.999),
		}
	}

	var throughput float64
	if elapsed > 0 {
		throughput = float64(totalOps) / elapsed.Seconds()
	}

	return report{
		ThroughputOpsSec: throughput,
		ElapsedSec:       elapsed.Seconds(),
		TotalOps:         totalOps,
		Errors:           r.errCount.Load(),
		PerOperation:     stats,
	}
}

func pct(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(p * float64(n-1))
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

func (r *Reporter) PrintReport(w io.Writer) {
	rpt := r.buildReport()
	fmt.Fprintf(w, "Throughput: %.2f ops/sec\n", rpt.ThroughputOpsSec)
	fmt.Fprintf(w, "Elapsed:    %.3f sec\n", rpt.ElapsedSec)
	fmt.Fprintf(w, "Total ops:  %d\n", rpt.TotalOps)
	fmt.Fprintf(w, "Errors:     %d\n", rpt.Errors)
	for op, s := range rpt.PerOperation {
		fmt.Fprintf(w, "\n%s (n=%d):\n", op, s.Count)
		fmt.Fprintf(w, "  min=%.3fms  mean=%.3fms  max=%.3fms\n", s.MinMs, s.MeanMs, s.MaxMs)
		fmt.Fprintf(w, "  p50=%.3fms  p95=%.3fms  p99=%.3fms  p999=%.3fms\n", s.P50Ms, s.P95Ms, s.P99Ms, s.P999Ms)
	}
}

func (r *Reporter) WriteJSON(path string) error {
	rpt := r.buildReport()
	data, err := json.MarshalIndent(rpt, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (r *Reporter) WriteLatencyCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Write([]string{"operation", "latency_us"})
	for _, rec := range r.records {
		w.Write([]string{
			string(rec.Op),
			strconv.FormatInt(rec.Latency.Microseconds(), 10),
		})
	}
	w.Flush()
	return w.Error()
}
