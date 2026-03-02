package ycsb

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/kvstore"
)

// sample records a single operation's latency and outcome status string.
type sample struct {
	latencyUs int64
	status    string
}

// Reporter collects per-operation latency samples and produces benchmark reports.
// All methods are safe for concurrent use.
type Reporter struct {
	mu        sync.Mutex
	startTime time.Time
	totalOps  int64         // accessed atomically
	samples   map[string][]sample // op name -> collected samples
	errors    map[string]int64
	timeouts  map[string]int64
}

// NewReporter creates a new Reporter and starts the benchmark clock.
func NewReporter() *Reporter {
	return &Reporter{
		startTime: time.Now(),
		samples:   make(map[string][]sample),
		errors:    make(map[string]int64),
		timeouts:  make(map[string]int64),
	}
}

// RecordOp records a single operation result.
// op is the operation name (e.g. "READ", "INSERT", "UPDATE", "DELETE").
// latencyUs is the operation latency in microseconds.
// status is the kvstore.Status returned by the operation.
func (r *Reporter) RecordOp(op string, latencyUs int64, status kvstore.Status) {
	atomic.AddInt64(&r.totalOps, 1)

	statusStr := status.String()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples[op] = append(r.samples[op], sample{latencyUs: latencyUs, status: statusStr})

	switch status {
	case kvstore.StatusError:
		r.errors[op]++
	case kvstore.StatusTimeout:
		r.timeouts[op]++
	}
}

// OpStats holds aggregated statistics for a single operation type.
type OpStats struct {
	Count    int64   `json:"count"`
	Errors   int64   `json:"errors"`
	Timeouts int64   `json:"timeouts"`
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
	P999Ms   float64 `json:"p999_ms"`
	MeanMs   float64 `json:"mean_ms"`
}

// Report holds the full benchmark report produced by Reporter.Report().
type Report struct {
	TotalOps         int64              `json:"total_ops"`
	DurationSec      float64            `json:"duration_sec"`
	ThroughputOpsSec float64            `json:"throughput_ops_sec"`
	PerOperation     map[string]OpStats `json:"per_operation"`
}

// percentileUs returns the given percentile (0.0–1.0) of a sorted slice of
// microsecond latencies, converted to milliseconds.
func percentileUs(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / 1000.0
}

// Report computes and returns the benchmark report from all recorded samples.
func (r *Reporter) Report() Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := time.Since(r.startTime).Seconds()
	total := atomic.LoadInt64(&r.totalOps)

	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(total) / elapsed
	}

	perOp := make(map[string]OpStats, len(r.samples))
	for op, opSamples := range r.samples {
		count := int64(len(opSamples))

		// Extract latencies for percentile calculation.
		lats := make([]int64, count)
		var sum int64
		for i, s := range opSamples {
			lats[i] = s.latencyUs
			sum += s.latencyUs
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

		meanMs := 0.0
		if count > 0 {
			meanMs = float64(sum) / float64(count) / 1000.0
		}

		perOp[op] = OpStats{
			Count:    count,
			Errors:   r.errors[op],
			Timeouts: r.timeouts[op],
			P50Ms:    percentileUs(lats, 0.50),
			P95Ms:    percentileUs(lats, 0.95),
			P99Ms:    percentileUs(lats, 0.99),
			P999Ms:   percentileUs(lats, 0.999),
			MeanMs:   meanMs,
		}
	}

	return Report{
		TotalOps:         total,
		DurationSec:      elapsed,
		ThroughputOpsSec: throughput,
		PerOperation:     perOp,
	}
}

// PrintReport writes a human-readable summary of the report to w.
func (r *Reporter) PrintReport(w io.Writer) {
	report := r.Report()
	fmt.Fprintf(w, "\n=== Benchmark Report ===\n")
	fmt.Fprintf(w, "Total ops:    %d\n", report.TotalOps)
	fmt.Fprintf(w, "Duration:     %.2fs\n", report.DurationSec)
	fmt.Fprintf(w, "Throughput:   %.0f ops/sec\n", report.ThroughputOpsSec)
	fmt.Fprintf(w, "\nPer-operation statistics:\n")

	// Print in a deterministic order.
	ops := make([]string, 0, len(report.PerOperation))
	for op := range report.PerOperation {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	for _, op := range ops {
		stats := report.PerOperation[op]
		fmt.Fprintf(w, "  %-8s count=%-8d errors=%-6d timeouts=%-6d "+
			"p50=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms mean=%.2fms\n",
			op, stats.Count, stats.Errors, stats.Timeouts,
			stats.P50Ms, stats.P95Ms, stats.P99Ms, stats.P999Ms, stats.MeanMs)
	}
	fmt.Fprintln(w)
}

// WriteJSON serialises the report to a JSON file at the given path.
// The JSON output uses the exact field names required by the evaluation script:
// throughput_ops_sec, per_operation.READ.p50_ms, etc.
func (r *Reporter) WriteJSON(path string) error {
	report := r.Report()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write report to %s: %w", path, err)
	}
	return nil
}

// WriteLatencyCSV writes per-operation latency samples to a CSV file.
// Format:
//
//	op,latency_us,status
//	READ,123,found
//	UPDATE,456,ok
func (r *Reporter) WriteLatencyCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv %s: %w", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"op", "latency_us", "status"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Write in deterministic operation order.
	ops := make([]string, 0, len(r.samples))
	for op := range r.samples {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	for _, op := range ops {
		for _, s := range r.samples[op] {
			row := []string{
				op,
				fmt.Sprintf("%d", s.latencyUs),
				s.status,
			}
			if err := w.Write(row); err != nil {
				return fmt.Errorf("write csv row: %w", err)
			}
		}
	}

	w.Flush()
	return w.Error()
}

// WriteCSV is an alias for WriteLatencyCSV for API compatibility.
func (r *Reporter) WriteCSV(path string) error {
	return r.WriteLatencyCSV(path)
}
