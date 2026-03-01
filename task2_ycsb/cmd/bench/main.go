// Command bench is a Go-based YCSB-compatible benchmark tool for the KV store.
//
// It generates workloads matching YCSB semantics (load + run phases) with
// configurable operation mix, key distribution, and concurrency. Results are
// written as structured JSON and CSV for post-processing.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/shared/types"
	"ai-kv-store/task2_ycsb/binding"
)

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

var (
	flagTarget           = flag.String("target", "localhost:9000", "KV store address (host:port)")
	flagThreads          = flag.Int("threads", 16, "Number of concurrent worker goroutines")
	flagRecordCount      = flag.Int("recordcount", 1000000, "Number of keys to load")
	flagOperationCount   = flag.Int("operationcount", 10000000, "Number of operations to run")
	flagDistribution     = flag.String("distribution", "zipfian", "Key distribution: zipfian|uniform")
	flagReadProportion   = flag.Float64("read-proportion", 0.95, "Fraction of reads")
	flagUpdateProportion = flag.Float64("update-proportion", 0.04, "Fraction of updates")
	flagDeleteProportion = flag.Float64("delete-proportion", 0.01, "Fraction of deletes")
	flagFieldLength      = flag.Int("fieldlength", 1024, "Value size in bytes")
	flagPhase            = flag.String("phase", "both", "Phase to run: load|run|both")
	flagOutputDir        = flag.String("output-dir", "./ycsb-results", "Directory for result files")
	flagTimeout          = flag.Duration("timeout", 5*time.Second, "Per-request timeout")
)

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

// makeKey produces a deterministic 128-bit key from a record number.
// First 8 bytes are zero, last 8 bytes are big-endian record number.
func makeKey(recordNum int64) types.Key {
	var k types.Key
	binary.BigEndian.PutUint64(k[8:], uint64(recordNum))
	return k
}

// ---------------------------------------------------------------------------
// Zipfian distribution (scrambled)
// ---------------------------------------------------------------------------

// ZipfianGenerator produces values in [0, n) following a Zipfian distribution
// with parameter theta. Uses inverse-CDF sampling with a precomputed harmonic
// number table for efficiency.
type ZipfianGenerator struct {
	n     int64
	theta float64
	zetan float64 // H(n, theta)
	zeta2 float64 // H(2, theta)
	alpha float64
	eta   float64
	rng   *rand.Rand
}

// harmonicNumber computes the generalised harmonic number H(n, theta).
func harmonicNumber(n int64, theta float64) float64 {
	sum := 0.0
	for i := int64(1); i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), theta)
	}
	return sum
}

// NewZipfianGenerator creates a scrambled Zipfian generator over [0, n).
func NewZipfianGenerator(n int64, theta float64, seed int64) *ZipfianGenerator {
	zetan := harmonicNumberApprox(n, theta)
	zeta2 := harmonicNumber(2, theta)
	alpha := 1.0 / (1.0 - theta)
	eta := (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - zeta2/zetan)
	return &ZipfianGenerator{
		n:     n,
		theta: theta,
		zetan: zetan,
		zeta2: zeta2,
		alpha: alpha,
		eta:   eta,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// harmonicNumberApprox computes the generalised harmonic number using
// an Euler-Maclaurin approximation for large n, exact for small n.
func harmonicNumberApprox(n int64, theta float64) float64 {
	if n <= 10000 {
		return harmonicNumber(n, theta)
	}
	// Euler-Maclaurin approximation: integral + correction terms
	fn := float64(n)
	if theta == 1.0 {
		return math.Log(fn) + 0.5772156649015329 // Euler-Mascheroni
	}
	s := 1.0 - theta
	return (math.Pow(fn, s) - 1.0) / s +
		0.5*math.Pow(fn, -theta) +
		theta/(12.0*math.Pow(fn, theta+1.0)) +
		0.5772156649015329*0 + // placeholder
		harmonicNumber(min64(n, 1000), theta) -
		harmonicNumberApprox2(min64(n, 1000), theta)
}

func harmonicNumberApprox2(n int64, theta float64) float64 {
	if theta == 1.0 {
		return math.Log(float64(n)) + 0.5772156649015329
	}
	fn := float64(n)
	s := 1.0 - theta
	return (math.Pow(fn, s) - 1.0) / s +
		0.5*math.Pow(fn, -theta) +
		theta/(12.0*math.Pow(fn, theta+1.0))
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Next returns the next Zipfian-distributed value in [0, n).
// Uses the YCSB inverse CDF method.
func (z *ZipfianGenerator) Next() int64 {
	u := z.rng.Float64()
	uz := u * z.zetan

	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}

	spread := float64(z.n)
	val := int64(spread * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
	if val >= z.n {
		val = z.n - 1
	}
	if val < 0 {
		val = 0
	}

	// Scramble to avoid hot-spotting on low-numbered keys
	return scramble(val, z.n)
}

// scramble applies FNV hash to spread Zipfian values across the keyspace.
func scramble(val, n int64) int64 {
	h := uint64(val)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return int64(h%uint64(n) + uint64(n)) % n
}

// ---------------------------------------------------------------------------
// HDR Histogram (microsecond precision)
// ---------------------------------------------------------------------------

// Histogram tracks latency values in microseconds with high dynamic range.
// It uses a flat array for values [0, 10000) us (10ms) and logarithmic
// buckets beyond that, up to ~1 hour.
const (
	histDirectMax  = 10000  // direct buckets for [0, 10ms) in 1us steps
	histLogBuckets = 200    // log buckets for [10ms, ~1hr)
	histLogBase    = 1.1    // each log bucket is 1.1x wider than the previous
)

type Histogram struct {
	direct    [histDirectMax]int64
	logBucket [histLogBuckets]int64
	count     int64
	sum       int64
	min       int64
	max       int64
}

func NewHistogram() *Histogram {
	return &Histogram{min: math.MaxInt64}
}

func (h *Histogram) Record(us int64) {
	h.count++
	h.sum += us
	if us < h.min {
		h.min = us
	}
	if us > h.max {
		h.max = us
	}
	if us < histDirectMax {
		if us < 0 {
			us = 0
		}
		h.direct[us]++
	} else {
		idx := int(math.Log(float64(us)/float64(histDirectMax)) / math.Log(histLogBase))
		if idx < 0 {
			idx = 0
		}
		if idx >= histLogBuckets {
			idx = histLogBuckets - 1
		}
		h.logBucket[idx]++
	}
}

// Merge adds another histogram's data into this one.
func (h *Histogram) Merge(other *Histogram) {
	if other.count == 0 {
		return
	}
	h.count += other.count
	h.sum += other.sum
	if other.min < h.min {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
	for i := range h.direct {
		h.direct[i] += other.direct[i]
	}
	for i := range h.logBucket {
		h.logBucket[i] += other.logBucket[i]
	}
}

// Percentile returns the value at the given percentile (0-100).
func (h *Histogram) Percentile(p float64) int64 {
	if h.count == 0 {
		return 0
	}
	target := int64(math.Ceil(p / 100.0 * float64(h.count)))
	if target <= 0 {
		target = 1
	}

	cumulative := int64(0)

	// Scan direct buckets
	for i := int64(0); i < histDirectMax; i++ {
		cumulative += h.direct[i]
		if cumulative >= target {
			return i
		}
	}

	// Scan log buckets
	for i := 0; i < histLogBuckets; i++ {
		cumulative += h.logBucket[i]
		if cumulative >= target {
			// Return the midpoint of this bucket
			lo := float64(histDirectMax) * math.Pow(histLogBase, float64(i))
			hi := float64(histDirectMax) * math.Pow(histLogBase, float64(i+1))
			return int64((lo + hi) / 2)
		}
	}

	return h.max
}

// Mean returns the mean latency in microseconds.
func (h *Histogram) Mean() float64 {
	if h.count == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.count)
}

// Count returns the total number of recorded values.
func (h *Histogram) Count() int64 {
	return h.count
}

// ---------------------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------------------

type opType int

const (
	opRead   opType = 0
	opUpdate opType = 1
	opDelete opType = 2
)

func (o opType) String() string {
	switch o {
	case opRead:
		return "READ"
	case opUpdate:
		return "UPDATE"
	case opDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// ---------------------------------------------------------------------------
// Time-series data point
// ---------------------------------------------------------------------------

type timeseriesPoint struct {
	Timestamp   time.Time
	ElapsedSec  float64
	Ops         int64
	Throughput  float64
	AvgLatUs    float64
	P50LatUs    int64
	P95LatUs    int64
	P99LatUs    int64
	P999LatUs   int64
}

// ---------------------------------------------------------------------------
// Per-operation-type result
// ---------------------------------------------------------------------------

type opResult struct {
	OpType     string  `json:"op_type"`
	Count      int64   `json:"count"`
	Errors     int64   `json:"errors"`
	AvgLatUs   float64 `json:"avg_latency_us"`
	P50LatUs   int64   `json:"p50_latency_us"`
	P95LatUs   int64   `json:"p95_latency_us"`
	P99LatUs   int64   `json:"p99_latency_us"`
	P999LatUs  int64   `json:"p999_latency_us"`
	P9999LatUs int64   `json:"p9999_latency_us"`
	MaxLatUs   int64   `json:"max_latency_us"`
}

type summaryResult struct {
	Phase          string     `json:"phase"`
	StartTime      string     `json:"start_time"`
	EndTime        string     `json:"end_time"`
	DurationSec    float64    `json:"duration_sec"`
	TotalOps       int64      `json:"total_ops"`
	TotalErrors    int64      `json:"total_errors"`
	ThroughputOps  float64    `json:"throughput_ops_per_sec"`
	Distribution   string     `json:"distribution"`
	Threads        int        `json:"threads"`
	RecordCount    int        `json:"record_count"`
	OperationCount int        `json:"operation_count"`
	FieldLength    int        `json:"field_length_bytes"`
	Operations     []opResult `json:"operations"`
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	// Validate proportions
	total := *flagReadProportion + *flagUpdateProportion + *flagDeleteProportion
	if math.Abs(total-1.0) > 0.001 {
		fmt.Fprintf(os.Stderr, "ERROR: proportions must sum to 1.0, got %.4f\n", total)
		os.Exit(1)
	}

	// Validate distribution
	dist := strings.ToLower(*flagDistribution)
	if dist != "zipfian" && dist != "uniform" {
		fmt.Fprintf(os.Stderr, "ERROR: distribution must be zipfian or uniform, got %q\n", dist)
		os.Exit(1)
	}

	// Create output directory
	if err := os.MkdirAll(*flagOutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot create output dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==========================================================")
	fmt.Println("  YCSB-Compatible KV Store Benchmark")
	fmt.Println("==========================================================")
	fmt.Printf("  Target:          %s\n", *flagTarget)
	fmt.Printf("  Threads:         %d\n", *flagThreads)
	fmt.Printf("  Record count:    %d\n", *flagRecordCount)
	fmt.Printf("  Operation count: %d\n", *flagOperationCount)
	fmt.Printf("  Distribution:    %s\n", dist)
	fmt.Printf("  Read proportion: %.2f\n", *flagReadProportion)
	fmt.Printf("  Update propn:    %.2f\n", *flagUpdateProportion)
	fmt.Printf("  Delete propn:    %.2f\n", *flagDeleteProportion)
	fmt.Printf("  Field length:    %d bytes\n", *flagFieldLength)
	fmt.Printf("  Phase:           %s\n", *flagPhase)
	fmt.Printf("  Output dir:      %s\n", *flagOutputDir)
	fmt.Println("==========================================================")

	phase := strings.ToLower(*flagPhase)

	if phase == "load" || phase == "both" {
		runLoadPhase()
	}

	if phase == "run" || phase == "both" {
		runWorkloadPhase(dist)
	}

	fmt.Println("\nBenchmark complete.")
}

// ---------------------------------------------------------------------------
// Load phase
// ---------------------------------------------------------------------------

func runLoadPhase() {
	fmt.Println("\n--- LOAD PHASE ---")
	fmt.Printf("Loading %d records...\n", *flagRecordCount)

	recordCount := int64(*flagRecordCount)
	threads := *flagThreads
	fieldLen := *flagFieldLength

	var totalOps atomic.Int64
	var totalErrors atomic.Int64
	histograms := make([]*Histogram, threads)
	for i := range histograms {
		histograms[i] = NewHistogram()
	}

	start := time.Now()
	var wg sync.WaitGroup

	keysPerThread := recordCount / int64(threads)
	remainder := recordCount % int64(threads)

	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			b := binding.New(*flagTarget, *flagTimeout)
			defer b.Close()

			rng := rand.New(rand.NewSource(int64(threadID) + time.Now().UnixNano()))
			hist := histograms[threadID]

			startKey := int64(threadID) * keysPerThread
			count := keysPerThread
			if int64(threadID) < remainder {
				startKey += int64(threadID)
				count++
			} else {
				startKey += remainder
			}

			value := make([]byte, fieldLen)
			for i := int64(0); i < count; i++ {
				key := makeKey(startKey + i)
				rng.Read(value)

				t0 := time.Now()
				status := b.Insert("usertable", key, map[string][]byte{"field0": value})
				latUs := time.Since(t0).Microseconds()

				hist.Record(latUs)
				totalOps.Add(1)
				if status != binding.StatusOK {
					totalErrors.Add(1)
				}
			}
		}(t)
	}

	// Progress reporting
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				ops := totalOps.Load()
				elapsed := time.Since(start).Seconds()
				fmt.Printf("  [LOAD] %d / %d ops (%.0f ops/sec, %.1fs elapsed)\n",
					ops, recordCount, float64(ops)/elapsed, elapsed)
			}
		}
	}()

	wg.Wait()
	close(done)

	elapsed := time.Since(start)

	// Merge histograms
	merged := NewHistogram()
	for _, h := range histograms {
		merged.Merge(h)
	}

	ops := totalOps.Load()
	errs := totalErrors.Load()
	throughput := float64(ops) / elapsed.Seconds()

	fmt.Printf("\n  LOAD COMPLETE\n")
	fmt.Printf("  Total ops:    %d\n", ops)
	fmt.Printf("  Errors:       %d\n", errs)
	fmt.Printf("  Duration:     %.2fs\n", elapsed.Seconds())
	fmt.Printf("  Throughput:   %.0f ops/sec\n", throughput)
	fmt.Printf("  Avg latency:  %.0f us\n", merged.Mean())
	fmt.Printf("  P50 latency:  %d us\n", merged.Percentile(50))
	fmt.Printf("  P95 latency:  %d us\n", merged.Percentile(95))
	fmt.Printf("  P99 latency:  %d us\n", merged.Percentile(99))
	fmt.Printf("  P999 latency: %d us\n", merged.Percentile(99.9))
	fmt.Printf("  Max latency:  %d us\n", merged.max)

	// Write load summary
	summary := summaryResult{
		Phase:          "load",
		StartTime:      start.Format(time.RFC3339),
		EndTime:        start.Add(elapsed).Format(time.RFC3339),
		DurationSec:    elapsed.Seconds(),
		TotalOps:       ops,
		TotalErrors:    errs,
		ThroughputOps:  throughput,
		Distribution:   "sequential",
		Threads:        *flagThreads,
		RecordCount:    *flagRecordCount,
		OperationCount: int(ops),
		FieldLength:    *flagFieldLength,
		Operations: []opResult{
			{
				OpType:     "INSERT",
				Count:      ops,
				Errors:     errs,
				AvgLatUs:   merged.Mean(),
				P50LatUs:   merged.Percentile(50),
				P95LatUs:   merged.Percentile(95),
				P99LatUs:   merged.Percentile(99),
				P999LatUs:  merged.Percentile(99.9),
				P9999LatUs: merged.Percentile(99.99),
				MaxLatUs:   merged.max,
			},
		},
	}
	writeJSON(filepath.Join(*flagOutputDir, "load_summary.json"), summary)
}

// ---------------------------------------------------------------------------
// Run (workload) phase
// ---------------------------------------------------------------------------

func runWorkloadPhase(dist string) {
	fmt.Println("\n--- RUN PHASE ---")
	fmt.Printf("Executing %d operations (%s distribution)...\n", *flagOperationCount, dist)

	opCount := int64(*flagOperationCount)
	recordCount := int64(*flagRecordCount)
	threads := *flagThreads
	fieldLen := *flagFieldLength

	readProp := *flagReadProportion
	updateProp := *flagUpdateProportion
	// deleteProp is the remainder

	// Cumulative thresholds for operation selection
	readThresh := readProp
	updateThresh := readProp + updateProp

	var totalOps atomic.Int64
	var totalErrors atomic.Int64

	// Per-thread, per-op histograms: [threadID][opType]
	type threadHists struct {
		read   *Histogram
		update *Histogram
		del    *Histogram
		all    *Histogram
	}
	allHists := make([]threadHists, threads)
	for i := range allHists {
		allHists[i] = threadHists{
			read:   NewHistogram(),
			update: NewHistogram(),
			del:    NewHistogram(),
			all:    NewHistogram(),
		}
	}

	// Time-series collection
	var tsMu sync.Mutex
	var timeseries []timeseriesPoint

	start := time.Now()
	var wg sync.WaitGroup

	opsPerThread := opCount / int64(threads)
	opsRemainder := opCount % int64(threads)

	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			b := binding.New(*flagTarget, *flagTimeout)
			defer b.Close()

			rng := rand.New(rand.NewSource(int64(threadID)*31337 + time.Now().UnixNano()))

			var zipf *ZipfianGenerator
			if dist == "zipfian" {
				zipf = NewZipfianGenerator(recordCount, 0.99, int64(threadID)*7919+time.Now().UnixNano())
			}

			hists := &allHists[threadID]

			count := opsPerThread
			if int64(threadID) < opsRemainder {
				count++
			}

			value := make([]byte, fieldLen)

			for i := int64(0); i < count; i++ {
				// Select key
				var keyIdx int64
				if dist == "zipfian" {
					keyIdx = zipf.Next()
				} else {
					keyIdx = rng.Int63n(recordCount)
				}
				key := makeKey(keyIdx)

				// Select operation
				roll := rng.Float64()
				var op opType
				if roll < readThresh {
					op = opRead
				} else if roll < updateThresh {
					op = opUpdate
				} else {
					op = opDelete
				}

				// Execute
				t0 := time.Now()
				var status binding.Status
				switch op {
				case opRead:
					_, status = b.Read("usertable", key, nil)
				case opUpdate:
					rng.Read(value)
					status = b.Update("usertable", key, map[string][]byte{"field0": value})
				case opDelete:
					status = b.Delete("usertable", key)
				}
				latUs := time.Since(t0).Microseconds()

				// Record
				hists.all.Record(latUs)
				switch op {
				case opRead:
					hists.read.Record(latUs)
				case opUpdate:
					hists.update.Record(latUs)
				case opDelete:
					hists.del.Record(latUs)
				}

				totalOps.Add(1)
				if status != binding.StatusOK && status != binding.StatusNotFound {
					totalErrors.Add(1)
				}
			}
		}(t)
	}

	// Progress and time-series collection
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastOps int64
		lastTime := start
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				ops := totalOps.Load()
				intervalOps := ops - lastOps
				intervalSec := now.Sub(lastTime).Seconds()
				throughput := float64(intervalOps) / intervalSec
				elapsed := now.Sub(start).Seconds()

				// Collect a snapshot by merging all thread histograms
				snap := NewHistogram()
				for i := range allHists {
					snap.Merge(allHists[i].all)
				}

				point := timeseriesPoint{
					Timestamp:  now,
					ElapsedSec: elapsed,
					Ops:        ops,
					Throughput: throughput,
					AvgLatUs:   snap.Mean(),
					P50LatUs:   snap.Percentile(50),
					P95LatUs:   snap.Percentile(95),
					P99LatUs:   snap.Percentile(99),
					P999LatUs:  snap.Percentile(99.9),
				}
				tsMu.Lock()
				timeseries = append(timeseries, point)
				tsMu.Unlock()

				fmt.Printf("  [RUN] %d / %d ops | interval: %.0f ops/sec | "+
					"p50=%dus p95=%dus p99=%dus p999=%dus\n",
					ops, opCount, throughput,
					snap.Percentile(50), snap.Percentile(95),
					snap.Percentile(99), snap.Percentile(99.9))

				lastOps = ops
				lastTime = now
			}
		}
	}()

	wg.Wait()
	close(done)

	elapsed := time.Since(start)

	// Merge all histograms
	mergedRead := NewHistogram()
	mergedUpdate := NewHistogram()
	mergedDelete := NewHistogram()
	mergedAll := NewHistogram()
	for i := range allHists {
		mergedRead.Merge(allHists[i].read)
		mergedUpdate.Merge(allHists[i].update)
		mergedDelete.Merge(allHists[i].del)
		mergedAll.Merge(allHists[i].all)
	}

	ops := totalOps.Load()
	errs := totalErrors.Load()
	throughput := float64(ops) / elapsed.Seconds()

	fmt.Println("\n==========================================================")
	fmt.Println("  RUN PHASE RESULTS")
	fmt.Println("==========================================================")
	fmt.Printf("  Total ops:       %d\n", ops)
	fmt.Printf("  Errors:          %d\n", errs)
	fmt.Printf("  Duration:        %.2fs\n", elapsed.Seconds())
	fmt.Printf("  Throughput:      %.0f ops/sec\n", throughput)
	fmt.Println()

	printOpStats("OVERALL", mergedAll)
	printOpStats("READ", mergedRead)
	printOpStats("UPDATE", mergedUpdate)
	printOpStats("DELETE", mergedDelete)

	// ASCII latency distribution chart
	printLatencyChart(mergedAll)

	// Write summary JSON
	summary := summaryResult{
		Phase:          "run",
		StartTime:      start.Format(time.RFC3339),
		EndTime:        start.Add(elapsed).Format(time.RFC3339),
		DurationSec:    elapsed.Seconds(),
		TotalOps:       ops,
		TotalErrors:    errs,
		ThroughputOps:  throughput,
		Distribution:   dist,
		Threads:        *flagThreads,
		RecordCount:    *flagRecordCount,
		OperationCount: *flagOperationCount,
		FieldLength:    *flagFieldLength,
		Operations: []opResult{
			makeOpResult("READ", mergedRead),
			makeOpResult("UPDATE", mergedUpdate),
			makeOpResult("DELETE", mergedDelete),
			makeOpResult("OVERALL", mergedAll),
		},
	}
	writeJSON(filepath.Join(*flagOutputDir, "summary.json"), summary)

	// Write time-series CSV
	tsMu.Lock()
	writeTimeseries(filepath.Join(*flagOutputDir, "timeseries.csv"), timeseries)
	tsMu.Unlock()

	// Print latency targets assessment
	fmt.Println("\n--- LATENCY TARGET ASSESSMENT ---")
	p99 := mergedAll.Percentile(99)
	p999 := mergedAll.Percentile(99.9)
	fmt.Printf("  P99 latency:  %d us (target: < 500 us)  ", p99)
	if p99 < 500 {
		fmt.Println("[PASS]")
	} else {
		fmt.Println("[FAIL]")
	}
	fmt.Printf("  P999 latency: %d us (target: < 1000 us) ", p999)
	if p999 < 1000 {
		fmt.Println("[PASS]")
	} else {
		fmt.Println("[FAIL]")
	}
	fmt.Println()
}

func printOpStats(name string, h *Histogram) {
	if h.Count() == 0 {
		return
	}
	fmt.Printf("  [%s] count=%d  avg=%.0fus  p50=%dus  p95=%dus  p99=%dus  p999=%dus  p9999=%dus  max=%dus\n",
		name, h.Count(), h.Mean(),
		h.Percentile(50), h.Percentile(95), h.Percentile(99),
		h.Percentile(99.9), h.Percentile(99.99), h.max)
}

func makeOpResult(name string, h *Histogram) opResult {
	if h.Count() == 0 {
		return opResult{OpType: name}
	}
	return opResult{
		OpType:     name,
		Count:      h.Count(),
		AvgLatUs:   h.Mean(),
		P50LatUs:   h.Percentile(50),
		P95LatUs:   h.Percentile(95),
		P99LatUs:   h.Percentile(99),
		P999LatUs:  h.Percentile(99.9),
		P9999LatUs: h.Percentile(99.99),
		MaxLatUs:   h.max,
	}
}

// ---------------------------------------------------------------------------
// ASCII latency distribution chart
// ---------------------------------------------------------------------------

func printLatencyChart(h *Histogram) {
	if h.Count() == 0 {
		return
	}

	fmt.Println("\n--- LATENCY DISTRIBUTION (all ops) ---")

	// Build latency buckets for display: powers of 2 in microseconds
	type bucket struct {
		label string
		lo    int64
		hi    int64
		count int64
	}

	// Define display buckets: <1us, 1-2, 2-4, 4-8, ..., up to max
	var buckets []bucket
	boundaries := []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
		2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576}

	prev := int64(0)
	for _, b := range boundaries {
		if b > h.max*2 && prev > 0 {
			break
		}
		var label string
		if b < 1000 {
			label = fmt.Sprintf("%5d us", b)
		} else if b < 1000000 {
			label = fmt.Sprintf("%5.1f ms", float64(b)/1000.0)
		} else {
			label = fmt.Sprintf("%5.1f s ", float64(b)/1000000.0)
		}
		buckets = append(buckets, bucket{label: label, lo: prev, hi: b})
		prev = b
	}
	// Overflow bucket
	if prev <= h.max {
		buckets = append(buckets, bucket{
			label: fmt.Sprintf("> %d us", prev),
			lo:    prev,
			hi:    h.max + 1,
		})
	}

	// Count values in each display bucket by iterating the histogram
	// We approximate by checking direct and log buckets
	for bi := range buckets {
		b := &buckets[bi]
		cnt := int64(0)

		// Direct range
		dlo := b.lo
		dhi := b.hi
		if dlo < 0 {
			dlo = 0
		}
		if dhi > histDirectMax {
			dhi = histDirectMax
		}
		for i := dlo; i < dhi; i++ {
			cnt += h.direct[i]
		}

		// Log range
		if b.hi > histDirectMax {
			for i := 0; i < histLogBuckets; i++ {
				lo := float64(histDirectMax) * math.Pow(histLogBase, float64(i))
				hi := float64(histDirectMax) * math.Pow(histLogBase, float64(i+1))
				mid := (lo + hi) / 2
				if mid >= float64(b.lo) && mid < float64(b.hi) {
					cnt += h.logBucket[i]
				}
			}
		}

		b.count = cnt
	}

	// Find max count for scaling
	maxCount := int64(1)
	for _, b := range buckets {
		if b.count > maxCount {
			maxCount = b.count
		}
	}

	// Print chart
	barWidth := 50
	for _, b := range buckets {
		if b.count == 0 {
			continue
		}
		bar := int(float64(b.count) / float64(maxCount) * float64(barWidth))
		if bar == 0 && b.count > 0 {
			bar = 1
		}
		pct := float64(b.count) / float64(h.Count()) * 100.0
		fmt.Printf("  %s [%6.2f%%] %s (%d)\n",
			b.label, pct, strings.Repeat("#", bar), b.count)
	}
	fmt.Println()
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

func writeJSON(path string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not marshal JSON for %s: %v\n", path, err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not write %s: %v\n", path, err)
		return
	}
	fmt.Printf("  Wrote %s\n", path)
}

func writeTimeseries(path string, points []timeseriesPoint) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not create %s: %v\n", path, err)
		return
	}
	defer f.Close()

	// Header
	fmt.Fprintf(f, "timestamp,elapsed_sec,total_ops,throughput_ops_per_sec,avg_latency_us,p50_latency_us,p95_latency_us,p99_latency_us,p999_latency_us\n")

	// Sort by elapsed time
	sort.Slice(points, func(i, j int) bool {
		return points[i].ElapsedSec < points[j].ElapsedSec
	})

	for _, p := range points {
		fmt.Fprintf(f, "%s,%.1f,%d,%.1f,%.1f,%d,%d,%d,%d\n",
			p.Timestamp.Format(time.RFC3339),
			p.ElapsedSec,
			p.Ops,
			p.Throughput,
			p.AvgLatUs,
			p.P50LatUs,
			p.P95LatUs,
			p.P99LatUs,
			p.P999LatUs)
	}

	fmt.Printf("  Wrote %s\n", path)
}
