// Command ycsbrun is the YCSB benchmark runner for the distributed KV store.
// It loads data into the cluster and/or runs a mixed read/update/delete workload,
// then writes latency statistics to JSON and CSV files.
//
// Usage:
//
//	ycsbrun -nodes=localhost:16001,localhost:16002,localhost:16003 \
//	        -recordcount=1000000 -operationcount=1000000 \
//	        -threads=8 -phase=both -outdir=ycsb-results
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"ai-kv-store/client"
	"ai-kv-store/kvstore"
	"ai-kv-store/ycsb"
)

func main() {
	// ── Flags ─────────────────────────────────────────────────────────────────
	nodes := flag.String("nodes", "localhost:16001",
		"Comma-separated HTTP addresses of KV nodes (e.g. localhost:16001,localhost:16002)")
	recordCount := flag.Int64("recordcount", 1_000_000,
		"Number of records to load in the load phase")
	opCount := flag.Int64("operationcount", 1_000_000,
		"Number of operations to perform in the run phase")
	threads := flag.Int("threads", 8,
		"Number of concurrent client goroutines")
	valueSize := flag.Int("valuesize", 1024,
		"Size of each value in bytes")
	phase := flag.String("phase", "both",
		`Phase to execute: "load", "run", or "both"`)
	outDir := flag.String("outdir", "ycsb-results",
		"Directory to write output files (JSON report, CSV latencies)")
	readProp := flag.Float64("readproportion", 0.95,
		"Fraction of run-phase operations that are reads")
	updateProp := flag.Float64("updateproportion", 0.04,
		"Fraction of run-phase operations that are updates")
	deleteProp := flag.Float64("deleteproportion", 0.01,
		"Fraction of run-phase operations that are deletes")
	zipfTheta := flag.Float64("zipfian.theta", 0.99,
		"Zipfian theta parameter (higher = more skew)")
	seed := flag.Int64("seed", 42,
		"Random seed for reproducibility")
	timeout := flag.Duration("timeout", 5*time.Second,
		"Per-operation client timeout")
	flag.Parse()

	// ── Validate phase ────────────────────────────────────────────────────────
	switch *phase {
	case "load", "run", "both":
	default:
		fmt.Fprintf(os.Stderr, "error: -phase must be load, run, or both (got %q)\n", *phase)
		os.Exit(1)
	}

	// ── Parse node list ───────────────────────────────────────────────────────
	var nodeList []string
	for _, n := range strings.Split(*nodes, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			nodeList = append(nodeList, n)
		}
	}
	if len(nodeList) == 0 {
		fmt.Fprintln(os.Stderr, "error: -nodes must specify at least one node address")
		os.Exit(1)
	}

	// ── Create output directory ───────────────────────────────────────────────
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot create output directory %q: %v\n", *outDir, err)
		os.Exit(1)
	}

	// ── Create KV client ─────────────────────────────────────────────────────
	kvClient := client.New(client.Config{
		Nodes:   nodeList,
		Timeout: *timeout,
	})
	defer kvClient.Close()

	// ── Create YCSB DB binding ────────────────────────────────────────────────
	binding := &KVBinding{c: kvClient}

	// ── Build workload config ─────────────────────────────────────────────────
	wCfg := ycsb.WorkloadConfig{
		RecordCount:      *recordCount,
		OperationCount:   *opCount,
		ValueSize:        *valueSize,
		ReadProportion:   *readProp,
		UpdateProportion: *updateProp,
		InsertProportion: 0.0,
		DeleteProportion: *deleteProp,
		ScanProportion:   0.0,
		ZipfianTheta:     *zipfTheta,
		Seed:             *seed,
		ThreadCount:      *threads,
	}

	fmt.Printf("=== YCSB Benchmark ===\n")
	fmt.Printf("Nodes:          %s\n", strings.Join(nodeList, ", "))
	fmt.Printf("RecordCount:    %d\n", *recordCount)
	fmt.Printf("OperationCount: %d\n", *opCount)
	fmt.Printf("Threads:        %d\n", *threads)
	fmt.Printf("ValueSize:      %d bytes\n", *valueSize)
	fmt.Printf("Phase:          %s\n", *phase)
	fmt.Printf("ZipfTheta:      %.2f\n", *zipfTheta)
	fmt.Printf("OutputDir:      %s\n", *outDir)
	fmt.Println()

	// ── Load phase ────────────────────────────────────────────────────────────
	if *phase == "load" || *phase == "both" {
		fmt.Println("=== LOAD PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(wCfg, binding)

		done := make(chan struct{})
		go printProgress(reporter, done)

		if err := w.Load(reporter); err != nil {
			fmt.Fprintf(os.Stderr, "warning: load phase error: %v\n", err)
		}
		close(done)

		reporter.PrintReport(os.Stdout)
		if err := reporter.WriteJSON(fmt.Sprintf("%s/load_report.json", *outDir)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write load report: %v\n", err)
		}
		if err := reporter.WriteLatencyCSV(fmt.Sprintf("%s/load_latencies.csv", *outDir)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write load latencies: %v\n", err)
		}
		fmt.Printf("Load results written to %s/\n\n", *outDir)
	}

	// ── Run phase ─────────────────────────────────────────────────────────────
	if *phase == "run" || *phase == "both" {
		fmt.Println("=== RUN PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(wCfg, binding)

		done := make(chan struct{})
		go printProgress(reporter, done)

		w.Run(reporter)
		close(done)

		reporter.PrintReport(os.Stdout)
		if err := reporter.WriteJSON(fmt.Sprintf("%s/run_report.json", *outDir)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write run report: %v\n", err)
		}
		if err := reporter.WriteLatencyCSV(fmt.Sprintf("%s/run_latencies.csv", *outDir)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write run latencies: %v\n", err)
		}
		fmt.Printf("Run results written to %s/\n\n", *outDir)
	}
}

// printProgress prints a progress line every 10 seconds until done is closed.
func printProgress(reporter *ycsb.Reporter, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			report := reporter.Report()
			fmt.Printf("  [progress] ops=%d throughput=%.0f ops/sec\n",
				report.TotalOps, report.ThroughputOpsSec)
		}
	}
}

// KVBinding adapts the *client.Client to implement ycsb.DB.
// Keys arrive as 32-char hex strings from the workload engine; they are
// decoded to 16-byte slices before being forwarded to the client library.
type KVBinding struct {
	c *client.Client
}

// Read retrieves the value for the given hex-encoded key.
func (b *KVBinding) Read(key string) (kvstore.Status, []byte, error) {
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return kvstore.StatusError, nil, fmt.Errorf("decode key %q: %w", key, err)
	}
	return b.c.Get(decoded)
}

// Insert stores a new key-value pair.
func (b *KVBinding) Insert(key string, value []byte) (kvstore.Status, error) {
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return kvstore.StatusError, fmt.Errorf("decode key %q: %w", key, err)
	}
	return b.c.Put(decoded, value)
}

// Update overwrites an existing key-value pair. Implemented as Put.
func (b *KVBinding) Update(key string, value []byte) (kvstore.Status, error) {
	return b.Insert(key, value)
}

// Delete removes the given key from the store.
func (b *KVBinding) Delete(key string) (kvstore.Status, error) {
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return kvstore.StatusError, fmt.Errorf("decode key %q: %w", key, err)
	}
	return b.c.Delete(decoded)
}

// Scan is not supported by the KV store; scanproportion=0 ensures it is not called.
func (b *KVBinding) Scan(_ string, _ int) (kvstore.Status, error) {
	return kvstore.StatusOK, nil
}

// Close releases the client's resources.
func (b *KVBinding) Close() error {
	return b.c.Close()
}
