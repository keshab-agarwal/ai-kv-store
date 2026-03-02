// Command etcdbench runs the same YCSB workload as ycsbrun but against a
// single-node etcd cluster, for comparison with the custom KV store.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"ai-kv-store/kvstore"
	"ai-kv-store/ycsb"
)

func main() {
	endpoints := flag.String("endpoints", "localhost:2379",
		"Comma-separated etcd gRPC endpoints")
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
	outDir := flag.String("outdir", "etcd-ycsb-results",
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
		"Per-operation etcd timeout")
	flag.Parse()

	switch *phase {
	case "load", "run", "both":
	default:
		fmt.Fprintf(os.Stderr, "error: -phase must be load, run, or both (got %q)\n", *phase)
		os.Exit(1)
	}

	eps := strings.Split(*endpoints, ",")
	for i := range eps {
		eps[i] = strings.TrimSpace(eps[i])
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot create output directory %q: %v\n", *outDir, err)
		os.Exit(1)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect to etcd %v: %v\n", eps, err)
		os.Exit(1)
	}
	defer cli.Close()

	binding := &etcdBinding{cli: cli, timeout: *timeout}

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

	fmt.Printf("=== YCSB Benchmark (etcd) ===\n")
	fmt.Printf("Endpoints:      %s\n", strings.Join(eps, ", "))
	fmt.Printf("RecordCount:    %d\n", *recordCount)
	fmt.Printf("OperationCount: %d\n", *opCount)
	fmt.Printf("Threads:        %d\n", *threads)
	fmt.Printf("ValueSize:      %d bytes\n", *valueSize)
	fmt.Printf("Phase:          %s\n", *phase)
	fmt.Printf("OutputDir:      %s\n", *outDir)
	fmt.Println()

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
		fmt.Printf("Load results written to %s/\n\n", *outDir)
	}

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

// etcdBinding implements ycsb.DB against etcd v3.
type etcdBinding struct {
	cli     *clientv3.Client
	timeout time.Duration
}

func (e *etcdBinding) Read(key string) (kvstore.Status, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	resp, err := e.cli.Get(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return kvstore.StatusTimeout, nil, nil
		}
		return kvstore.StatusError, nil, err
	}
	if len(resp.Kvs) == 0 {
		return kvstore.StatusNotFound, nil, nil
	}
	return kvstore.StatusFound, resp.Kvs[0].Value, nil
}

func (e *etcdBinding) Insert(key string, value []byte) (kvstore.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	_, err := e.cli.Put(ctx, key, string(value))
	if err != nil {
		if ctx.Err() != nil {
			return kvstore.StatusTimeout, nil
		}
		return kvstore.StatusError, err
	}
	return kvstore.StatusOK, nil
}

func (e *etcdBinding) Update(key string, value []byte) (kvstore.Status, error) {
	return e.Insert(key, value)
}

func (e *etcdBinding) Delete(key string) (kvstore.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	_, err := e.cli.Delete(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return kvstore.StatusTimeout, nil
		}
		return kvstore.StatusError, err
	}
	return kvstore.StatusOK, nil
}

func (e *etcdBinding) Scan(_ string, _ int) (kvstore.Status, error) {
	return kvstore.StatusOK, nil
}

func (e *etcdBinding) Close() error {
	return e.cli.Close()
}
