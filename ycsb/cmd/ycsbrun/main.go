package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"ai-kv-store/ycsb"
)

func main() {
	nodes := flag.String("nodes", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated node client addresses")
	recordCount := flag.Int64("recordcount", 1_000_000, "Number of records to load")
	opCount := flag.Int64("operationcount", 1_000_000, "Number of operations in run phase")
	threads := flag.Int("threads", 8, "Number of client threads")
	valueSize := flag.Int("valuesize", 1024, "Value size in bytes")
	phase := flag.String("phase", "both", "Phase to run: load, run, or both")
	outDir := flag.String("outdir", "ycsb-results", "Output directory for results")
	readProp := flag.Float64("readproportion", 0.95, "Read proportion")
	updateProp := flag.Float64("updateproportion", 0.04, "Update proportion")
	deleteProp := flag.Float64("deleteproportion", 0.01, "Delete proportion")
	theta := flag.Float64("zipfian.theta", 0.99, "Zipfian theta parameter")
	flag.Parse()

	nodeList := strings.Split(*nodes, ",")
	for i := range nodeList {
		nodeList[i] = strings.TrimSpace(nodeList[i])
	}

	os.MkdirAll(*outDir, 0755)

	cfg := ycsb.WorkloadConfig{
		RecordCount:      *recordCount,
		OperationCount:   *opCount,
		ReadProportion:   *readProp,
		UpdateProportion: *updateProp,
		InsertProportion: 0.0,
		DeleteProportion: *deleteProp,
		ScanProportion:   0.0,
		ZipfianTheta:     *theta,
		ValueSize:        *valueSize,
		ThreadCount:      *threads,
	}

	binding := ycsb.NewKVBinding(nodeList)

	if *phase == "load" || *phase == "both" {
		fmt.Println("=== LOAD PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(cfg, binding)
		if err := w.Load(reporter); err != nil {
			fmt.Fprintf(os.Stderr, "load failed: %v\n", err)
		}
		reporter.PrintReport(os.Stdout)
		reporter.WriteJSON(fmt.Sprintf("%s/load_report.json", *outDir))
		reporter.WriteLatencyCSV(fmt.Sprintf("%s/load_latencies.csv", *outDir))
	}

	if *phase == "run" || *phase == "both" {
		fmt.Println("=== RUN PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(cfg, binding)
		w.Run(reporter)
		reporter.PrintReport(os.Stdout)
		reporter.WriteJSON(fmt.Sprintf("%s/run_report.json", *outDir))
		reporter.WriteLatencyCSV(fmt.Sprintf("%s/run_latencies.csv", *outDir))
	}

	fmt.Printf("\nResults written to %s/\n", *outDir)
}
