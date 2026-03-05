package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	redisbinding "ai-kv-store/redis"
	"ai-kv-store/redis/ycsb"
)

func main() {
	addrs := flag.String("addrs", "localhost:6379", "Redis address (comma-separated for cluster)")
	recordCount := flag.Int64("recordcount", 1_000_000, "Number of records to load")
	opCount := flag.Int64("operationcount", 1_000_000, "Number of run-phase operations")
	threads := flag.Int("threads", 8, "Concurrent client goroutines")
	phase := flag.String("phase", "both", "Phase: load, run, or both")
	outDir := flag.String("outdir", "redis-results", "Output directory")
	flag.Parse()

	addrList := strings.Split(*addrs, ",")
	for i := range addrList {
		addrList[i] = strings.TrimSpace(addrList[i])
	}

	os.MkdirAll(*outDir, 0755)

	cfg := redisbinding.DefaultRedisConfig()
	cfg.Addrs = addrList

	binding, err := redisbinding.NewRedisBinding(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer binding.Close()

	fmt.Println("Flushing Redis database...")
	if err := binding.FlushDB(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: FLUSHDB failed: %v\n", err)
	}

	meta := map[string]interface{}{
		"target":       "redis",
		"addrs":        addrList,
		"record_count": *recordCount,
		"op_count":     *opCount,
		"threads":      *threads,
	}
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(fmt.Sprintf("%s/benchmark_meta.json", *outDir), metaJSON, 0644)

	wCfg := ycsb.WorkloadConfig{
		RecordCount:      *recordCount,
		OperationCount:   *opCount,
		ReadProportion:   0.95,
		UpdateProportion: 0.04,
		DeleteProportion: 0.01,
		ZipfianTheta:     0.99,
		ValueSize:        1024,
		ThreadCount:      *threads,
	}

	if *phase == "load" || *phase == "both" {
		fmt.Println("=== LOAD PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(wCfg, binding)
		if err := w.Load(reporter); err != nil {
			fmt.Fprintf(os.Stderr, "load failed: %v\n", err)
		}
		reporter.PrintReport(os.Stdout)
		reporter.WriteJSON(fmt.Sprintf("%s/load_report.json", *outDir))
		reporter.WriteLatencyCSV(fmt.Sprintf("%s/load_latencies.csv", *outDir))

		size, err := binding.DBSize()
		if err == nil {
			fmt.Printf("Redis DBSIZE after load: %d keys\n", size)
		}
	}

	if *phase == "run" || *phase == "both" {
		fmt.Println("=== RUN PHASE ===")
		reporter := ycsb.NewReporter()
		w := ycsb.NewWorkload(wCfg, binding)
		w.Run(reporter)
		reporter.PrintReport(os.Stdout)
		reporter.WriteJSON(fmt.Sprintf("%s/run_report.json", *outDir))
		reporter.WriteLatencyCSV(fmt.Sprintf("%s/run_latencies.csv", *outDir))
	}

	memInfo, err := binding.Info("memory")
	if err == nil {
		os.WriteFile(fmt.Sprintf("%s/redis_memory_info.txt", *outDir), []byte(memInfo), 0644)
	}

	fmt.Printf("\nResults written to %s/\n", *outDir)
}
