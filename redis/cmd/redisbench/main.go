//go:build ignore

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	redisbinding "ai-kv-store/redis"
	"ai-kv-store/ycsb"
)

func main() {
	addrs := flag.String("addrs", "localhost:6379", "Comma-separated Redis addresses")
	password := flag.String("password", "", "Redis AUTH password")
	db := flag.Int("db", 0, "Redis database number")
	poolSize := flag.Int("poolsize", 128, "Connection pool size")
	clusterMode := flag.Bool("cluster", false, "Enable Redis cluster mode")
	readTimeout := flag.Duration("read-timeout", 3*time.Second, "Redis read timeout")
	writeTimeout := flag.Duration("write-timeout", 3*time.Second, "Redis write timeout")
	opTimeout := flag.Duration("op-timeout", 5*time.Second, "Per-operation timeout")

	recordCount := flag.Int64("recordcount", 1_000_000, "Number of records to load")
	opCount := flag.Int64("operationcount", 1_000_000, "Number of operations in run phase")
	threads := flag.Int("threads", 8, "Number of client threads")
	valueSize := flag.Int("valuesize", 1024, "Value size in bytes")
	phase := flag.String("phase", "both", "Phase to run: load, run, or both")
	outDir := flag.String("outdir", "redis-results", "Output directory for results")
	readProp := flag.Float64("readproportion", 0.95, "Read proportion")
	updateProp := flag.Float64("updateproportion", 0.04, "Update proportion")
	deleteProp := flag.Float64("deleteproportion", 0.01, "Delete proportion")
	theta := flag.Float64("zipfian.theta", 0.99, "Zipfian theta parameter")

	flushBefore := flag.Bool("flush", true, "FLUSHDB before benchmark (standalone mode only)")
	serverInfo := flag.Bool("info", false, "Print Redis server info before benchmark")
	flag.Parse()

	addrList := strings.Split(*addrs, ",")
	for i := range addrList {
		addrList[i] = strings.TrimSpace(addrList[i])
	}

	os.MkdirAll(*outDir, 0755)

	cfg := redisbinding.RedisConfig{
		Addrs:        addrList,
		Password:     *password,
		DB:           *db,
		PoolSize:     *poolSize,
		ReadTimeout:  *readTimeout,
		WriteTimeout: *writeTimeout,
		OpTimeout:    *opTimeout,
		ClusterMode:  *clusterMode,
	}

	binding, err := redisbinding.NewRedisBinding(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer binding.Close()

	if *serverInfo {
		info, err := binding.Info("server")
		if err == nil {
			fmt.Println("=== Redis Server Info ===")
			fmt.Println(info)
		}
	}

	if *flushBefore && !*clusterMode {
		fmt.Println("Flushing Redis database...")
		if err := binding.FlushDB(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: FLUSHDB failed: %v\n", err)
		}
	}

	meta := map[string]interface{}{
		"target":       "redis",
		"addrs":        addrList,
		"cluster_mode": *clusterMode,
		"pool_size":    *poolSize,
		"db":           *db,
		"record_count": *recordCount,
		"op_count":     *opCount,
		"threads":      *threads,
		"value_size":   *valueSize,
		"read_prop":    *readProp,
		"update_prop":  *updateProp,
		"delete_prop":  *deleteProp,
		"zipfian_theta": *theta,
	}
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(fmt.Sprintf("%s/benchmark_meta.json", *outDir), metaJSON, 0644)

	wCfg := ycsb.WorkloadConfig{
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

	if !*clusterMode {
		memInfo, err := binding.Info("memory")
		if err == nil {
			os.WriteFile(fmt.Sprintf("%s/redis_memory_info.txt", *outDir), []byte(memInfo), 0644)
		}
	}

	fmt.Printf("\nResults written to %s/\n", *outDir)
}
