package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"base/task3_harness/internal/harness"
)

func main() {
	var (
		runDir         = flag.String("run-dir", "", "artifact directory")
		seed           = flag.Int64("seed", time.Now().UnixNano(), "seed")
		duration       = flag.Duration("duration", 20*time.Second, "run duration")
		workers        = flag.Int("workers", 64, "concurrent workers")
		endpoints      = flag.String("endpoints", "http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003", "comma separated endpoints")
		keyspace       = flag.Uint64("keyspace", 1_000_000, "hot keyspace")
		maxValue       = flag.Int("max-value-bytes", 1<<20, "max value bytes")
		realisticSizes = flag.Bool("realistic-sizes", true, "sample sizes up to max")
		injectFault    = flag.Bool("inject-fault", false, "crash + recover 1 node during run")
		crashNode      = flag.Int("crash-node", 1, "node index to crash")
		crashAfter     = flag.Duration("crash-after", 6*time.Second, "when to crash")
		recoverAfter   = flag.Duration("recover-after", 8*time.Second, "delay after crash for recover")
		manageCluster  = flag.Bool("manage-cluster", true, "run start/stop commands")
		checkerBin     = flag.String("checker-bin", "./bin/porcupine_check", "checker binary path")
	)
	flag.Parse()

	if *runDir == "" {
		*runDir = filepath.Join("artifacts", time.Now().UTC().Format("20060102T150405Z"))
	}

	cfg := harness.Config{
		RunDir:         *runDir,
		Seed:           *seed,
		Duration:       *duration,
		Workers:        *workers,
		Endpoints:      splitCSV(*endpoints),
		Keyspace:       *keyspace,
		MaxValueBytes:  *maxValue,
		RealisticSizes: *realisticSizes,
		InjectFault:    *injectFault,
		CrashNode:      *crashNode,
		CrashAfter:     *crashAfter,
		RecoverAfter:   *recoverAfter,
		ManageCluster:  *manageCluster,
		CheckerBinary:  *checkerBin,
		StartCmd:       envDefault("HARNESS_START_CMD", "./bin/clusterctl -cmd start -nodes 3 -base-port 9001 -state-dir .run/cluster -node-bin ./bin/kvnode"),
		StopCmd:        envDefault("HARNESS_STOP_CMD", "./bin/clusterctl -cmd stop -state-dir .run/cluster"),
		CrashCmd:       envDefault("HARNESS_CRASH_CMD", "./bin/clusterctl -cmd crash -state-dir .run/cluster -node {node}"),
		RecoverCmd:     envDefault("HARNESS_RECOVER_CMD", "./bin/clusterctl -cmd recover -state-dir .run/cluster -node {node} -node-bin ./bin/kvnode"),
	}
	if err := harness.Run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("artifacts: %s\n", *runDir)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trim := strings.TrimSpace(p)
		if trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

func envDefault(k, d string) string {
	if v := os.Getenv(k); strings.TrimSpace(v) != "" {
		return v
	}
	return d
}
