// harness is the correctness test harness for the distributed KV store.
// It starts a cluster of kvnode processes, runs concurrent client workers,
// optionally injects faults, records a history of all operations, and then
// runs the Porcupine linearizability checker.
//
// Usage:
//
//	harness -bin=./bin/kvnode -nodes=3 -duration=15s -workers=4 [-faults] [-seed=42]
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"ai-kv-store/correctness"
)

func main() {
	binPath := flag.String("bin", "./bin/kvnode", "Path to kvnode binary")
	numNodes := flag.Int("nodes", 3, "Number of nodes (3-5)")
	duration := flag.Duration("duration", 15*time.Second, "Test duration")
	numWorkers := flag.Int("workers", 4, "Concurrent client workers")
	faults := flag.Bool("faults", false, "Enable fault injection (crash 1 node)")
	seed := flag.Int64("seed", 0, "Random seed (0 = random)")
	checkTimeout := flag.Duration("checktimeout", 120*time.Second, "Timeout for Porcupine checker")
	outdir := flag.String("outdir", "", "Output directory (required)")
	flag.Parse()

	if *outdir == "" {
		log.Fatal("-outdir is required")
	}
	if *numNodes < 3 || *numNodes > 5 {
		log.Fatal("-nodes must be 3-5")
	}

	// Seed the random number generator.
	if *seed == 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
		if err != nil {
			log.Fatalf("Failed to generate random seed: %v", err)
		}
		*seed = n.Int64()
	}
	rng := mrand.New(mrand.NewSource(*seed))
	log.Printf("Using seed: %d", *seed)

	// Create output directories.
	logsDir := filepath.Join(*outdir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		log.Fatalf("Failed to create logs dir: %v", err)
	}

	// Port bases.
	const rpcBase = 18001
	const httpBase = 19001

	// Start nodes.
	type nodeInfo struct {
		id       int
		rpcPort  int
		httpPort int
		cmd      *exec.Cmd
		logFile  *os.File
	}
	nodes := make([]*nodeInfo, *numNodes)

	startNode := func(idx int) error {
		n := &nodeInfo{
			id:       idx + 1,
			rpcPort:  rpcBase + idx,
			httpPort: httpBase + idx,
		}

		// Build peers string.
		var peers []string
		for j := 0; j < *numNodes; j++ {
			if j == idx {
				continue
			}
			peers = append(peers, fmt.Sprintf("%d=localhost:%d", j+1, rpcBase+j))
		}

		args := []string{
			fmt.Sprintf("-id=%d", n.id),
			fmt.Sprintf("-rpc=:%d", n.rpcPort),
			fmt.Sprintf("-client=:%d", n.httpPort),
			fmt.Sprintf("-peers=%s", strings.Join(peers, ",")),
		}

		n.cmd = exec.Command(*binPath, args...)

		logPath := filepath.Join(logsDir, fmt.Sprintf("node%d.log", n.id))
		lf, err := os.Create(logPath)
		if err != nil {
			return fmt.Errorf("create log file for node %d: %w", n.id, err)
		}
		n.logFile = lf
		n.cmd.Stdout = lf
		n.cmd.Stderr = lf

		if err := n.cmd.Start(); err != nil {
			lf.Close()
			return fmt.Errorf("start node %d: %w", n.id, err)
		}

		nodes[idx] = n
		log.Printf("Started node %d (pid=%d, rpc=:%d, http=:%d)",
			n.id, n.cmd.Process.Pid, n.rpcPort, n.httpPort)
		return nil
	}

	stopNode := func(idx int) {
		n := nodes[idx]
		if n == nil || n.cmd == nil || n.cmd.Process == nil {
			return
		}
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = n.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = n.cmd.Process.Kill()
			<-done
		}
		if n.logFile != nil {
			n.logFile.Close()
		}
		log.Printf("Stopped node %d", n.id)
	}

	killNode := func(idx int) {
		n := nodes[idx]
		if n == nil || n.cmd == nil || n.cmd.Process == nil {
			return
		}
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
		if n.logFile != nil {
			n.logFile.Close()
		}
		log.Printf("Killed node %d (SIGKILL)", n.id)
	}

	// Start all nodes.
	for i := 0; i < *numNodes; i++ {
		if err := startNode(i); err != nil {
			// Cleanup already-started nodes.
			for j := 0; j < i; j++ {
				stopNode(j)
			}
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	// Ensure cleanup on exit.
	defer func() {
		for i := 0; i < *numNodes; i++ {
			stopNode(i)
		}
	}()

	// Wait for leader election.
	log.Println("Waiting for leader election (~3s)...")
	time.Sleep(3 * time.Second)

	// Build key pool (~100 random 128-bit keys).
	keyPool := make([]string, 100)
	for i := range keyPool {
		var buf [16]byte
		_, _ = rand.Read(buf[:])
		keyPool[i] = hex.EncodeToString(buf[:])
	}

	// HTTP client with short timeouts.
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// History recorder.
	recorder := correctness.NewHistoryRecorder()

	// Counters.
	var totalOps int64
	var errorCount int64
	var timeoutCount int64

	// Shared leader tracking — workers update this on 421 redirects.
	var leaderMu sync.Mutex
	leaderURL := "" // discovered leader base URL

	// doHTTP performs an HTTP request, following X-Leader redirects up to 3 times.
	doHTTP := func(ctx context.Context, method, path string, body []byte, nodes int) (*http.Response, error) {
		var baseURL string
		leaderMu.Lock()
		if leaderURL != "" {
			baseURL = leaderURL
		}
		leaderMu.Unlock()

		for attempt := 0; attempt < 3; attempt++ {
			if baseURL == "" {
				baseURL = fmt.Sprintf("http://localhost:%d", httpBase+mrand.Intn(nodes))
			}
			url := baseURL + path
			var req *http.Request
			var err error
			if body != nil {
				req, err = http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(body)))
			} else {
				req, err = http.NewRequestWithContext(ctx, method, url, nil)
			}
			if err != nil {
				return nil, err
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				baseURL = "" // try another node
				continue
			}

			if resp.StatusCode == 421 {
				resp.Body.Close()
				// Follow X-Leader redirect
				ldr := resp.Header.Get("X-Leader")
				if ldr != "" {
					// ldr may be ":19003" or "localhost:19003"
					if strings.HasPrefix(ldr, ":") {
						ldr = "localhost" + ldr
					}
					if !strings.HasPrefix(ldr, "http") {
						ldr = "http://" + ldr
					}
					baseURL = ldr
					leaderMu.Lock()
					leaderURL = ldr
					leaderMu.Unlock()
					continue
				}
				baseURL = ""
				continue
			}

			if resp.StatusCode == 503 {
				resp.Body.Close()
				baseURL = ""
				continue
			}

			return resp, nil
		}
		return nil, fmt.Errorf("all attempts failed")
	}

	// Worker function.
	worker := func(ctx context.Context, clientID int, workerRng *mrand.Rand) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Pick operation: 95% get, 4% put, 1% delete.
			key := keyPool[workerRng.Intn(len(keyPool))]
			roll := workerRng.Float64()

			opID := recorder.NextOpID()
			callTime := time.Now().UnixNano()

			var ev correctness.HistoryEvent
			ev.OpID = opID
			ev.ClientID = clientID
			ev.CallTime = callTime
			ev.Key = key

			path := "/kv/" + key

			if roll < 0.95 {
				// GET
				ev.OpType = "get"
				resp, err := doHTTP(ctx, "GET", path, nil, *numNodes)
				ev.ReturnTime = time.Now().UnixNano()

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					ev.Status = "error"
					atomic.AddInt64(&errorCount, 1)
				} else {
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()

					switch resp.StatusCode {
					case 200:
						ev.Status = "found"
						ev.OutputValue = hex.EncodeToString(body)
					case 404:
						ev.Status = "not_found"
					default:
						ev.Status = "error"
						atomic.AddInt64(&errorCount, 1)
					}
				}
			} else if roll < 0.99 {
				// PUT
				ev.OpType = "put"
				valLen := 64 + workerRng.Intn(961) // 64-1024 bytes
				val := make([]byte, valLen)
				_, _ = rand.Read(val)
				ev.InputValue = hex.EncodeToString(val)

				resp, err := doHTTP(ctx, "PUT", path, val, *numNodes)
				ev.ReturnTime = time.Now().UnixNano()

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					ev.Status = "timeout"
					atomic.AddInt64(&timeoutCount, 1)
				} else {
					resp.Body.Close()
					switch resp.StatusCode {
					case 200:
						ev.Status = "ok"
					case 504:
						ev.Status = "timeout"
						atomic.AddInt64(&timeoutCount, 1)
					default:
						ev.Status = "error"
						atomic.AddInt64(&errorCount, 1)
					}
				}
			} else {
				// DELETE
				ev.OpType = "delete"
				resp, err := doHTTP(ctx, "DELETE", path, nil, *numNodes)
				ev.ReturnTime = time.Now().UnixNano()

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					ev.Status = "timeout"
					atomic.AddInt64(&timeoutCount, 1)
				} else {
					resp.Body.Close()
					switch resp.StatusCode {
					case 200:
						ev.Status = "ok"
					case 504:
						ev.Status = "timeout"
						atomic.AddInt64(&timeoutCount, 1)
					default:
						ev.Status = "error"
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}

			recorder.Record(ev)
			atomic.AddInt64(&totalOps, 1)
		}
	}

	// Run workers.
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		workerRng := mrand.New(mrand.NewSource(rng.Int63()))
		go func(id int) {
			defer wg.Done()
			worker(ctx, id, workerRng)
		}(i)
	}

	// Fault injection.
	var faultEvents int
	if *faults {
		go func() {
			// Wait 30% of duration before injecting fault.
			faultDelay := time.Duration(float64(*duration) * 0.3)
			select {
			case <-time.After(faultDelay):
			case <-ctx.Done():
				return
			}

			// Pick a non-leader node to crash. We pick a non-first node as a heuristic
			// (node 1 is often the leader after initial election).
			crashIdx := 1 + rng.Intn(*numNodes-1)
			log.Printf("FAULT: Killing node %d", crashIdx+1)
			killNode(crashIdx)
			faultEvents++

			// Clear cached leader so workers discover the new leader.
			leaderMu.Lock()
			leaderURL = ""
			leaderMu.Unlock()

			// Wait 3-5 seconds then restart.
			restartDelay := 3*time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
			select {
			case <-time.After(restartDelay):
			case <-ctx.Done():
				return
			}

			log.Printf("FAULT: Restarting node %d", crashIdx+1)
			if err := startNode(crashIdx); err != nil {
				log.Printf("FAULT: Failed to restart node %d: %v", crashIdx+1, err)
			} else {
				faultEvents++
			}
		}()
	}

	// Wait for workers to finish.
	wg.Wait()
	durationSec := float64(*duration) / float64(time.Second)

	log.Printf("Test complete: %d ops, %d errors, %d timeouts in %.1fs",
		atomic.LoadInt64(&totalOps),
		atomic.LoadInt64(&errorCount),
		atomic.LoadInt64(&timeoutCount),
		durationSec)

	// Write history.
	historyPath := filepath.Join(*outdir, "history.jsonl")
	if err := recorder.WriteJSONL(historyPath); err != nil {
		log.Fatalf("Failed to write history: %v", err)
	}
	log.Printf("Wrote history to %s", historyPath)

	// Run checker.
	checkerBin := "./bin/checker"
	var linearizable bool
	checkerResultPath := filepath.Join(*outdir, "checker_result.txt")

	checkerCmd := exec.Command(checkerBin,
		"-history="+historyPath,
		"-timeout="+checkTimeout.String(),
	)
	checkerOutput, checkerErr := checkerCmd.CombinedOutput()
	checkerResult := string(checkerOutput)

	if checkerErr != nil {
		if exitErr, ok := checkerErr.(*exec.ExitError); ok {
			log.Printf("Checker exited with code %d", exitErr.ExitCode())
			linearizable = false
		} else {
			// Checker binary not found or other exec error.
			log.Printf("Checker not available: %v", checkerErr)
			checkerResult = fmt.Sprintf("Checker not available: %v\n", checkerErr)
			linearizable = false
		}
	} else {
		linearizable = true
	}

	fmt.Print(checkerResult)

	if err := os.WriteFile(checkerResultPath, []byte(checkerResult), 0644); err != nil {
		log.Printf("Warning: failed to write checker result: %v", err)
	}

	// Write summary.
	summary := map[string]interface{}{
		"total_ops":     atomic.LoadInt64(&totalOps),
		"errors":        atomic.LoadInt64(&errorCount),
		"timeouts":      atomic.LoadInt64(&timeoutCount),
		"fault_events":  faultEvents,
		"duration_sec":  durationSec,
		"linearizable":  linearizable,
		"seed":          *seed,
		"nodes":         *numNodes,
		"workers":       *numWorkers,
		"faults_enabled": *faults,
	}
	summaryJSON, _ := json.MarshalIndent(summary, "", "  ")
	summaryPath := filepath.Join(*outdir, "summary.json")
	if err := os.WriteFile(summaryPath, summaryJSON, 0644); err != nil {
		log.Printf("Warning: failed to write summary: %v", err)
	}
	log.Printf("Wrote summary to %s", summaryPath)

	// Print final summary.
	fmt.Println()
	fmt.Println("=== Summary ===")
	fmt.Printf("Total ops:    %d\n", atomic.LoadInt64(&totalOps))
	fmt.Printf("Errors:       %d\n", atomic.LoadInt64(&errorCount))
	fmt.Printf("Timeouts:     %d\n", atomic.LoadInt64(&timeoutCount))
	fmt.Printf("Fault events: %d\n", faultEvents)
	fmt.Printf("Duration:     %.1fs\n", durationSec)
	fmt.Printf("Linearizable: %v\n", linearizable)

	if !linearizable {
		os.Exit(1)
	}
}

// unused but kept for reference — converts port number to string.
func portStr(port int) string {
	return strconv.Itoa(port)
}
