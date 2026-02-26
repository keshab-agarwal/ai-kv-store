package workload

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// PerfResult holds aggregated performance metrics from a workload run.
type PerfResult struct {
	// TotalOps is the number of operations recorded (excludes ramp-up).
	TotalOps int

	// Duration is the measurement window (total workload time minus RampUp).
	Duration time.Duration

	// Throughput is operations per second during the measurement window.
	Throughput float64

	// Read latency percentiles.
	ReadLatencyP50  time.Duration
	ReadLatencyP99  time.Duration
	ReadLatencyP999 time.Duration

	// Write latency percentiles.
	WriteLatencyP50  time.Duration
	WriteLatencyP99  time.Duration
	WriteLatencyP999 time.Duration
}

// valuePool reduces GC pressure when generating many large values.
var valuePool = &sync.Pool{
	New: func() interface{} {
		b := make([]byte, interfaces.MaxValueSize)
		return &b
	},
}

// RunWorkload drives cfg.NumClients concurrent goroutines against cluster for
// cfg.Duration, returning the merged operation history and performance metrics.
//
// All operations are recorded (including during RampUp) so that the phantom-read
// checker can see ramp-up writes and will not produce false positives when reads
// during the measurement window observe values written during warm-up.
// PerfResult counts only operations whose CallTime falls after the ramp-up window.
func RunWorkload(cluster interfaces.Cluster, cfg WorkloadConfig) ([]recorder.HistoryEvent, PerfResult, error) {
	nodes := cluster.NodeIDs()
	if len(nodes) == 0 {
		return nil, PerfResult{}, nil
	}

	// Derive the ordered key space.
	keys := deriveKeys(cfg.NumKeys)

	// Track which key indices have been written so Gets target only written keys.
	var writtenMu sync.Mutex
	writtenSet := make(map[int]bool)

	// One recorder per client to avoid lock contention during the workload.
	recorders := make([]*recorder.Recorder, cfg.NumClients)
	for i := range recorders {
		recorders[i] = recorder.NewRecorder()
	}

	startTime := time.Now()
	measureStart := startTime.Add(cfg.RampUp)
	endTime := startTime.Add(cfg.Duration)

	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	for clientIdx := 0; clientIdx < cfg.NumClients; clientIdx++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Connect this client to its assigned node.
			var nodeID interfaces.NodeID
			if cfg.TargetNode != nil {
				nodeID = *cfg.TargetNode
			} else {
				nodeID = nodes[id%len(nodes)]
			}

			store, err := cluster.Connect(nodeID)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}

			rec := recorders[id]
			rs := recorder.NewRecordingStore(store, id, rec)

			rng := rand.New(rand.NewSource(int64(id) * 6364136223846793005))
			dist := newDistribution(cfg, int64(id))

			ctx := context.Background()

			for time.Now().Before(endTime) {
				keyIdx := dist.Next()
				key := keys[keyIdx]

				// Decide read vs write.
				isRead := rng.Float64() < cfg.ReadRatio

				// If no keys have been written yet, force a write.
				if isRead {
					writtenMu.Lock()
					empty := len(writtenSet) == 0
					writtenMu.Unlock()
					if empty {
						isRead = false
					}
				}
				// For reads, ensure the selected key has been written; if not,
				// pick one that has (or fall back to a write).
				if isRead {
					writtenMu.Lock()
					if !writtenSet[keyIdx] {
						writtenKeys := make([]int, 0, len(writtenSet))
						for k := range writtenSet {
							writtenKeys = append(writtenKeys, k)
						}
						if len(writtenKeys) > 0 {
							keyIdx = writtenKeys[rng.Intn(len(writtenKeys))]
							key = keys[keyIdx]
						} else {
							isRead = false
						}
					}
					writtenMu.Unlock()
				}

				// Always record via rs — ramp-up writes must appear in
				// writtenHashes to avoid phantom-read false positives.
				if isRead {
					rs.Get(ctx, key) //nolint:errcheck
				} else {
					size := nextValueSize(cfg, rng)
					val := generateValue(size, rng)
					putErr := rs.Put(ctx, key, val)
					returnVal(val)
					if putErr == nil {
						writtenMu.Lock()
						writtenSet[keyIdx] = true
						writtenMu.Unlock()
					}
				}
			}
		}(clientIdx)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, PerfResult{}, firstErr
	}

	// Merge histories from all clients.
	merged := recorder.Merge(recorders...)
	history := merged.GetHistory()

	measureDuration := endTime.Sub(measureStart)
	perf := computePerf(history, measureStart, measureDuration)

	return history, perf, nil
}

// deriveKeys produces a deterministic slice of NumKeys keys by hashing integers.
func deriveKeys(n int) []interfaces.Key {
	keys := make([]interfaces.Key, n)
	for i := 0; i < n; i++ {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		sum := md5.Sum(buf[:])
		copy(keys[i][:], sum[:])
	}
	return keys
}

// newDistribution constructs the key-selection distribution from the config.
func newDistribution(cfg WorkloadConfig, seed int64) Distribution {
	switch cfg.KeyDistribution {
	case "zipfian":
		theta := cfg.ZipfianConstant
		if theta == 0 {
			theta = 0.99
		}
		return NewZipfianDistribution(cfg.NumKeys, theta, seed)
	default:
		return NewUniformDistribution(cfg.NumKeys, seed)
	}
}

// nextValueSize returns the size in bytes for the next value (avg ValueSize, cap ValueSizeMax).
func nextValueSize(cfg WorkloadConfig, rng *rand.Rand) int {
	if cfg.ValueSizeMax <= 0 {
		return cfg.ValueSize
	}
	// Exponential with mean ValueSize, capped at ValueSizeMax.
	size := 1 + int(rng.ExpFloat64()*float64(cfg.ValueSize))
	if size > cfg.ValueSizeMax {
		size = cfg.ValueSizeMax
	}
	if size < interfaces.MinValueSize {
		size = interfaces.MinValueSize
	}
	return size
}

// generateValue creates a random value of the given size using the pool.
func generateValue(size int, rng *rand.Rand) interfaces.Value {
	bufPtr := valuePool.Get().(*[]byte)
	if size > cap(*bufPtr) {
		buf := make([]byte, size)
		rng.Read(buf)
		return interfaces.Value(buf)
	}
	buf := (*bufPtr)[:size]
	rng.Read(buf)
	return interfaces.Value(buf)
}

// returnVal returns a value buffer to the pool when it was from the pool.
func returnVal(v interfaces.Value) {
	if cap(v) >= interfaces.MaxValueSize {
		buf := ([]byte)(v[:interfaces.MaxValueSize])
		valuePool.Put(&buf)
	}
}

// computePerf derives PerfResult from a recorded history and the measurement window.
// Only operations whose CallTime is at or after measureStart are counted; ramp-up
// operations are excluded from throughput and latency metrics.
func computePerf(history []recorder.HistoryEvent, measureStart time.Time, duration time.Duration) PerfResult {
	var readLatencies, writeLatencies []time.Duration
	var total int

	for _, e := range history {
		if e.CallTime.Before(measureStart) {
			continue // exclude ramp-up operations
		}
		total++
		lat := e.ReturnTime.Sub(e.CallTime)
		if e.Type == recorder.OpGet {
			readLatencies = append(readLatencies, lat)
		} else {
			writeLatencies = append(writeLatencies, lat)
		}
	}

	secs := duration.Seconds()
	throughput := 0.0
	if secs > 0 {
		throughput = float64(total) / secs
	}

	return PerfResult{
		TotalOps:         total,
		Duration:         duration,
		Throughput:       throughput,
		ReadLatencyP50:   percentile(readLatencies, 50),
		ReadLatencyP99:   percentile(readLatencies, 99),
		ReadLatencyP999:  percentile(readLatencies, 99.9),
		WriteLatencyP50:  percentile(writeLatencies, 50),
		WriteLatencyP99:  percentile(writeLatencies, 99),
		WriteLatencyP999: percentile(writeLatencies, 99.9),
	}
}

// percentile returns the p-th percentile of a slice of durations.
// p is in [0, 100]. Returns 0 for empty slices.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	// Simple insertion-sort-based selection; for production use a proper sort.
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sortDurations(sorted)

	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}

// sortDurations sorts a slice of durations in ascending order.
func sortDurations(s []time.Duration) {
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
}
