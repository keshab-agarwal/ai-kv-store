package correctness

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/kvstore"
	"ai-kv-store/kvstore/client"
)

type WorkloadConfig struct {
	Duration      time.Duration
	Workers       int
	MaxValueSize  int
	ReadRatio     float64
	PutRatio      float64
	DeleteRatio   float64
}

func DefaultCorrectnessWorkload() WorkloadConfig {
	return WorkloadConfig{
		Duration:     30 * time.Second,
		Workers:      10,
		MaxValueSize: 256,
		ReadRatio:    0.95,
		PutRatio:     0.04,
		DeleteRatio:  0.01,
	}
}

type WorkloadRunner struct {
	config   WorkloadConfig
	nodes    []string
	recorder *HistoryRecorder
	logger   *log.Logger
	opSeq    atomic.Int64
}

func NewWorkloadRunner(cfg WorkloadConfig, nodes []string, recorder *HistoryRecorder, logger *log.Logger) *WorkloadRunner {
	return &WorkloadRunner{
		config:   cfg,
		nodes:    nodes,
		recorder: recorder,
		logger:   logger,
	}
}

func (w *WorkloadRunner) Run() (gets, puts, deletes, errors, timeouts int) {
	var wg sync.WaitGroup
	deadline := time.Now().Add(w.config.Duration)

	type counters struct {
		gets, puts, deletes, errors, timeouts int
	}
	results := make([]counters, w.config.Workers)

	for i := 0; i < w.config.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c := client.NewWithID(w.nodes, fmt.Sprintf("w%d", workerID))
			rng := mrand.New(mrand.NewSource(time.Now().UnixNano() + int64(workerID)))
			var local counters

			knownKeys := make([]kvstore.Key, 0, 100)

			for time.Now().Before(deadline) {
				opID := w.opSeq.Add(1)
				r := rng.Float64()

				var key kvstore.Key
				if len(knownKeys) > 0 {
					key = knownKeys[rng.Intn(len(knownKeys))]
				} else {
					rand.Read(key[:])
				}

				if r < w.config.PutRatio {
					rand.Read(key[:])
					valSize := 1 + rng.Intn(w.config.MaxValueSize)
					value := make([]byte, valSize)
					rand.Read(value)

					callTime := NowNano()
					result := c.Put(key, value)
					returnTime := NowNano()

					w.recorder.Record(HistoryEvent{
						OpID:       opID,
						ClientID:   c.ClientID(),
						CallTime:   callTime,
						ReturnTime: returnTime,
						OpType:     "Put",
						Key:        key.Hex(),
						InputValue: hex.EncodeToString(value),
						Output:     result.Status.String(),
					})

					if result.Status == kvstore.StatusOK {
						knownKeys = append(knownKeys, key)
						local.puts++
					} else if result.Status == kvstore.StatusTimeout {
						// TIMEOUT strategy: record as-is. The Porcupine checker
						// treats TIMEOUT operations as having unbounded return time,
						// meaning the operation may or may not have taken effect.
						// This is sound: the checker will try both possibilities.
						local.timeouts++
					} else {
						local.errors++
					}

				} else if r < w.config.PutRatio+w.config.DeleteRatio {
					callTime := NowNano()
					result := c.Delete(key)
					returnTime := NowNano()

					w.recorder.Record(HistoryEvent{
						OpID:       opID,
						ClientID:   c.ClientID(),
						CallTime:   callTime,
						ReturnTime: returnTime,
						OpType:     "Delete",
						Key:        key.Hex(),
						Output:     result.Status.String(),
					})

					if result.Status == kvstore.StatusTimeout {
						local.timeouts++
					} else if result.Status == kvstore.StatusError {
						local.errors++
					} else {
						local.deletes++
					}

				} else {
					callTime := NowNano()
					result := c.Get(key)
					returnTime := NowNano()

					outputVal := ""
					if result.Status == kvstore.StatusFound {
						outputVal = hex.EncodeToString(result.Value)
					}

					w.recorder.Record(HistoryEvent{
						OpID:        opID,
						ClientID:    c.ClientID(),
						CallTime:    callTime,
						ReturnTime:  returnTime,
						OpType:      "Get",
						Key:         key.Hex(),
						Output:      result.Status.String(),
						OutputValue: outputVal,
					})

					if result.Status == kvstore.StatusTimeout {
						local.timeouts++
					} else if result.Status == kvstore.StatusError {
						local.errors++
					} else {
						local.gets++
					}
				}
			}
			results[workerID] = local
		}(i)
	}

	wg.Wait()

	for _, r := range results {
		gets += r.gets
		puts += r.puts
		deletes += r.deletes
		errors += r.errors
		timeouts += r.timeouts
	}
	return
}
