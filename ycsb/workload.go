package ycsb

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// WorkloadConfig holds all parameters that control the benchmark workload.
type WorkloadConfig struct {
	// RecordCount is the total number of unique keys in the dataset.
	RecordCount int64

	// OperationCount is the total number of operations to perform during Run.
	OperationCount int64

	// ValueSize is the size of values in bytes.
	ValueSize int

	// ReadProportion is the fraction of operations that are reads (e.g. 0.95).
	ReadProportion float64

	// UpdateProportion is the fraction of operations that are updates (e.g. 0.04).
	UpdateProportion float64

	// InsertProportion is the fraction of operations that are inserts (e.g. 0.0).
	InsertProportion float64

	// DeleteProportion is the fraction of operations that are deletes (e.g. 0.01).
	DeleteProportion float64

	// ScanProportion is the fraction of operations that are scans (e.g. 0.0).
	ScanProportion float64

	// ZipfianTheta controls the skew of the Zipfian distribution (default 0.99).
	ZipfianTheta float64

	// Seed is the random seed for reproducibility.
	Seed int64

	// ThreadCount is the number of concurrent goroutines used during Run.
	ThreadCount int
}

// KeyGen generates deterministic YCSB keys from integer indices.
// Key[i] is derived from the 8-byte big-endian encoding of i, zero-padded to 16 bytes,
// yielding a deterministic 32-character hex string.
type KeyGen struct {
	recordCount int64
}

// NewKeyGen creates a KeyGen for datasets of recordCount keys.
func NewKeyGen(recordCount int64) *KeyGen {
	return &KeyGen{recordCount: recordCount}
}

// KeyFor returns the 32-char hex key string for index idx.
// The key is deterministic: the first 8 bytes are the big-endian encoding of idx,
// and the remaining 8 bytes are zero, giving a unique 128-bit key per index.
func (k *KeyGen) KeyFor(idx int64) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(idx))
	// bytes 8-15 remain zero, making each key unique in the full 128-bit space.
	return fmt.Sprintf("%016x%016x",
		binary.BigEndian.Uint64(buf[0:8]),
		binary.BigEndian.Uint64(buf[8:16]))
}

// Workload drives the YCSB benchmark against a DB backend.
type Workload struct {
	db      DB
	keyGen  *KeyGen
	cfg     WorkloadConfig
	zipf    *ZipfGenerator
	uniform *UniformGenerator
}

// NewWorkload creates a new Workload with the given configuration and DB backend.
func NewWorkload(cfg WorkloadConfig, db DB) *Workload {
	if cfg.ZipfianTheta == 0 {
		cfg.ZipfianTheta = 0.99
	}
	if cfg.ThreadCount <= 0 {
		cfg.ThreadCount = 1
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 1024
	}
	if cfg.RecordCount <= 0 {
		cfg.RecordCount = 1
	}
	if cfg.Seed == 0 {
		cfg.Seed = 42
	}

	return &Workload{
		db:      db,
		keyGen:  NewKeyGen(cfg.RecordCount),
		cfg:     cfg,
		zipf:    NewZipfGenerator(cfg.RecordCount, cfg.ZipfianTheta, cfg.Seed),
		uniform: NewUniformGenerator(cfg.RecordCount, cfg.Seed+1),
	}
}

// randomValue generates a random byte slice of size cfg.ValueSize.
func (w *Workload) randomValue(rng *rand.Rand) []byte {
	v := make([]byte, w.cfg.ValueSize)
	for i := range v {
		v[i] = byte(rng.Intn(256))
	}
	return v
}

// Load inserts all RecordCount keys sequentially into the database.
// It uses w.cfg.ThreadCount goroutines to parallelise inserts.
// Progress and errors are recorded via the reporter.
func (w *Workload) Load(reporter *Reporter) error {
	ctx := context.Background()
	return w.LoadContext(ctx, reporter)
}

// LoadContext is like Load but respects context cancellation.
func (w *Workload) LoadContext(ctx context.Context, reporter *Reporter) error {
	total := w.cfg.RecordCount
	threads := w.cfg.ThreadCount
	if threads <= 0 {
		threads = 1
	}

	// Partition keys evenly across threads.
	perThread := total / int64(threads)
	remainder := total % int64(threads)

	var wg sync.WaitGroup
	var insertErr atomic.Value // stores first error seen

	for t := 0; t < threads; t++ {
		start := int64(t) * perThread
		end := start + perThread
		if t == threads-1 {
			end += remainder
		}

		wg.Add(1)
		go func(startIdx, endIdx int64, threadSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(threadSeed))
			for i := startIdx; i < endIdx; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := w.keyGen.KeyFor(i)
				value := w.randomValue(rng)

				t0 := time.Now()
				status, err := w.db.Insert(key, value)
				latUs := time.Since(t0).Microseconds()

				if err != nil {
					if insertErr.Load() == nil {
						insertErr.Store(err)
					}
				}
				reporter.RecordOp("INSERT", latUs, status)
			}
		}(start, end, w.cfg.Seed+int64(t)*1000)
	}

	wg.Wait()

	if err, ok := insertErr.Load().(error); ok && err != nil {
		return fmt.Errorf("load phase error: %w", err)
	}
	return nil
}

// Run executes the benchmark workload for OperationCount operations using
// w.cfg.ThreadCount concurrent goroutines. Operations are distributed
// according to the configured proportions (Read/Update/Delete).
// Zipfian distribution is used for key selection.
func (w *Workload) Run(reporter *Reporter) {
	ctx := context.Background()
	w.RunContext(ctx, reporter)
}

// RunContext is like Run but respects context cancellation.
func (w *Workload) RunContext(ctx context.Context, reporter *Reporter) {
	total := w.cfg.OperationCount
	threads := w.cfg.ThreadCount
	if threads <= 0 {
		threads = 1
	}

	// Each thread gets an equal share of operations.
	opsPerThread := total / int64(threads)
	remainder := total % int64(threads)

	var wg sync.WaitGroup

	for t := 0; t < threads; t++ {
		myOps := opsPerThread
		if int64(t) < remainder {
			myOps++
		}

		wg.Add(1)
		go func(ops int64, threadSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(threadSeed))

			readBound := w.cfg.ReadProportion
			updateBound := readBound + w.cfg.UpdateProportion

			for i := int64(0); i < ops; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Choose operation type.
				pick := rng.Float64()

				if pick < readBound {
					// READ
					idx := w.zipf.Next()
					key := w.keyGen.KeyFor(idx)
					t0 := time.Now()
					status, _, err := w.db.Read(key)
					latUs := time.Since(t0).Microseconds()
					_ = err
					reporter.RecordOp("READ", latUs, status)

				} else if pick < updateBound {
					// UPDATE
					idx := w.zipf.Next()
					key := w.keyGen.KeyFor(idx)
					value := w.randomValue(rng)
					t0 := time.Now()
					status, err := w.db.Update(key, value)
					latUs := time.Since(t0).Microseconds()
					_ = err
					reporter.RecordOp("UPDATE", latUs, status)

				} else {
					// DELETE: use uniform distribution to avoid exhausting all keys.
					idx := w.uniform.Next()
					key := w.keyGen.KeyFor(idx)
					t0 := time.Now()
					status, err := w.db.Delete(key)
					latUs := time.Since(t0).Microseconds()
					_ = err
					reporter.RecordOp("DELETE", latUs, status)
				}
			}
		}(myOps, w.cfg.Seed+int64(t)*7919)
	}

	wg.Wait()
}

