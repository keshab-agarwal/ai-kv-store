package ycsb

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Workload struct {
	cfg   WorkloadConfig
	db    DB
	value []byte
}

func NewWorkload(cfg WorkloadConfig, db DB) *Workload {
	if cfg.ThreadCount <= 0 {
		cfg.ThreadCount = 1
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 1024
	}
	if cfg.ZipfianTheta <= 0 {
		cfg.ZipfianTheta = 0.99
	}

	rng := rand.New(rand.NewSource(42))
	value := make([]byte, cfg.ValueSize)
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}

	return &Workload{
		cfg:   cfg,
		db:    db,
		value: value,
	}
}

func (w *Workload) Load(rep *Reporter) error {
	total := w.cfg.RecordCount
	threads := w.cfg.ThreadCount
	perThread := total / int64(threads)

	rep.Start()

	var wg sync.WaitGroup
	var loadErr error
	var errOnce sync.Once

	for t := 0; t < threads; t++ {
		start := int64(t) * perThread
		end := start + perThread
		if t == threads-1 {
			end = total
		}

		wg.Add(1)
		go func(start, end int64) {
			defer wg.Done()
			for i := start; i < end; i++ {
				key := BuildKeyName(i)
				t0 := time.Now()
				status, err := w.db.Insert(key, w.value)
				dur := time.Since(t0)

				if err != nil || status == StatusError {
					rep.Record(OpInsert, dur, fmt.Errorf("insert %s: %v", key, err))
					if err != nil {
						errOnce.Do(func() { loadErr = err })
					}
				} else {
					rep.Record(OpInsert, dur, nil)
				}
			}
		}(start, end)
	}

	wg.Wait()
	rep.Stop()
	return loadErr
}

func (w *Workload) Run(rep *Reporter) {
	total := w.cfg.OperationCount
	threads := w.cfg.ThreadCount
	perThread := total / int64(threads)

	readCum := w.cfg.ReadProportion
	updateCum := readCum + w.cfg.UpdateProportion
	insertCum := updateCum + w.cfg.InsertProportion

	var insertSeq atomic.Int64
	insertSeq.Store(w.cfg.RecordCount)

	rep.Start()

	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		ops := perThread
		if t == threads-1 {
			ops = total - perThread*int64(threads-1)
		}

		wg.Add(1)
		go func(ops int64, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			zipf := NewScrambledZipfianGenerator(w.cfg.RecordCount, w.cfg.ZipfianTheta, rng)

			for i := int64(0); i < ops; i++ {
				dice := rng.Float64()
				keyIdx := zipf.Next()
				key := BuildKeyName(keyIdx)

				var op OpType
				var t0 time.Time
				var opErr error

				switch {
				case dice < readCum:
					op = OpRead
					t0 = time.Now()
					_, _, opErr = w.db.Read(key)
				case dice < updateCum:
					op = OpUpdate
					t0 = time.Now()
					_, opErr = w.db.Update(key, w.value)
				case dice < insertCum:
					op = OpInsert
					newKey := BuildKeyName(insertSeq.Add(1) - 1)
					t0 = time.Now()
					_, opErr = w.db.Insert(newKey, w.value)
				default:
					op = OpDelete
					t0 = time.Now()
					_, opErr = w.db.Delete(key)
				}

				rep.Record(op, time.Since(t0), opErr)
			}
		}(ops, time.Now().UnixNano()+int64(t)*7919)
	}

	wg.Wait()
	rep.Stop()
}
