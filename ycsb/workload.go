package ycsb

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type OpKind int

const (
	OpRead   OpKind = 0
	OpInsert OpKind = 1
	OpUpdate OpKind = 2
	OpDel    OpKind = 3
)

type WorkloadConfig struct {
	RecordCount     int64
	OperationCount  int64
	ReadProportion  float64
	UpdateProportion float64
	InsertProportion float64
	DeleteProportion float64
	ScanProportion  float64
	ZipfianTheta    float64
	ValueSize       int
	ThreadCount     int
}

func DefaultWorkloadConfig() WorkloadConfig {
	return WorkloadConfig{
		RecordCount:      1_000_000,
		OperationCount:   1_000_000,
		ReadProportion:   0.95,
		UpdateProportion: 0.04,
		InsertProportion: 0.0,
		DeleteProportion: 0.01,
		ScanProportion:   0.0,
		ZipfianTheta:     0.99,
		ValueSize:        1024,
		ThreadCount:      8,
	}
}

type Workload struct {
	config WorkloadConfig
	db     DB
}

func NewWorkload(cfg WorkloadConfig, db DB) *Workload {
	return &Workload{config: cfg, db: db}
}

func (w *Workload) Load(reporter *Reporter) error {
	var wg sync.WaitGroup
	perThread := w.config.RecordCount / int64(w.config.ThreadCount)
	var errCount atomic.Int64

	for t := 0; t < w.config.ThreadCount; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			start := int64(threadID) * perThread
			end := start + perThread
			if threadID == w.config.ThreadCount-1 {
				end = w.config.RecordCount
			}
			value := make([]byte, w.config.ValueSize)
			rand.Read(value)

			for i := start; i < end; i++ {
				key := fmt.Sprintf("user%012d", i)
				t0 := time.Now()
				status, err := w.db.Insert(key, value)
				latency := time.Since(t0)
				if err != nil || (status != 0 && status != 1) {
					errCount.Add(1)
				}
				reporter.Record(OpInsert, latency)
			}
		}(t)
	}
	wg.Wait()

	if ec := errCount.Load(); ec > 0 {
		return fmt.Errorf("load phase: %d errors", ec)
	}
	return nil
}

func (w *Workload) Run(reporter *Reporter) {
	var wg sync.WaitGroup
	perThread := w.config.OperationCount / int64(w.config.ThreadCount)

	for t := 0; t < w.config.ThreadCount; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))
			keyChooser := NewScrambledZipfian(rng, 0, w.config.RecordCount-1, w.config.ZipfianTheta)
			value := make([]byte, w.config.ValueSize)

			ops := perThread
			if threadID == w.config.ThreadCount-1 {
				ops = w.config.OperationCount - perThread*int64(w.config.ThreadCount-1)
			}

			for i := int64(0); i < ops; i++ {
				op := w.chooseOp(rng)
				keyNum := keyChooser.Next()
				key := fmt.Sprintf("user%012d", keyNum)

				t0 := time.Now()
				switch op {
				case OpRead:
					w.db.Read(key)
				case OpUpdate:
					rng.Read(value)
					w.db.Update(key, value)
				case OpInsert:
					rng.Read(value)
					w.db.Insert(key, value)
				case OpDel:
					w.db.Delete(key)
				}
				latency := time.Since(t0)
				reporter.Record(op, latency)
			}
		}(t)
	}
	wg.Wait()
}

func (w *Workload) chooseOp(rng *rand.Rand) OpKind {
	r := rng.Float64()
	cumulative := w.config.ReadProportion
	if r < cumulative {
		return OpRead
	}
	cumulative += w.config.UpdateProportion
	if r < cumulative {
		return OpUpdate
	}
	cumulative += w.config.InsertProportion
	if r < cumulative {
		return OpInsert
	}
	return OpDel
}
