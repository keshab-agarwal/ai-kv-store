package instrumentation

import (
	"sync"
	"time"
)

type Trace struct {
	StartTime time.Time
	EndTime   time.Time
	Mutex     sync.Mutex
}

func NewTrace() *Trace {
	return &Trace{StartTime: time.Now()}
}

func (t *Trace) End() {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.EndTime = time.Now()
}

func (t *Trace) Latency() time.Duration {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.EndTime.Sub(t.StartTime)
}