package instrumentation

import (
	"expvar"
	"sync"
	"time"
)

var (
	requestCount   = expvar.NewInt("request_count")
	latencyTotal   = expvar.NewInt("latency_total")
	latencyCount   = expvar.NewInt("latency_count")
	latencyMax     = expvar.NewInt("latency_max")
	latencyMin     = expvar.NewInt("latency_min")
	latencyMinOnce sync.Once
)

func RecordRequest(latency time.Duration) {
	requestCount.Add(1)
	latencyTotal.Add(int64(latency))
	latencyCount.Add(1)

	latencyValue := int64(latency)
	if latencyValue > latencyMax.Value() {
		latencyMax.Set(latencyValue)
	}

	latencyMinOnce.Do(func() {
		latencyMin.Set(latencyValue)
	})

	if latencyValue < latencyMin.Value() {
		latencyMin.Set(latencyValue)
	}
}