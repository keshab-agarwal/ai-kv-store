package instrumentation

import (
	"testing"
	"time"
)

func TestRecordRequest(t *testing.T) {
	initialCount := requestCount.Value()
	initialLatencyTotal := latencyTotal.Value()

	latency := 100 * time.Millisecond
	RecordRequest(latency)

	if requestCount.Value() != initialCount+1 {
		t.Errorf("expected request count to be %d, got %d", initialCount+1, requestCount.Value())
	}

	if latencyTotal.Value() != initialLatencyTotal+int64(latency) {
		t.Errorf("expected latency total to be %d, got %d", initialLatencyTotal+int64(latency), latencyTotal.Value())
	}
}