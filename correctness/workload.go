package correctness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerConfig configures the workload generator.
type WorkerConfig struct {
	Workers    int
	Duration   time.Duration
	ReadProp   float64 // proportion of reads,  e.g. 0.95
	PutProp    float64 // proportion of puts,   e.g. 0.04
	DeleteProp float64 // proportion of deletes, e.g. 0.01
	NumKeys    int     // number of distinct keys (default 1000)
	MaxValueSz int     // max value size in bytes (default 256)
	Seed       int64
}

// WorkloadStats aggregates statistics from all workers.
type WorkloadStats struct {
	TotalOps int64
	Errors   int64
	Timeouts int64
}

// worker is the internal per-goroutine state.
type worker struct {
	id         int
	nodes      []string // client HTTP addresses
	history    *History
	keys       []string // pre-generated 32-hex keys
	rng        *rand.Rand
	cfg        WorkerConfig
	clientSeq  uint64
	httpClient *http.Client

	ops      int64
	errors   int64
	timeouts int64
}

// RunWorkload runs concurrent workers for the given duration and returns aggregated stats.
func RunWorkload(nodes []string, history *History, cfg WorkerConfig) WorkloadStats {
	if cfg.NumKeys <= 0 {
		cfg.NumKeys = 1000
	}
	if cfg.MaxValueSz <= 0 {
		cfg.MaxValueSz = 256
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}

	// Pre-generate the key pool using the seed.
	masterRng := rand.New(rand.NewSource(cfg.Seed))
	keys := make([]string, cfg.NumKeys)
	for i := range keys {
		b := make([]byte, 16)
		_, _ = masterRng.Read(b)
		keys[i] = hex.EncodeToString(b)
	}

	var wg sync.WaitGroup
	workers := make([]*worker, cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := &worker{
			id:      i,
			nodes:   nodes,
			history: history,
			keys:    keys,
			rng:     rand.New(rand.NewSource(cfg.Seed + int64(i)*1000)),
			cfg:     cfg,
			httpClient: &http.Client{
				Timeout: 4 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 8,
					IdleConnTimeout:     30 * time.Second,
				},
			},
		}
		workers[i] = w
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			w.run(cfg.Duration)
		}(w)
	}
	wg.Wait()

	var stats WorkloadStats
	for _, w := range workers {
		stats.TotalOps += atomic.LoadInt64(&w.ops)
		stats.Errors += atomic.LoadInt64(&w.errors)
		stats.Timeouts += atomic.LoadInt64(&w.timeouts)
	}
	return stats
}

// run executes the worker loop for the given duration.
func (w *worker) run(duration time.Duration) {
	deadline := time.Now().Add(duration)
	// Cache the current best-known leader index.
	leaderIdx := w.rng.Intn(len(w.nodes))

	for time.Now().Before(deadline) {
		op := w.pickOp()
		keyIdx := w.rng.Intn(len(w.keys))
		key := w.keys[keyIdx]

		switch op {
		case OpGet:
			leaderIdx = w.doGet(key, leaderIdx)
		case OpPut:
			val := w.randomValue()
			leaderIdx = w.doPut(key, val, leaderIdx)
		case OpDelete:
			leaderIdx = w.doDelete(key, leaderIdx)
		}
		atomic.AddInt64(&w.ops, 1)
	}
}

// pickOp selects an operation type based on configured proportions.
func (w *worker) pickOp() OpType {
	r := w.rng.Float64()
	if r < w.cfg.ReadProp {
		return OpGet
	}
	r -= w.cfg.ReadProp
	if r < w.cfg.PutProp {
		return OpPut
	}
	return OpDelete
}

// randomValue generates a random byte slice of random length [1, MaxValueSz].
func (w *worker) randomValue() []byte {
	sz := 1 + w.rng.Intn(w.cfg.MaxValueSz)
	b := make([]byte, sz)
	_, _ = w.rng.Read(b)
	return b
}

// workerClientID returns the stable client identifier for this worker.
func (w *worker) workerClientID() string {
	return fmt.Sprintf("worker-%d", w.id)
}

// nextSeq returns the next operation sequence number for this worker.
func (w *worker) nextSeq() uint64 {
	return atomic.AddUint64(&w.clientSeq, 1)
}

// nodeAddr returns the HTTP address for the given index (wraps around).
func (w *worker) nodeAddr(idx int) string {
	return w.nodes[idx%len(w.nodes)]
}

// doGet performs a GET operation and records it in history.
// Returns the updated leader index hint.
func (w *worker) doGet(key string, leaderIdx int) int {
	callTime := time.Now().UnixNano()
	opID := w.history.BeginOp(w.id, OpGet, key, "", callTime)

	status, outputValue, newLeaderIdx := w.httpGet(key, leaderIdx)

	returnTime := time.Now().UnixNano()
	w.history.EndOp(opID, returnTime, status, outputValue)

	switch status {
	case "error":
		atomic.AddInt64(&w.errors, 1)
	case "timeout":
		atomic.AddInt64(&w.timeouts, 1)
	}

	return newLeaderIdx
}

// doPut performs a PUT operation and records it in history.
func (w *worker) doPut(key string, value []byte, leaderIdx int) int {
	b64Val := base64.StdEncoding.EncodeToString(value)
	callTime := time.Now().UnixNano()
	opID := w.history.BeginOp(w.id, OpPut, key, b64Val, callTime)

	seq := w.nextSeq()
	status, newLeaderIdx := w.httpPut(key, b64Val, seq, leaderIdx)

	returnTime := time.Now().UnixNano()
	w.history.EndOp(opID, returnTime, status, "")

	switch status {
	case "error":
		atomic.AddInt64(&w.errors, 1)
	case "timeout":
		atomic.AddInt64(&w.timeouts, 1)
	}

	return newLeaderIdx
}

// doDelete performs a DELETE operation and records it in history.
func (w *worker) doDelete(key string, leaderIdx int) int {
	callTime := time.Now().UnixNano()
	opID := w.history.BeginOp(w.id, OpDelete, key, "", callTime)

	seq := w.nextSeq()
	status, newLeaderIdx := w.httpDelete(key, seq, leaderIdx)

	returnTime := time.Now().UnixNano()
	w.history.EndOp(opID, returnTime, status, "")

	switch status {
	case "error":
		atomic.AddInt64(&w.errors, 1)
	case "timeout":
		atomic.AddInt64(&w.timeouts, 1)
	}

	return newLeaderIdx
}

// --------------------------------------------------------------------------
// Direct HTTP request types
// --------------------------------------------------------------------------

type getReq struct {
	Key string `json:"key"`
}

type getResp struct {
	Status string `json:"status"`
	Value  string `json:"value"`
	Leader string `json:"leader"`
}

type putReq struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

type putResp struct {
	Status string `json:"status"`
	Leader string `json:"leader"`
}

type delReq struct {
	Key       string `json:"key"`
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

type delResp struct {
	Status string `json:"status"`
	Leader string `json:"leader"`
}

// httpGet sends POST /api/get and follows redirects.
// Returns (status, outputValue, newLeaderIdx).
func (w *worker) httpGet(key string, leaderIdx int) (string, string, int) {
	const maxRedirects = 5
	for i := 0; i < maxRedirects; i++ {
		addr := w.nodeAddr(leaderIdx)
		url := "http://" + addr + "/api/get"

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var resp getResp
		err := postJSON(ctx, w.httpClient, url, getReq{Key: key}, &resp)
		cancel()

		if err != nil {
			return "error", "", leaderIdx
		}

		switch resp.Status {
		case "found":
			return "found", resp.Value, leaderIdx
		case "not_found":
			return "not_found", "", leaderIdx
		case "redirect":
			leaderIdx = w.resolveLeader(resp.Leader, leaderIdx)
			continue
		case "timeout":
			return "timeout", "", leaderIdx
		default:
			return "error", "", leaderIdx
		}
	}
	return "error", "", leaderIdx
}

// httpPut sends POST /api/put and follows redirects.
// Returns (status, newLeaderIdx).
func (w *worker) httpPut(key, b64Value string, seq uint64, leaderIdx int) (string, int) {
	const maxRedirects = 5
	req := putReq{
		Key:       key,
		Value:     b64Value,
		ClientID:  w.workerClientID(),
		ClientSeq: seq,
	}

	for i := 0; i < maxRedirects; i++ {
		addr := w.nodeAddr(leaderIdx)
		url := "http://" + addr + "/api/put"

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var resp putResp
		err := postJSON(ctx, w.httpClient, url, req, &resp)
		cancel()

		if err != nil {
			return "error", leaderIdx
		}

		switch resp.Status {
		case "ok":
			return "ok", leaderIdx
		case "redirect":
			leaderIdx = w.resolveLeader(resp.Leader, leaderIdx)
			continue
		case "timeout":
			return "timeout", leaderIdx
		default:
			return "error", leaderIdx
		}
	}
	return "error", leaderIdx
}

// httpDelete sends POST /api/delete and follows redirects.
// Returns (status, newLeaderIdx).
func (w *worker) httpDelete(key string, seq uint64, leaderIdx int) (string, int) {
	const maxRedirects = 5
	req := delReq{
		Key:       key,
		ClientID:  w.workerClientID(),
		ClientSeq: seq,
	}

	for i := 0; i < maxRedirects; i++ {
		addr := w.nodeAddr(leaderIdx)
		url := "http://" + addr + "/api/delete"

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var resp delResp
		err := postJSON(ctx, w.httpClient, url, req, &resp)
		cancel()

		if err != nil {
			return "error", leaderIdx
		}

		switch resp.Status {
		case "ok":
			return "ok", leaderIdx
		case "redirect":
			leaderIdx = w.resolveLeader(resp.Leader, leaderIdx)
			continue
		case "timeout":
			return "timeout", leaderIdx
		default:
			return "error", leaderIdx
		}
	}
	return "error", leaderIdx
}

// resolveLeader finds the node index for the given leader address hint.
// Falls back to round-robin if the address is not found in the node list.
func (w *worker) resolveLeader(leaderAddr string, currentIdx int) int {
	if leaderAddr == "" {
		return (currentIdx + 1) % len(w.nodes)
	}
	for i, addr := range w.nodes {
		if addr == leaderAddr {
			return i
		}
	}
	// Try suffix match (e.g., leader returns ":16101", we store "localhost:16101").
	for i, addr := range w.nodes {
		n := len(addr)
		m := len(leaderAddr)
		if m <= n && addr[n-m:] == leaderAddr {
			return i
		}
		if n <= m && leaderAddr[m-n:] == addr {
			return i
		}
	}
	return (currentIdx + 1) % len(w.nodes)
}

// --------------------------------------------------------------------------
// HTTP helpers
// --------------------------------------------------------------------------

// postJSON marshals body as JSON, POSTs to url with the given context, and
// decodes the JSON response into result.
func postJSON(ctx context.Context, httpClient *http.Client, url string, body, result interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(result)
}

// decodeJSON decodes a JSON HTTP response body into v.
func decodeJSON(resp *http.Response, v interface{}) error {
	return json.NewDecoder(resp.Body).Decode(v)
}
