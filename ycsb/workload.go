package ycsb

import (
	"crypto/md5"
	"math"
	"math/rand"
	"sync"
)

// -----------------------------------------------------------------------
// Scrambled Zipfian generator (matches YCSB paper / reference impl)
// -----------------------------------------------------------------------

const (
	zipfianConstant = 0.99
	// FNV offset basis used to scramble the output of the Zipfian so that
	// popular items are scattered across the key space rather than bunched at
	// the low end.
	fnvOffsetBasis64 = 14695981039346656037
	fnvPrime64       = 1099511628211
)

// ZipfianGenerator produces integers in [0, itemCount) following a Zipfian
// (power-law) distribution, then scrambles them with FNV to spread hot items.
type ZipfianGenerator struct {
	mu        sync.Mutex
	rng       *rand.Rand
	itemCount int64
	theta     float64
	zetan     float64
	zeta2     float64
	alpha     float64
	eta       float64
}

// NewZipfianGenerator creates a scrambled Zipfian generator over [0, n).
func NewZipfianGenerator(n int64, seed int64) *ZipfianGenerator {
	theta := zipfianConstant
	zeta2 := zetaStatic(2, theta)
	zetan := zetaStatic(n, theta)

	alpha := 1.0 / (1.0 - theta)
	eta := (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - zeta2/zetan)

	return &ZipfianGenerator{
		rng:       rand.New(rand.NewSource(seed)),
		itemCount: n,
		theta:     theta,
		zetan:     zetan,
		zeta2:     zeta2,
		alpha:     alpha,
		eta:       eta,
	}
}

// Next returns the next scrambled-Zipfian value in [0, itemCount).
func (z *ZipfianGenerator) Next() int64 {
	z.mu.Lock()
	v := z.nextZipfian()
	z.mu.Unlock()
	return scramble(v)
}

func (z *ZipfianGenerator) nextZipfian() int64 {
	u := z.rng.Float64()
	uz := u * z.zetan
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}
	return int64(float64(z.itemCount) * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
}

func scramble(v int64) int64 {
	h := uint64(v)
	h = h ^ fnvOffsetBasis64
	h = h * fnvPrime64
	// Make sure the result is non-negative.
	return int64(h & 0x7fffffffffffffff)
}

// zetaStatic computes the generalized harmonic number of order theta for n items.
func zetaStatic(n int64, theta float64) float64 {
	var sum float64
	for i := int64(0); i < n; i++ {
		sum += 1.0 / math.Pow(float64(i+1), theta)
	}
	return sum
}

// -----------------------------------------------------------------------
// Key helpers
// -----------------------------------------------------------------------

// KeyFromID deterministically derives a 128-bit key from an integer ID
// using MD5.
func KeyFromID(id int64) [16]byte {
	b := make([]byte, 8)
	b[0] = byte(id >> 56)
	b[1] = byte(id >> 48)
	b[2] = byte(id >> 40)
	b[3] = byte(id >> 32)
	b[4] = byte(id >> 24)
	b[5] = byte(id >> 16)
	b[6] = byte(id >> 8)
	b[7] = byte(id)
	return md5.Sum(b)
}

// -----------------------------------------------------------------------
// Operation types
// -----------------------------------------------------------------------

// OpType is the type of a YCSB operation.
type OpType int

const (
	OpRead   OpType = iota
	OpUpdate        // PUT with new value
	OpDelete
)

func (o OpType) String() string {
	switch o {
	case OpRead:
		return "READ"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// -----------------------------------------------------------------------
// Workload
// -----------------------------------------------------------------------

// WorkloadConfig holds the tunables for a YCSB workload.
type WorkloadConfig struct {
	RecordCount    int64
	OperationCount int64
}

// Workload generates operations according to the YCSB workload-B-like mix:
//
//	95 % READ, 4 % UPDATE, 1 % DELETE, 0 % SCAN, 0 % INSERT
type Workload struct {
	cfg   WorkloadConfig
	zipf  *ZipfianGenerator
	mu    sync.Mutex
	opRng *rand.Rand
}

// NewWorkload creates a workload with the given config and random seed.
func NewWorkload(cfg WorkloadConfig, seed int64) *Workload {
	return &Workload{
		cfg:   cfg,
		zipf:  NewZipfianGenerator(cfg.RecordCount, seed),
		opRng: rand.New(rand.NewSource(seed + 1)),
	}
}

// NextOp returns the operation type and the target key (chosen via scrambled
// Zipfian). Safe for concurrent use.
func (w *Workload) NextOp() (OpType, [16]byte) {
	// Choose key via Zipfian over [0, RecordCount).
	raw := w.zipf.Next() % w.cfg.RecordCount
	key := KeyFromID(raw)

	// Choose operation type.
	w.mu.Lock()
	r := w.opRng.Intn(100)
	w.mu.Unlock()

	var op OpType
	switch {
	case r < 95:
		op = OpRead
	case r < 99:
		op = OpUpdate
	default:
		op = OpDelete
	}
	return op, key
}

// RandomValue returns a random 1 KB value (for inserts / updates).
func RandomValue(rng *rand.Rand) []byte {
	buf := make([]byte, 1024)
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
}
