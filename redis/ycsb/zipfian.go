package ycsb

import (
	"encoding/binary"
	"math"
	"math/rand"
)

// zetaApproxThreshold is the number of terms computed exactly before
// switching to an integral approximation for the generalized harmonic sum.
const zetaApproxThreshold = 10_000_000

// ScrambledZipfianGenerator produces Zipfian-distributed values with FNV64
// scrambling so that hot items are spread across the key space rather than
// clustered at low indices.
type ScrambledZipfianGenerator struct {
	items int64
	gen   *zipfianGenerator
}

func NewScrambledZipfianGenerator(items int64, theta float64, rng *rand.Rand) *ScrambledZipfianGenerator {
	if items <= 0 {
		items = 1
	}
	return &ScrambledZipfianGenerator{
		items: items,
		gen:   newZipfianGenerator(items, theta, rng),
	}
}

func (s *ScrambledZipfianGenerator) Next() int64 {
	raw := s.gen.next()
	return fnv64Scramble(raw) % s.items
}

// zipfianGenerator implements the closed-form Zipfian generation algorithm
// used in the original YCSB benchmark.
type zipfianGenerator struct {
	items int64
	theta float64
	alpha float64
	eta   float64
	zetan float64
	rng   *rand.Rand
}

func newZipfianGenerator(items int64, theta float64, rng *rand.Rand) *zipfianGenerator {
	zeta2 := zetaN(2, theta)
	zetan := zetaN(items, theta)

	oneMinusTheta := 1.0 - theta
	alpha := 1.0 / oneMinusTheta
	eta := (1.0 - math.Pow(2.0/float64(items), oneMinusTheta)) / (1.0 - zeta2/zetan)

	return &zipfianGenerator{
		items: items,
		theta: theta,
		alpha: alpha,
		eta:   eta,
		zetan: zetan,
		rng:   rng,
	}
}

func (z *zipfianGenerator) next() int64 {
	u := z.rng.Float64()
	uz := u * z.zetan

	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}
	return int64(float64(z.items) * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
}

// zetaN computes the generalized harmonic number H(n, theta).
// Exact summation up to zetaApproxThreshold terms, then integral
// approximation for the tail — keeps startup fast even for n > 1B.
func zetaN(n int64, theta float64) float64 {
	if n <= 0 {
		return 0
	}

	limit := n
	if limit > zetaApproxThreshold {
		limit = zetaApproxThreshold
	}

	var sum float64
	for i := int64(1); i <= limit; i++ {
		sum += 1.0 / math.Pow(float64(i), theta)
	}

	if n > zetaApproxThreshold {
		oneMinusTheta := 1.0 - theta
		sum += (math.Pow(float64(n), oneMinusTheta) - math.Pow(float64(zetaApproxThreshold), oneMinusTheta)) / oneMinusTheta
	}

	return sum
}

func fnv64Scramble(val int64) int64 {
	hash := uint64(0xcbf29ce484222325)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(val))
	for _, b := range buf {
		hash ^= uint64(b)
		hash *= 0x100000001b3
	}
	return int64(hash & 0x7fffffffffffffff)
}
