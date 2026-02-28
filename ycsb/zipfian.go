package ycsb

import (
	"math"
	"math/rand"
)

// ZipfianGenerator produces integers following a Zipfian (power-law) distribution.
// Uses the rejection-inversion method for O(1) sample generation.
type ZipfianGenerator struct {
	rng       *rand.Rand
	items     int64
	theta     float64
	zetan     float64
	zeta2     float64
	alpha     float64
	eta       float64
	countforzeta int64
}

func NewZipfianGenerator(rng *rand.Rand, items int64, theta float64) *ZipfianGenerator {
	z := &ZipfianGenerator{
		rng:          rng,
		items:        items,
		theta:        theta,
		countforzeta: items,
	}
	z.zeta2 = zetaStatic(2, theta)
	z.zetan = zetaStatic(items, theta)
	z.alpha = 1.0 / (1.0 - theta)
	z.eta = (1.0 - math.Pow(2.0/float64(items), 1.0-theta)) / (1.0 - z.zeta2/z.zetan)
	return z
}

func (z *ZipfianGenerator) Next() int64 {
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

// ScrambledZipfian adds FNV hash scrambling to spread hot keys across the key space
type ScrambledZipfian struct {
	gen   *ZipfianGenerator
	min   int64
	max   int64
	count int64
}

func NewScrambledZipfian(rng *rand.Rand, min, max int64, theta float64) *ScrambledZipfian {
	count := max - min + 1
	return &ScrambledZipfian{
		gen:   NewZipfianGenerator(rng, count, theta),
		min:   min,
		max:   max,
		count: count,
	}
}

func (s *ScrambledZipfian) Next() int64 {
	raw := s.gen.Next()
	return s.min + fnvHash64(raw)%s.count
}

func zetaStatic(n int64, theta float64) float64 {
	sum := 0.0
	for i := int64(0); i < n; i++ {
		sum += 1.0 / math.Pow(float64(i+1), theta)
	}
	return sum
}

func fnvHash64(val int64) int64 {
	h := uint64(0xcbf29ce484222325)
	for i := 0; i < 8; i++ {
		h ^= uint64(val & 0xff)
		h *= 0x100000001b3
		val >>= 8
	}
	return int64(h & 0x7fffffffffffffff)
}
