package workload

import (
	"math"
	"math/rand"
)

// Distribution generates integer keys in [0, n).
type Distribution interface {
	// Next returns the next key index.
	Next() int
}

// uniformDist returns keys uniformly at random from [0, n).
type uniformDist struct {
	n   int
	rng *rand.Rand
}

// NewUniformDistribution creates a uniform distribution over [0, n).
func NewUniformDistribution(n int, seed int64) Distribution {
	return &uniformDist{n: n, rng: rand.New(rand.NewSource(seed))}
}

func (u *uniformDist) Next() int {
	return u.rng.Intn(u.n)
}

// zipfianDist implements the scrambled Zipfian distribution from the YCSB paper.
//
// The scrambled variant hashes the raw Zipfian variate to avoid hot-key locality
// (the most popular key in plain Zipfian is always key 0; scrambling spreads load
// across the key space while preserving the overall frequency distribution shape).
//
// Algorithm: Zipf-Mandelbrot distribution with the standard YCSB parameters.
type zipfianDist struct {
	n       int
	theta   float64
	alpha   float64
	zetan   float64
	eta     float64
	rng     *rand.Rand
}

// NewZipfianDistribution creates a scrambled Zipfian distribution over [0, n)
// with the given skew constant theta (typically 0.99).
func NewZipfianDistribution(n int, theta float64, seed int64) Distribution {
	rng := rand.New(rand.NewSource(seed))
	zetan := zetaStatic(n, theta)
	zeta2 := zetaStatic(2, theta)
	alpha := 1.0 / (1.0 - theta)
	eta := (1 - math.Pow(2.0/float64(n), 1-theta)) / (1 - zeta2/zetan)
	return &zipfianDist{
		n:     n,
		theta: theta,
		alpha: alpha,
		zetan: zetan,
		eta:   eta,
		rng:   rng,
	}
}

// Next returns a key index sampled from the scrambled Zipfian distribution.
func (z *zipfianDist) Next() int {
	u := z.rng.Float64()
	uz := u * z.zetan

	var raw int
	switch {
	case uz < 1.0:
		raw = 0
	case uz < 1.0+math.Pow(0.5, z.theta):
		raw = 1
	default:
		raw = int(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
		if raw >= z.n {
			raw = z.n - 1
		}
	}

	// Scramble: FNV-1a hash to spread hot keys across the key space.
	return int(fnv1a(uint64(raw)) % uint64(z.n))
}

// zetaStatic computes the Riemann zeta sum for the Zipfian distribution:
//
//	sum_{i=1}^{n} 1/i^theta
func zetaStatic(n int, theta float64) float64 {
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += math.Pow(1.0/float64(i), theta)
	}
	return sum
}

// fnv1a computes a 64-bit FNV-1a hash of v.
func fnv1a(v uint64) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	b := [8]byte{
		byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56),
	}
	h := offset
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
