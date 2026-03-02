// Package ycsb implements the YCSB (Yahoo! Cloud Serving Benchmark) workload
// engine for the distributed KV store.
package ycsb

import (
	"math"
	"math/rand"
	"sync"
)

// ZipfGenerator generates integers in [0, n) following a Zipfian distribution.
// Uses the "Zipfian with Hot Set" algorithm from the YCSB paper.
// theta=0.99 gives standard YCSB Zipfian skew (higher theta = more skew).
type ZipfGenerator struct {
	n     int64
	theta float64
	alpha float64
	zetan float64
	eta   float64
	mu    sync.Mutex
	rng   *rand.Rand
}

// zetaStatic computes zeta(n, theta) = sum_{i=1}^{n} 1/i^theta
// For large n, we approximate with the integral formula:
//
//	zeta(n,theta) ~ (n^(1-theta)-1)/(1-theta) + 1 + 0.5*n^(-theta) + theta/12 * n^(-theta-1)
//
// For small n we sum directly.
func zetaStatic(n int64, theta float64) float64 {
	if n <= 0 {
		return 0
	}
	// For n <= 1000, sum directly for accuracy.
	if n <= 1000 {
		sum := 0.0
		for i := int64(1); i <= n; i++ {
			sum += 1.0 / math.Pow(float64(i), theta)
		}
		return sum
	}
	// Euler-Maclaurin approximation for large n.
	// sum_{i=1}^{n} i^{-theta} ~ integral + correction terms
	// integral_1^{n} x^{-theta} dx = (n^{1-theta} - 1) / (1 - theta)
	sum := (math.Pow(float64(n), 1.0-theta) - 1.0) / (1.0 - theta)
	sum += 1.0 // f(1) correction
	sum += 0.5 * math.Pow(float64(n), -theta)
	sum += theta / 12.0 * math.Pow(float64(n), -theta-1.0)
	return sum
}

// NewZipfGenerator creates a new Zipfian generator over the range [0, n).
// theta controls the skew (0.99 is the YCSB default).
// seed initialises the random source for reproducibility.
func NewZipfGenerator(n int64, theta float64, seed int64) *ZipfGenerator {
	if n <= 0 {
		n = 1
	}
	zeta2 := zetaStatic(2, theta)
	zetan := zetaStatic(n, theta)
	alpha := 1.0 / (1.0 - theta)
	eta := (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - zeta2/zetan)

	return &ZipfGenerator{
		n:     n,
		theta: theta,
		alpha: alpha,
		zetan: zetan,
		eta:   eta,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// Next returns a value in [0, n) drawn from the Zipfian distribution.
// Lower values are returned much more frequently (hot keys).
// This method is safe for concurrent use.
func (z *ZipfGenerator) Next() int64 {
	z.mu.Lock()
	u := z.rng.Float64()
	z.mu.Unlock()

	uz := u * z.zetan

	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}

	v := int64(float64(z.n) * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
	if v < 0 {
		v = 0
	}
	if v >= z.n {
		v = z.n - 1
	}
	return v
}

// UniformGenerator generates integers in [0, n) with a uniform distribution.
// Used during the load phase to spread inserts evenly across all key slots.
type UniformGenerator struct {
	n   int64
	rng *rand.Rand
	mu  sync.Mutex
}

// NewUniformGenerator creates a new uniform random generator over [0, n).
// seed initialises the random source for reproducibility.
func NewUniformGenerator(n int64, seed int64) *UniformGenerator {
	if n <= 0 {
		n = 1
	}
	return &UniformGenerator{
		n:   n,
		rng: rand.New(rand.NewSource(seed)),
	}
}

// Next returns a uniformly random value in [0, n).
// This method is safe for concurrent use.
func (u *UniformGenerator) Next() int64 {
	u.mu.Lock()
	v := u.rng.Int63n(u.n)
	u.mu.Unlock()
	return v
}
