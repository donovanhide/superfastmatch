package document

import (
	"fmt"
	"math/rand"
	"testing"
)

const maxSize = 10000000
const collProb = 0.15

var sizes = []uint64{10, 100, 1000, 10000, 100000, 1000000, 10000000}

func buildRandom() []uint64 {
	r, random, i := make(map[uint64]struct{}, maxSize), make([]uint64, maxSize), 0
	for len(r) < maxSize {
		n := uint64(rand.Int63())
		if _, ok := r[n]; !ok {
			r[n] = struct{}{}

			random[i] = n
			i++
		}
	}
	return random
}

func setBloom(b Bloom, r []uint64) {
	for i, length := 0, len(r); i < length; i++ {
		b.Set(r[i])
	}
}

func testBloom(b Bloom, r []uint64) {
	for i, length := 0, len(r); i < length; i++ {
		b.Test(r[i])
	}
}

func benchmarkBloomSet(b *testing.B, bloom Bloom, r []uint64) {
	setBloom(bloom, r)
}

func benchmarkBloomTest(b *testing.B, bloom Bloom, r []uint64) {
	setBloom(bloom, r)
	b.ResetTimer()
	testBloom(bloom, r)
}

func benchmarkBloomCount(b *testing.B, bloom Bloom, r []uint64) {
	setBloom(bloom, r)
	testBloom(bloom, r)
	b.ResetTimer()
	if bloom.Count() == 0 {
		b.Logf("Bad count\n")
	}
}

var benchmarks = []struct {
	name string
	test func(b *testing.B, bloom Bloom, r []uint64)
}{
	{"Set", benchmarkBloomSet},
	{"Test", benchmarkBloomTest},
	{"Count", benchmarkBloomCount},
}
var types = []struct {
	name        string
	constructor func(n uint64, p float64) Bloom
}{
	{"Fixed", NewFixedBloom},
	{"Dynamic", NewDynamicBloom},
}

func TestBenchmark(t *testing.T) {
	r := buildRandom()
	results := "Bloom Benchmark\n"
	for _, bench := range benchmarks {
		for _, size := range sizes {
			for _, t := range types {
				bloom := t.constructor(uint64(size), collProb)
				benchmark := testing.Benchmark(func(b *testing.B) { bench.test(b, bloom, r[:size]) })
				nsPerOp := float64(benchmark.T.Nanoseconds()) / float64(size)
				results += fmt.Sprintf("%v\t%v\tSize:\t%8d Bloom Size:%10d Bloom Count:%10d Speed:%6.2f ns/op\n", t.name, bench.name, size, bloom.Size(), bloom.Count(), nsPerOp)
			}
		}
	}
	t.Log(results)
}

func TestBloom(t *testing.T) {
	r := buildRandom()
	for _, size := range sizes {
		bloom := NewDynamicBloom(size, collProb)
		setBloom(bloom, r[:size])
		for i := 0; i < int(size); i++ {
			if bloom.Test(r[i]) != true {
				t.Logf("%v not set\n", i)
			}
		}
		t.Log(bloom)
	}
}
