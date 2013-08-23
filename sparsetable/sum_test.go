package sparsetable

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

type TestSlice []uint8

func buildTestSlice(length int, capacity int) TestSlice {
	test := make(TestSlice, length, capacity)
	for i := range test {
		test[i] = uint8(rand.Uint32())
	}
	return test
}

func (s TestSlice) Generate(rand *rand.Rand, size int) reflect.Value {
	length := rand.Int() % (1 << 16)
	return reflect.ValueOf(buildTestSlice(length, length+rand.Int()%1024))
}

func TestSmallSumBytes(t *testing.T) {
	slice := buildTestSlice(32, 32)
	for i := range slice {
		slow := SumUint8(slice[:i])
		fast := FastSumUint8(slice[:i])
		if fast != slow {
			t.Errorf("%v %v Fast: %v Slow: %v", i, slice[:i], fast, slow)
		}
	}
}

func TestSumBytes(t *testing.T) {
	config := &quick.Config{MaxCount: 1 << 10}
	fast := func(x TestSlice) uint64 { return FastSumUint8(x) }
	slow := func(x TestSlice) uint64 { return SumUint8(x) }
	if err := quick.CheckEqual(fast, slow, config); err != nil {
		t.Error(err)
	}
}

func runBenchmark(f func(x []uint8) uint64, numbers []uint8, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(numbers)
	}
}

func TestBenchmark(t *testing.T) {
	benchmarks := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24, 31, 32, 63, 64, 100, 135, 256, 1 << 10}
	results := "Benchmark Results\n"
	for _, n := range benchmarks {
		slice := buildTestSlice(n, n)
		fast := testing.Benchmark(func(b *testing.B) { runBenchmark(FastSumUint8, slice, b) })
		slow := testing.Benchmark(func(b *testing.B) { runBenchmark(SumUint8, slice, b) })
		fastNsPerOp := float64(fast.T.Nanoseconds()) / float64(fast.N)
		slowNsPerOp := float64(slow.T.Nanoseconds()) / float64(slow.N)
		results += fmt.Sprintf("Length: %8d Fast:%12.2f ns/op Slow:%12.2f ns/op Improvement %6.2f%%\n", n, fastNsPerOp, slowNsPerOp, ((1/(fastNsPerOp/slowNsPerOp))-1)*100)
	}
	t.Log(results)
}
