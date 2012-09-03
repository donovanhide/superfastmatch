package sparsetable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkSparseTable(b *testing.B) {
	const width uint64 = 1 << 24
	const length int32 = 255
	const groupSize uint64 = 48
	A := Init(width, groupSize)
	R := make([]byte, length)
	for i := 0; i < b.N; i++ {
		A.Set(uint64(rand.Int63n(int64(width))), R[:rand.Int31n(length)])
	}
}

func BenchmarkMap(b *testing.B) {
	const width uint64 = 1 << 27
	const length int32 = 255
	const groupSize uint64 = 48
	A := make(map[uint64][]byte)
	R := make([]byte, length)
	for i := 0; i < b.N; i++ {
		A[uint64(rand.Int63n(int64(width)))] = R[:rand.Int31n(length)]
	}
}

func Test_SparseTable(t *testing.T) {
	A := Init(1024, 48)
	if A.Size() != 1024 {
		t.Error("Incorrect Size.")
	}
	if A.Count() != 0 {
		t.Error("Incorrect Count.")
	}
	if A.Memory() == 0 {
		t.Error("Incorrect Memory Usage.")
	}
	smallValue := []byte("testing")
	if err := A.Set(1, smallValue); err != nil {
		t.Error("Could not set value.")
	}
	if A.Size() != 1024 {
		t.Error("Incorrect Size.")
	}
	if A.Count() != 1 {
		t.Error("Incorrect Count.")
	}
	if err := A.Set(1025, smallValue); err == nil {
		t.Error("Out of range position accepted.")
	}
	if err := A.Remove(1025); err == nil {
		t.Error("Out of range position accepted.")
	}
	if value, _ := A.Get(1); bytes.Compare(value, smallValue) != 0 {
		t.Error("Could not get value.")
	}
	if err := A.Remove(1); err != nil {
		t.Error("Could not remove value.")
	}
	bigValue := make([]byte, 2048)
	if err := A.Set(2, bigValue); err == nil {
		t.Error("Added too large value.")
	}
	if len(A.String()) == 0 {
		t.Error("Incorrect string output.")
	}
}

func Test_DifferentSizeSparseTables(t *testing.T) {
	A := Init(5, 2)
	A.Set(0, []byte("First is a long string!"))
	A.Set(1, []byte("Second"))
	A.Set(2, []byte("Third"))
	A.Set(3, []byte("Fourth"))
	A.Set(4, []byte("Fifth"))
	A.String()
	B := Init(6, 2)
	B.Set(0, []byte("First is a long string!"))
	B.Set(1, []byte("Second"))
	B.Set(2, []byte("Third"))
	B.Set(3, []byte("Fourth"))
	B.Set(4, []byte("Fifth"))
	B.Set(5, []byte("Sixth"))
	B.String()
}

func ExampleInOrderSparseTable() {
	A := Init(6, 2)
	A.Set(0, []byte("First is a long string!"))
	A.Set(1, []byte("Second"))
	A.Set(2, []byte("Third"))
	A.Set(3, []byte("Fourth"))
	A.Set(4, []byte("Fifth"))
	A.Set(5, []byte("Sixth"))
	A.Remove(3)
	fmt.Print(A.String())
	// Output:
	// SparseTable Count:5 Size: 6
	// Groups: [29,5,10]
	// 0: "First is a long string!"
	// 1: "Second"
	// 2: "Third"
	// 4: "Fifth"
	// 5: "Sixth"
}

func ExampleRandomOrderSparseTable() {
	A := Init(4, 48)
	A.Set(3, []byte("Fourth"))
	A.Set(1, []byte("Second"))
	A.Set(2, []byte("Third"))
	A.Set(0, []byte("First is a long string!"))
	fmt.Print(A.String())
	// Output:
	// SparseTable Count:4 Size: 4
	// Groups: [40]
	// 0: "First is a long string!"
	// 1: "Second"
	// 2: "Third"
	// 3: "Fourth"
}
