package sparsetable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

const width uint64 = 1 << 24
const length int32 = 255
const randLength int32 = 1 << 16
const groupSize uint64 = 48

func Test_SparseTableStress(t *testing.T) {
	A := Init(width, groupSize)
	R := make([]byte, randLength)
	fake := make(map[uint64][]byte)
	count := 1000000
	for i := int32(0); i < randLength; i++ {
		R[i] = byte(rand.Int31n(length))
	}
	for i := 0; i < count; i++ {
		start := rand.Int31n(randLength - length)
		l := rand.Int31n(length) + 1
		index := uint64(rand.Int63n(int64(width)))
		data := R[start : start+l]
		err := A.SetBytes(index, data)
		if len(data) > 255 && err == nil {
			t.Fatalf("Expected Value greater than 255 bytes")
		}
		if err == nil {
			fake[index] = data
		}
	}
	for i, d := range fake {
		data, err := A.GetBytes(i)
		if err != nil {
			fmt.Println(data, d)
			t.Fail()
		}
		for j, _ := range data {
			if data[j] != d[j] {
				fmt.Println(data, d)
				t.Fail()
			}
		}
	}
	if A.Count() != uint64(len(fake)) {
		t.Fail()
	}
}

func BenchmarkSparseTable(b *testing.B) {
	A := Init(width, groupSize)
	R := make([]byte, randLength)
	for i := int32(0); i < randLength; i++ {
		R[i] = byte(rand.Int31n(length))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := rand.Int31n(randLength - length)
		l := rand.Int31n(length)
		A.SetBytes(uint64(rand.Int63n(int64(width))), R[start:start+l])
	}
}

func BenchmarkMap(b *testing.B) {
	A := make(map[uint64][]byte)
	R := make([]byte, randLength)
	for i := int32(0); i < randLength; i++ {
		R[i] = byte(rand.Int31n(length))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := rand.Int31n(randLength - length)
		l := rand.Int31n(length)
		A[uint64(rand.Int63n(int64(width)))] = R[start : start+l]
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
	if err := A.SetBytes(1, smallValue); err != nil {
		t.Error("Could not set value.")
	}
	if A.Size() != 1024 {
		t.Error("Incorrect Size.")
	}
	if A.Count() != 1 {
		t.Error("Incorrect Count.")
	}
	if err := A.SetBytes(1025, smallValue); err == nil {
		t.Error("Out of range position accepted.")
	}
	if err := A.Remove(1025); err == nil {
		t.Error("Out of range position accepted.")
	}
	if value, _ := A.GetBytes(1); bytes.Compare(value, smallValue) != 0 {
		t.Error("Could not get value.")
	}
	if err := A.Remove(1); err != nil {
		t.Error("Could not remove value.")
	}
	bigValue := make([]byte, 2048)
	if err := A.SetBytes(2, bigValue); err == nil {
		t.Error("Added too large value.")
	}
	if len(A.String()) == 0 {
		t.Error("Incorrect string output.")
	}
}

func Test_DifferentSizeSparseTables(t *testing.T) {
	A := Init(5, 2)
	A.SetBytes(0, []byte("First is a long string!"))
	A.SetBytes(1, []byte("Second"))
	A.SetBytes(2, []byte("Third"))
	A.SetBytes(3, []byte("Fourth"))
	A.SetBytes(4, []byte("Fifth"))
	A.String()
	B := Init(6, 2)
	B.SetBytes(0, []byte("First is a long string!"))
	B.SetBytes(1, []byte("Second"))
	B.SetBytes(2, []byte("Third"))
	B.SetBytes(3, []byte("Fourth"))
	B.SetBytes(4, []byte("Fifth"))
	B.SetBytes(5, []byte("Sixth"))
	B.String()
}

func ExampleInOrderSparseTable() {
	A := Init(6, 2)
	A.SetBytes(0, []byte("First is a long string!"))
	A.SetBytes(1, []byte("Second"))
	A.SetBytes(2, []byte("Third"))
	A.SetBytes(3, []byte("Fourth"))
	A.SetBytes(4, []byte("Fifth"))
	A.SetBytes(5, []byte("Sixth"))
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
	A := Init(12, 48)
	A.SetBytes(3, []byte("Fourth"))
	A.SetBytes(1, []byte("Second"))
	A.SetBytes(2, []byte("Third"))
	A.SetBytes(0, []byte("First is a long string!"))
	A.SetBytes(5, []byte("Sixth"))
	A.SetBytes(7, []byte("Eigth"))
	fmt.Print(A.String())
	// Output:
	// SparseTable Count:6 Size: 12
	// Groups: [50]
	// 0: "First is a long string!"
	// 1: "Second"
	// 2: "Third"
	// 3: "Fourth"
	// 5: "Sixth"
	// 7: "Eigth"
}
