package sparsetable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

const width uint64 = 1 << 16
const length int32 = 255
const randLength int32 = 1 << 16
const groupSize uint64 = 128

func buildData(count int) ([]byte, [][]int) {
	R := make([]byte, randLength)
	L := make([][]int, count)
	for i := int32(0); i < randLength; i++ {
		R[i] = byte(rand.Int31n(length))
	}
	for i := 0; i < count; i++ {
		L[i] = make([]int, 3)
		L[i][0] = rand.Intn(int(width))
		L[i][1] = rand.Intn(int(randLength - length))
		L[i][2] = L[i][1] + rand.Intn(int(length))
	}
	return R, L
}

func Test_SparseTableStress(t *testing.T) {
	A := Init(width, groupSize)
	fake := make(map[uint64][]byte)
	count := 1000000
	R, L := buildData(count)
	for i := 0; i < count; i++ {
		v := &L[i]
		index := uint64((*v)[0])
		data := R[(*v)[1]:(*v)[2]]
		err := A.SetBytes(index, data)
		if len(data) > 255 && err == nil {
			t.Fatalf("Expected Value greater than 255 bytes")
		}
		if err != nil {
			t.Fatal(err)
		}
		if err == nil {
			if len(data) == 0 {
				delete(fake, index)
			} else {
				fake[index] = data
			}
		}
	}
	for i, d := range fake {
		data, err := A.GetBytes(i)
		if err != nil {
			t.Error(data, d)
		}
		for j, _ := range data {
			if data[j] != d[j] {
				t.Error(data, d)
			}
		}
	}
	c := A.Count()
	if c != uint64(len(fake)) {
		t.Error(c, len(fake))
	}
}

func BenchmarkSparseTable(b *testing.B) {
	A := Init(width, groupSize)
	R, L := buildData(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := &L[i]
		A.SetBytes(uint64((*v)[0]), R[(*v)[1]:(*v)[2]])
	}
}

func BenchmarkMap(b *testing.B) {
	A := make(map[uint64][]byte)
	R, L := buildData(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := &L[i]
		A[uint64((*v)[0])] = R[(*v)[1]:(*v)[2]]
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
