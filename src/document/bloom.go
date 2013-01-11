package document

import (
	"fmt"
	"math"
)

type Bloom interface {
	Intersection(other Bloom) Bloom
	Set(hash uint64)
	Test(hash uint64) bool
	String() string
	Count() (x uint64)
	Size() (x uint64)
}

const log64 = uint(6)
const mask64 = uint64((1 << log64) - 1)

type bloom struct {
	bits []uint64
	size uint64
}

type DynamicBloom struct {
	bloom
}

type FixedBloom struct {
	bloom
	mask uint64
}

func NewDynamicBloom(n uint64, p float64) Bloom {
	size := uint64(estimateSize(n, p))
	return &DynamicBloom{
		bloom: bloom{
			bits: make([]uint64, (size>>log64)+1),
			size: size,
		},
	}
}

func (b *DynamicBloom) Set(hash uint64) {
	i := hash % b.size
	b.bits[i>>log64] |= 1 << (i & mask64)
}

func (b *DynamicBloom) Test(hash uint64) bool {
	i := hash % b.size
	return (b.bits[i>>log64] & (1 << (i & mask64))) != 0
}

func NewFixedBloom(n uint64, p float64) Bloom {
	bits := numBits(estimateSize(n, p))
	size := uint64(1) << bits
	return &FixedBloom{
		bloom: bloom{
			bits: make([]uint64, size>>log64+1),
			size: size,
		},
		mask: size - 1,
	}
}

func (b *DynamicBloom) Intersection(other Bloom) Bloom {
	panic("not implemented")
}

func (b *FixedBloom) Intersection(a Bloom) Bloom {
	other, ok := a.(*FixedBloom)
	if !ok {
		panic("Not a FixedBloom")
	}
	if b.size != other.size {
		panic("Wrong size blooms intersected")
	}
	inter := &FixedBloom{
		bloom: bloom{
			bits: make([]uint64, len(b.bits)),
			size: b.size,
		},
		mask: b.mask,
	}
	for i := range b.bits {
		inter.bits[i] = b.bits[i] & other.bits[i]
	}
	return inter
}

func (b *FixedBloom) Set(hash uint64) {
	i := hash & b.mask
	b.bits[i>>log64] |= 1 << (i & mask64)
}

func (b *FixedBloom) Test(hash uint64) bool {
	i := hash & b.mask
	return (b.bits[i>>log64] & (1 << (i & mask64))) != 0
}

func (b *bloom) Size() uint64 {
	return b.size
}

func (b *bloom) String() string {
	return fmt.Sprintf("Count: %v Size: %v Occupied: %.2f%%", b.Count(), b.Size(), float64(b.Count())/float64(b.Size())*100)
}

func (b *bloom) Count() (x uint64) {
	for _, v := range b.bits {
		// fmt.Printf("%064b\n", v)
		x += popcount(v)
	}
	return
}

func numBits(x float64) uint64 {
	return uint64(math.Ceil(math.Log2(x)))
}

func estimateSize(n uint64, p float64) float64 {
	return -((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2))
}

// from http://en.wikipedia.org/wiki/Hamming_weight
const m1 = uint64(0x5555555555555555)
const m2 = uint64(0x3333333333333333)
const m4 = uint64(0x0f0f0f0f0f0f0f0f)
const h1 = uint64(0x0101010101010101)

func popcount(x uint64) uint64 {
	x -= (x >> 1) & m1
	x = (x & m2) + ((x >> 2) & m2)
	x = (x + (x >> 4)) & m4
	return (x * h1) >> 56
}
