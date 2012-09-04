package document

import (
	"strings"
	"unicode"
)

type HasherFunc func(text string, windowSize int, count int) []uint32

func mix(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func rollingRabinKarp(text string, windowSize int, count int) []uint32 {
	const base uint32 = 37
	bases := make([]uint32, windowSize)
	hashes := make([]uint32, count)
	previous := make([]uint32, windowSize)
	previousMarker := 0
	reader := strings.NewReader(text)
	hash := uint32(0)
	bases[windowSize-1] = 1
	for i := windowSize - 2; i >= 0; i-- {
		bases[i] = bases[i+1] * base
	}
	high := bases[0]
	for i := 0; i < windowSize; i++ {
		r, _, _ := reader.ReadRune()
		previous[i] = uint32(r)
		hash += uint32(r) * bases[i]
	}
	hashes[0] = hash
	for i := 1; i < count; i++ {
		r, _, _ := reader.ReadRune()
		hash -= previous[previousMarker] * high
		previous[previousMarker] = uint32(r)
		previousMarker = (previousMarker + 1) % windowSize
		hash *= base
		hash += uint32(r)
		hashes[i] = hash
	}
	return hashes
}

func rollingRabinKarp2(text string, windowSize int, count int) []uint32 {
	const base uint32 = 37
	bases := make([]uint32, windowSize)
	hashes := make([]uint32, count)
	front := strings.NewReader(text)
	back := strings.NewReader(text)
	hash := uint32(0)
	bases[windowSize-1] = 1
	for i := windowSize - 2; i >= 0; i-- {
		bases[i] = bases[i+1] * base
	}
	high := bases[0]
	for i := 0; i < windowSize; i++ {
		b, _, _ := back.ReadRune()
		hash += uint32(b) * bases[i]
	}
	hashes[0] = hash
	for i := 1; i < count; i++ {
		f, _, _ := front.ReadRune()
		b, _, _ := back.ReadRune()
		hash -= uint32(f) * high
		hash *= base
		hash += uint32(b)
		hashes[i] = hash
	}
	return hashes
}

const whiteSpace = rune(' ')

func normaliseRune(r rune) rune {
	switch {
	case unicode.IsLetter(r) || unicode.IsDigit(r):
		return unicode.ToUpper(r)
	}
	return whiteSpace
}
