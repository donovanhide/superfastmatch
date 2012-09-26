package document

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"strings"
	"unicode"
	"unicode/utf8"
)

type HasherFunc func(text string, length uint64, key HashKey) []uint64

func mix(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func rollingRabinKarp(text string, length uint64, key HashKey) []uint64 {
	const base uint64 = 37
	windowSize := key.WindowSize
	hashWidth := key.HashWidth
	hashMask := uint64(1<<hashWidth) - 1
	bases := make([]uint64, windowSize)
	hashes := make([]uint64, length)
	previous := make([]uint64, windowSize)
	previousMarker := uint64(0)
	reader := strings.NewReader(text)
	hash := uint64(0)
	bases[windowSize-1] = 1
	for i := windowSize - 2; ; i-- {
		bases[i] = bases[i+1] * base
		if i == 0 {
			break
		}
	}
	high := bases[0]
	for i := uint64(0); i < windowSize; i++ {
		r, _, _ := reader.ReadRune()
		previous[i] = uint64(r)
		hash += uint64(r) * bases[i]
	}
	hashes[0] = (hash >> hashWidth) ^ (hash & hashMask)
	for i := uint64(1); i < length; i++ {
		r, _, _ := reader.ReadRune()
		hash -= previous[previousMarker] * high
		previous[previousMarker] = uint64(r)
		previousMarker = (previousMarker + 1) % windowSize
		hash *= base
		hash += uint64(r)
		hashes[i] = (hash >> hashWidth) ^ (hash & hashMask)
	}
	return hashes
}

func rollingRabinKarp2(text string, windowSize int, count int) []uint32 {
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

const whiteSpace = rune(' ')

func normaliseRune(r rune) rune {
	switch {
	case unicode.IsLetter(r) || unicode.IsDigit(r):
		return unicode.ToUpper(r)
	}
	return whiteSpace
}

func BuildTestCorpus(maxDoctype uint32, maxDocid uint32, maxLength int) chan *Document {
	docs := make(chan *Document, 100)
	go func() {
		for i := uint32(1); i <= maxDoctype; i++ {
			for j := uint32(1); j <= maxDocid; j++ {
				id := &DocumentID{
					Doctype: i,
					Docid:   j,
				}
				doc, err := NewTestDocument(id, maxLength)
				if err == nil {
					docs <- doc
				}
			}
		}
		docs <- nil
	}()
	return docs
}

func NewTestDocument(id *DocumentID, maxLength int) (*Document, error) {
	if len(words) == 0 {
		content, err := ioutil.ReadFile("/usr/share/dict/words")
		if err != nil {
			return nil, err
		}
		words = strings.Split(string(content), "\n")
	}
	title := new(bytes.Buffer)
	text := new(bytes.Buffer)
	titleLength := rand.Intn(5) + 5
	textLength := rand.Intn(maxLength) + 100
	for i := 0; i < titleLength; i++ {
		title.WriteString(words[rand.Intn(len(words))] + " ")
	}
	for i := 0; i < textLength; i++ {
		text.WriteString(words[rand.Intn(len(words))] + " ")
	}
	return &Document{
		Id:     *id,
		Title:  title.String(),
		Text:   text.String(),
		Length: uint64(utf8.RuneCountInString(text.String())),
	}, nil
}
