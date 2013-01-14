package document

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"strings"
	"unicode"
)

const maxWindowSize = 256
const base uint64 = 37

var bases = make([][]uint64, maxWindowSize+1)
var words []string
var whiteSpaceHashes = make(map[HashKey]uint64, maxWindowSize+1)

type StreamFunc func(i int, h uint64)
type HasherFunc func(text string, length uint64, key HashKey, f StreamFunc)

func init() {
	bases[0] = []uint64{}
	bases[1] = []uint64{base}
	for i := 2; i <= maxWindowSize; i++ {
		bases[i] = make([]uint64, i)
		bases[i][i-1] = 1
		for j := i - 2; ; j-- {
			bases[i][j] = bases[i][j+1] * base
			if j == 0 {
				break
			}
		}
	}
}

func whiteSpaceHash(hashKey HashKey) uint64 {
	hash, ok := whiteSpaceHashes[hashKey]
	if ok {
		return hash
	}
	f := func(i int, h uint64) {
		hash = h
	}
	rollingRabinKarp3(strings.Repeat(" ", int(hashKey.WindowSize)), hashKey.WindowSize, hashKey, f)
	whiteSpaceHashes[hashKey] = hash
	return hash
}

func mix(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func rollingRabinKarp(text string, length uint64, key HashKey, f StreamFunc) {
	const base uint64 = 37
	windowSize := key.WindowSize
	hashWidth := key.HashWidth
	hashMask := uint64(1<<hashWidth) - 1
	bases := make([]uint64, windowSize)
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
	f(0, ((hash>>hashWidth)^hash)&hashMask)
	for i := 1; i < int(length); i++ {
		r, _, _ := reader.ReadRune()
		hash -= previous[previousMarker] * high
		previous[previousMarker] = uint64(r)
		previousMarker = (previousMarker + 1) % windowSize
		hash *= base
		hash += uint64(r)
		f(i, ((hash>>hashWidth)^hash)&hashMask)
	}
}

func initialHash(text string, windowSize uint64) (high, hash uint64, offset int, previous []uint64) {
	previous = make([]uint64, windowSize)
	b, j := bases[windowSize], 0
	high = b[0]
	for i, r := range text {
		if j == int(windowSize) {
			offset = i
			break
		}
		previous[j] = uint64(r)
		hash += uint64(r) * b[j]
		j++

	}
	return
}

func buildHashes(text string, length, windowSize, hashWidth, high, hash uint64, previous []uint64, f StreamFunc) {
	prev, hashMask := 0, uint64(1<<hashWidth)-1
	f(0, ((hash>>hashWidth)^hash)&hashMask)
	i, limit := 1, len(previous)
	for _, r := range text {
		p, u := &previous[prev], uint64(r)
		hash = (hash-(*p*high))*base + u
		prev++
		if prev == limit {
			prev = 0
		}
		*p = u
		f(i, ((hash>>hashWidth)^hash)&hashMask)
		i++
	}
}

func rollingRabinKarp3(text string, length uint64, key HashKey, f StreamFunc) {
	high, hash, offset, previous := initialHash(text, key.WindowSize)
	buildHashes(text[offset:], length, key.WindowSize, key.HashWidth, high, hash, previous, f)
}

const whiteSpace = rune(' ')

func normaliseRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
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

func RandomWords(maxLength int) string {
	if len(words) == 0 {
		content, err := ioutil.ReadFile("/usr/share/dict/words")
		if err != nil {
			panic("Words not available")
		}
		words = strings.Split(string(content), "\n")
	}
	text := new(bytes.Buffer)
	for i := 0; i < maxLength; i++ {
		text.WriteString(words[rand.Intn(len(words))] + " ")
	}
	return text.String()
}

func NewTestDocument(id *DocumentID, maxLength int) (*Document, error) {
	title := RandomWords(rand.Intn(5) + 5)
	text := RandomWords(rand.Intn(maxLength) + 100)
	return BuildDocument(id.Doctype, id.Docid, title, text, nil)
}
