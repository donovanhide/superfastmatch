package document

import (
	"bytes"
	"container/heap"
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
	hashes[0] = ((hash >> hashWidth) ^ hash) & hashMask
	for i := uint64(1); i < length; i++ {
		r, _, _ := reader.ReadRune()
		hash -= previous[previousMarker] * high
		previous[previousMarker] = uint64(r)
		previousMarker = (previousMarker + 1) % windowSize
		hash *= base
		hash += uint64(r)
		hashes[i] = ((hash >> hashWidth) ^ hash) & hashMask
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
	text := RandomWords(rand.Intn(maxLength) + 100)
	return &Document{
		Id:     *id,
		Title:  RandomWords(rand.Intn(5) + 5),
		Text:   text,
		Length: uint64(utf8.RuneCountInString(text)),
	}, nil
}

// An Item is something we manage in a priority queue.
type Item struct {
	value    string // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	// The index is needed by changePriority and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	a := *pq
	n := len(a)
	item := a[n-1]
	item.index = -1 // for safety
	*pq = a[0 : n-1]
	return item
}

func (pq *PriorityQueue) update(value string, priority int) {
	item := heap.Pop(pq).(*Item)
	item.value = value
	item.priority = priority
	heap.Push(pq, item)
}

func (pq *PriorityQueue) changePriority(item *Item, priority int) {
	heap.Remove(pq, item.index)
	item.priority = priority
	heap.Push(pq, item)
}
