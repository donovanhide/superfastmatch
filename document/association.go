package document

import (
	"bytes"
	"code.google.com/p/go.exp/utf8string"
	"fmt"
	"sync"
)

type Inverted struct {
	Hash     uint32
	Position int32
}

type InvertedSlice []Inverted

type Association struct {
	Document
	Fragments     FragmentSlice `json:"fragments"`
	FragmentCount int           `json:"fragment_count"`
}

type AssociationSlice []Association

type Associations struct {
	Meta      MetaMap
	Documents []Association
}

func (s AssociationSlice) String(other string) string {
	var buf bytes.Buffer
	text := utf8string.NewString(other)
	for _, a := range s {
		for _, f := range a.Fragments {
			buf.WriteString(fmt.Sprintf("%s\t%s", a.Document.Pretty(15), f.Pretty(60, text)))
		}
	}
	return buf.String()
}

func Greater(l, r Inverted) bool {
	return l.Hash > r.Hash || (l.Hash == r.Hash && l.Position > r.Position)
}

func (s InvertedSlice) ShellSort() {
	size := len(s)
	for inc := size / 2; inc > 0; inc = (inc + 1) * 5 / 11 {
		for i := inc; i < size; i++ {
			j, temp := i, s[i]
			for ; j >= inc && Greater(s[j-inc], temp); j -= inc {
				s[j] = s[j-inc]
			}
			s[j] = temp
		}
	}
}

func (s InvertedSlice) Sort(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		// sort.Sort(&s)
		s.ShellSort()
		wg.Done()
	}()
}

func (left InvertedSlice) Merge(right InvertedSlice) *Pairs {
	buf := make(PositionSlice, 0)
	pairs := NewPairs(len(left))
	cursor := 0
	for _, l := range left {
	step:
		for _, r := range right[cursor:] {
			switch {
			case r.Hash == l.Hash:
				buf = append(buf, r.Position)
			case r.Hash < l.Hash:
				cursor++
				continue step
			case r.Hash > l.Hash:
				break step
			}
		}
		if len(buf) > 0 {
			pairs.Append(l.Position, buf)
			buf = buf[:0]
		}
	}
	pairs.Sort()
	return pairs
}

func (s InvertedSlice) String() string {
	var buf bytes.Buffer
	last := uint32(0)
	for _, v := range s {
		buf.WriteString(fmt.Sprintf("%10d:%10d:%v\n", v.Hash, v.Position, last == v.Hash))
		last = v.Hash
	}
	return buf.String()
}

func Common(doc, other *Document, hashKey HashKey) *Pairs {
	estimate := doc.Length
	bloomKey := BloomKey{
		HashKey: hashKey,
		Size:    estimate,
	}
	bloom := doc.Bloom(bloomKey).Intersection(other.Bloom(bloomKey))
	right, left := other.InvertedSlice(hashKey, bloom), doc.InvertedSlice(hashKey, bloom)
	wg := new(sync.WaitGroup)
	left.Sort(wg)
	right.Sort(wg)
	wg.Wait()
	return left.Merge(right)
}

func BuildAssociation(windowSize uint64, left *Document, right *Document) (*Association, ThemeMap) {
	var themes ThemeMap
	var fragments FragmentSlice
	hashKey := HashKey{
		WindowSize: windowSize - 3, // Tunable! This helps eliminate false matches
		HashWidth:  32,             // Tunable! Wider the better!
	}
	pairs := Common(left, right, hashKey)
	fragments, themes = pairs.BuildFragments(left, int(hashKey.WindowSize), int(windowSize))
	right.Associations = nil
	return &Association{
		Document:      *right,
		Fragments:     fragments,
		FragmentCount: len(fragments),
	}, themes
}
