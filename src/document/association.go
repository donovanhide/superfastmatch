package document

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
)

type Association struct {
	Document
	Fragments     FragmentSlice `json:"fragments"`
	FragmentCount int           `json:"fragment_count"`
}

type Associations struct {
	Meta      MetaMap
	Documents []Association
}

type Pair struct {
	left  int
	right PositionSet
}

type PairSlice []Pair

func (p PairSlice) String() string {
	var buf bytes.Buffer
	for _, pair := range p {
		buf.WriteString(fmt.Sprintf("%d: ", pair.left))
		for pos := range pair.right {
			buf.WriteString(fmt.Sprintf("%d,", pos))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

func (p PairSlice) BuildFragments(left, right *Document, windowSize, minLength int) (FragmentSlice, ThemeMap) {
	fragments, themes := make(FragmentSlice, 0), make(ThemeMap)
	for i, first := range p {
		for r := range first.right {
			length := r + 1
			for _, next := range p[i+1:] {
				if _, ok := next.right[length]; !ok {
					break
				}
				delete(next.right, length)
				length++
			}
			fragmentLength := length - r + windowSize - 1
			if fragmentLength >= minLength {
				fragment, theme := newFragment(left.NormalisedText(), first.left, r, fragmentLength)
				if fragment.Length >= minLength {
					fragments = append(fragments, *fragment)
					themes[theme.Id] = *theme
				}
			}
		}
	}
	sort.Sort(fragments)
	return fragments, themes
}

func BuildAssociation(windowSize uint64, left *Document, right *Document) (*Association, ThemeMap) {
	var themes ThemeMap
	var fragments FragmentSlice
	hashKey := HashKey{
		WindowSize: windowSize - 3, // Tunable! This helps eliminate false matches
		HashWidth:  32,             // Tunable! Wider the better!
	}
	pairs := left.Common(right, hashKey)
	runtime.GC()
	fragments, themes = pairs.BuildFragments(left, right, int(hashKey.WindowSize), int(windowSize))
	right.Text = ""
	right.Associations = nil
	return &Association{
		Document:      *right,
		Fragments:     fragments,
		FragmentCount: len(fragments),
	}, themes
}
