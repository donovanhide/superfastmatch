package document

import (
// "runtime"
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

func BuildAssociation(windowSize uint64, left *Document, right *Document) (*Association, ThemeMap) {
	var themes ThemeMap
	var fragments FragmentSlice
	hashKey := HashKey{
		WindowSize: windowSize - 3, // Tunable! This helps eliminate false matches
		HashWidth:  32,             // Tunable! Wider the better!
	}
	// if left.Length <= right.Length {
	pairs := left.Common(right, hashKey)
	// runtime.GC()
	fragments, themes = pairs.BuildFragments(left, int(hashKey.WindowSize), int(windowSize))
	// } else {
	// pairs := right.Common(left, hashKey)
	// fragments, themes = pairs.BuildFragments(right, int(hashKey.WindowSize), int(windowSize))
	// fragments.Flip()
	// }

	right.Text = ""
	right.Associations = nil
	return &Association{
		Document:      *right,
		Fragments:     fragments,
		FragmentCount: len(fragments),
	}, themes
}
