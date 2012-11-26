package posting

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
)

// Sortable uint32 slice
type UIntSlice []uint32

func (p UIntSlice) Len() int           { return len(p) }
func (p UIntSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p UIntSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func SortedKeys(i interface{}) []uint32 {
	mk := reflect.ValueOf(i).MapKeys()
	keys := make(UIntSlice, len(mk))
	for i, k := range mk {
		keys[i] = uint32(k.Uint())
	}
	sort.Sort(keys)
	return keys
}

// Mock up a set of uint32
type UInt32Set map[uint32]interface{}

// Return true if v did not exist in set
func (s UInt32Set) Add(v uint32) bool {
	if _, ok := s[v]; !ok {
		s[v] = struct{}{}
		return true
	}
	return false
}

// Return true if v did exist in set
func (s UInt32Set) Remove(v uint32) bool {
	if _, ok := s[v]; ok {
		delete(s, v)
		return true
	}
	return false
}

// Mock up a posting line
type fakePostings map[uint32]UInt32Set

func (f fakePostings) String() string {
	b := new(bytes.Buffer)
	doctypes := SortedKeys(f)
	for _, doctype := range doctypes {
		docids := SortedKeys(f[doctype])
		b.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v Docids:%v\n", doctype, len(docids), encodeDeltas(docids), docids))
	}
	return b.String()
}

func (f fakePostings) Add(doctype uint32, docid uint32) {
	if _, ok := f[doctype]; !ok {
		f[doctype] = make(UInt32Set)
	}
	f[doctype][docid] = struct{}{}
}

func (f fakePostings) Remove(doctype uint32, docid uint32) {
	if docids, ok := f[doctype]; ok {
		delete(docids, docid)
		if len(docids) == 0 {
			delete(f, doctype)
		}
	}
}
