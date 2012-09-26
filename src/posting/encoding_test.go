package posting

import (
	"bytes"
	"document"
	"fmt"
	. "launchpad.net/gocheck"
	"math/rand"
	"sort"
)

// Mock up a posting line
type fakePostings map[uint32]map[uint32]struct{}

type UIntSlice []uint32

func (p UIntSlice) Len() int           { return len(p) }
func (p UIntSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p UIntSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (f fakePostings) String() string {
	b := new(bytes.Buffer)
	doctypes := make(UIntSlice, len(f))
	i := 0
	for doctype, _ := range f {
		doctypes[i] = doctype
		i++
	}
	sort.Sort(doctypes)
	for _, doctype := range doctypes {
		i = 0
		docids := make(UIntSlice, len(f[doctype]))
		for docid, _ := range f[doctype] {
			docids[i] = docid
			i++
		}
		sort.Sort(docids)
		deltas := make(UIntSlice, len(docids))
		previous := uint32(0)
		for i, d := range docids {
			deltas[i] = d - previous
			previous = d
		}
		b.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v Docids:%v\n", doctype, len(docids), deltas, docids))
	}
	return b.String()
}

func (s *PostingSuite) TestPostingLine(c *C) {
	const docCount = 100000
	b := make([]byte, 0)
	r := bytes.NewReader(b)
	line, err := ReadPostingLine(r)
	c.Check(err, IsNil)
	c.Check(line.Count, Equals, uint64(0))
	postings := make(fakePostings, docCount)
	for i := 0; i <= docCount; i++ {
		doctype := rand.Uint32()%1000 + 1
		docid := rand.Uint32()%1000 + 1
		if _, ok := postings[doctype]; !ok {
			postings[doctype] = make(map[uint32]struct{})
		}
		postings[doctype][docid] = struct{}{}
		line.AddDocumentId(&document.DocumentID{Doctype: doctype, Docid: docid})
	}
	c.Check(line.String(), Equals, postings.String())
	b2 := line.Write()
	line2, err := ReadPostingLine(bytes.NewReader(b2))
	c.Check(err, IsNil)
	c.Check(line2.String(), Equals, postings.String())
}

func (s *PostingSuite) BenchmarkPostingLine(c *C) {
	b := make([]byte, 0)
	r := bytes.NewReader(b)
	line, _ := ReadPostingLine(r)
	for i := 0; i <= c.N; i++ {
		doctype := rand.Uint32()%100 + 1
		docid := rand.Uint32()%100 + 1
		line.AddDocumentId(&document.DocumentID{Doctype: doctype, Docid: docid})
	}
}

func (s *PostingSuite) BenchmarkMap(c *C) {
	postings := make(fakePostings, c.N)
	for i := 0; i <= c.N; i++ {
		doctype := rand.Uint32()%100 + 1
		docid := rand.Uint32()%100 + 1
		if _, ok := postings[doctype]; !ok {
			postings[doctype] = make(map[uint32]struct{})
		}
		postings[doctype][docid] = struct{}{}
	}
}
