package posting

import (
	"bytes"
	"document"
	"fmt"
	"io"
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

func (f fakePostings) Add(doctype uint32, docid uint32) {
	if _, ok := f[doctype]; !ok {
		f[doctype] = make(map[uint32]struct{})
	}
	f[doctype][docid] = struct{}{}
}

func CheckLine(c *C, line *PostingLine, doctype uint32, docid uint32, length int) {
	line.AddDocumentId(&document.DocumentID{Doctype: doctype, Docid: docid})
	buf := make([]byte, line.Length)
	line.Read(buf)
	if line.Length != length {
		c.Log("Fail:", doctype, docid, length, line.Length, line.count, buf)
		c.Fail()
	} else {
		c.Log("Pass:", doctype, docid, length, line.Length, line.count, buf)
	}
}

func (s *PostingSuite) TestLineLength(c *C) {
	line := NewPostingLine()
	c.Check(line.Length, Equals, 1)
	CheckLine(c, line, 2, 1, 4)
	CheckLine(c, line, 2, 1, 4)
	CheckLine(c, line, 2, 2, 5)
	CheckLine(c, line, 2, 3, 6)
	CheckLine(c, line, 2, 1999, 8)
	CheckLine(c, line, 1, 45, 11)
	CheckLine(c, line, 1, 32, 12)
	CheckLine(c, line, 1, 999, 14)
	CheckLine(c, line, 3, 999, 18)
	CheckLine(c, line, 3, 300000000, 23)
	CheckLine(c, line, 3, 1, 24)
	// This should never happen!
	CheckLine(c, line, 3, 0, 25)
}

func (s *PostingSuite) TestLineLengthSpecficExample(c *C) {
	line := NewPostingLine()
	c.Check(line.Length, Equals, 1)
	CheckLine(c, line, 8, 105, 4)
	CheckLine(c, line, 59, 12, 4)
	CheckLine(c, line, 53, 55, 5)
	CheckLine(c, line, 81, 334, 6)
	CheckLine(c, line, 94, 194, 8)
	CheckLine(c, line, 5, 114, 8)
	CheckLine(c, line, 94, 266, 11)
}

func (s *PostingSuite) TestInsertDocid(c *C) {
	docids := make(map[uint32]struct{})
	deltas := make([]byte, 0, 255)
	buf := make([]byte, 0, 255)
	for i := 0; i <= 10000; i++ {
		docid := rand.Uint32()%1000 + 1
		buf = insertDocid(docid, deltas, buf)
		if len(buf) > 0 {
			deltas = deltas[:len(buf)]
			copy(deltas, buf)
		}
		_, ok := docids[docid]
		c.Assert(len(buf) == 0, Equals, ok)
		docids[docid] = struct{}{}
		if len(deltas) >= 255 {
			break
		}
	}
	sorted := make([]uint32, 0)
	for k, _ := range docids {
		sorted = append(sorted, k)
	}
	sort.Sort(UIntSlice(sorted))
	c.Check(decodeDocids(deltas), DeepEquals, sorted)
}

func (s *PostingSuite) TestPostingLine(c *C) {
	const docCount = 100000
	postings := make(fakePostings, docCount)
	line := NewPostingLine()
	reader := make([]byte, 0, 4096)
	writer := make([]byte, 0, 4096)
	readBuf := bytes.NewBuffer(reader)
	writeBuf := bytes.NewBuffer(writer)
	c.Check(line.Length, Equals, 1)
	for i := 0; i <= docCount; i++ {
		_, err := readBuf.WriteTo(line)
		c.Assert(err, IsNil)
		doctype := rand.Uint32()%500 + 1
		docid := rand.Uint32()%500 + 1
		if line.AddDocumentId(&document.DocumentID{Doctype: doctype, Docid: docid}) {
			postings.Add(doctype, docid)
		}
		_, err = writeBuf.ReadFrom(line)
		c.Assert(err, IsNil)
		_, err = io.Copy(readBuf, writeBuf)
		c.Assert(err, IsNil)
	}
	c.Check(line.String(), Equals, postings.String())
}

func (s *PostingSuite) BenchmarkPostingLine(c *C) {
	b := make([]byte, 0)
	buf := bytes.NewBuffer(b)
	line := NewPostingLine()
	_, err := buf.WriteTo(line)
	c.Check(err, IsNil)
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
		postings.Add(doctype, docid)
	}
}
