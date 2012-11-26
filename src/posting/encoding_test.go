package posting

import (
	"bytes"
	"document"
	. "launchpad.net/gocheck"
	"math/rand"
)

func CheckLine(c *C, line *PostingLine, buf []byte, doctype uint32, docid uint32, length int) []byte {
	line.Write(buf)
	line.AddDocumentId(&document.DocumentID{Doctype: doctype, Docid: docid})
	if line.Length != length {
		c.Log("Fail:", doctype, docid, length, line.Length, line.count, buf)
		c.Fail()
	} else {
		c.Log("Pass:", doctype, docid, length, line.Length, line.count, buf)
	}
	newBuf := make([]byte, line.Length)
	line.Read(newBuf)
	return newBuf
}

func (s *PostingSuite) TestLineLength(c *C) {
	line := NewPostingLine()
	buf := make([]byte, 0)
	c.Check(line.Length, Equals, 1)
	buf = CheckLine(c, line, buf, 2, 1, 4)
	buf = CheckLine(c, line, buf, 2, 1, 4)
	buf = CheckLine(c, line, buf, 2, 2, 5)
	buf = CheckLine(c, line, buf, 2, 3, 6)
	buf = CheckLine(c, line, buf, 2, 1999, 8)
	buf = CheckLine(c, line, buf, 1, 45, 11)
	buf = CheckLine(c, line, buf, 1, 32, 12)
	buf = CheckLine(c, line, buf, 1, 999, 14)
	buf = CheckLine(c, line, buf, 3, 999, 18)
	buf = CheckLine(c, line, buf, 3, 300000000, 23)
	buf = CheckLine(c, line, buf, 3, 1, 24)
	// This should never happen!
	buf = CheckLine(c, line, buf, 3, 0, 25)
}

func (s *PostingSuite) TestLineLengthSpecficExample(c *C) {
	line := NewPostingLine()
	buf := make([]byte, 0)
	c.Check(line.Length, Equals, 1)
	buf = CheckLine(c, line, buf, 8, 105, 4)
	buf = CheckLine(c, line, buf, 59, 12, 7)
	buf = CheckLine(c, line, buf, 53, 55, 10)
	buf = CheckLine(c, line, buf, 81, 334, 14)
	buf = CheckLine(c, line, buf, 94, 194, 18)
	buf = CheckLine(c, line, buf, 5, 114, 21)
	buf = CheckLine(c, line, buf, 94, 266, 22)
}

func (s *PostingSuite) TestInsertRemoveDocid(c *C) {
	maxLine := 255
	docids := make(UInt32Set)
	deltas := make([]byte, 0, maxLine)
	buf := make([]byte, 0, maxLine)
	for i := 0; i <= 10000; i++ {
		docid := rand.Uint32()%1000 + 1
		buf = insertDocid(docid, deltas, buf)
		if len(buf) > 0 {
			deltas = deltas[:len(buf)]
			copy(deltas, buf)
			c.Assert(docids.Add(docid), Equals, true)
		}
		// c.Assert(len(buf) > 0, Equals, docids.Add(docid))
		if len(deltas) >= maxLine {
			break
		}
		c.Check(decodeDocids(deltas), DeepEquals, SortedKeys(docids))
	}
	changed := true
	for i := 0; i <= 10000; i++ {
		docid := rand.Uint32()%1000 + 1
		buf, changed = removeDocid(docid, deltas, buf)
		if changed {
			deltas = deltas[:len(buf)]
			copy(deltas, buf)
			c.Assert(docids.Remove(docid), Equals, true)
		}
		c.Check(decodeDocids(deltas), DeepEquals, SortedKeys(docids))
	}
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
