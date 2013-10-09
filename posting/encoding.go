package posting

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/donovanhide/superfastmatch/document"
	"io"
)

const maxHeaders = 255
const maxDeltas = 255
const maxSize = 255
const sizeOfZero = 1

type Header struct {
	Doctype  uint32
	existing []byte
	updated  []byte
	buf      []uint32
}

type HeaderList struct {
	list.List
}

type PostingLine struct {
	Length  int
	count   uint32
	headers HeaderList
}

func sizeUVarint32(value uint32) int {
	switch {
	case value < 0x80:
		return 1
	case value < 0x4000:
		return 2
	case value < 0x200000:
		return 3
	case value < 0x10000000:
		return 4
	}
	return 5
}

// Assumes stream is correctly encoded
func readUvarint32(buf []byte, pos int) (uint32, int) {
	var x uint64
	var s uint
	for i, b := range buf[pos:] {
		if b < 0x80 {
			return uint32(x | uint64(b)<<s), pos + i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, pos + 1
}

func putUvarint32(buf []byte, pos int, value uint32) int {
	for value >= 0x80 {
		buf[pos] = byte(value) | 0x80
		value >>= 7
		pos++
	}
	buf[pos] = byte(value)
	return pos + 1
}

func (h *Header) write(b []byte) int {
	pos := 0
	h.Doctype, pos = readUvarint32(b, pos)
	length, pos := readUvarint32(b, pos)
	h.existing = b[pos : pos+int(length)]
	h.updated = h.updated[:0]
	return pos + int(length)
}

func (h *Header) read(b []byte) int {
	deltas := h.existing
	if len(h.updated) > 0 {
		deltas = h.updated
	}
	prelude := sizeUVarint32(h.Doctype) + sizeUVarint32(uint32(len(deltas)))
	copy(b[prelude:prelude+len(deltas)], deltas)
	pos := putUvarint32(b, 0, h.Doctype)
	pos = putUvarint32(b, pos, uint32(len(deltas)))
	return pos + len(deltas)
}

func newHeader() *Header {
	return &Header{
		existing: make([]byte, 0, maxSize),
		updated:  make([]byte, 0, maxSize),
		buf:      make([]uint32, maxDeltas),
	}
}

func (h *Header) String() string {
	return fmt.Sprintf(" E: %v U: %v ", h.existing, h.updated)
}

func (h *Header) Docids() []uint32 {
	deltas := h.existing
	if len(h.updated) > 0 { //Get rid!!
		deltas = h.updated
	}
	current, delta, i := uint32(0), uint32(0), 0
	for pos, length := 0, len(deltas); pos < length; i++ {
		delta, pos = readUvarint32(deltas, pos)
		current += delta
		h.buf[i] = current
	}
	return h.buf[:i]
}

func (h *Header) Deltas() []uint32 {
	deltas := h.existing
	if len(h.updated) > 0 { //Get rid!!
		deltas = h.updated
	}
	i := 0
	for pos, length := 0, len(deltas); pos < length; i++ {
		h.buf[i], pos = readUvarint32(deltas, pos)
	}
	return h.buf[:i]
}

// Returns difference in length,count and true if a change has occurred
func (h *Header) removeDocid(docid uint32) (int, int, bool) {
	if len(h.existing) == 0 {
		return 0, 0, false
	}
	previous, current, length := uint32(0), uint32(0), len(h.existing)
	h.updated = h.updated[:length]
	for pos := 0; pos < length; {
		delta, currentPos := readUvarint32(h.existing, pos)
		current += delta
		if current == docid {
			pos = copy(h.updated, h.existing[:pos])
			v, nextPos := readUvarint32(h.existing, currentPos)
			next := current + v
			if next > current {
				pos = putUvarint32(h.updated, pos, next-previous)
				pos += copy(h.updated[pos:], h.existing[nextPos:])
			}
			h.updated = h.updated[:pos]
			return len(h.updated) - len(h.existing), len(h.updated), true
		}
		previous = current
		pos = currentPos
	}
	h.updated = h.updated[:0]
	return 0, len(h.existing), false
}

// Returns difference in length and true if a change has occurred
func (h *Header) insertDocid(docid uint32) (int, bool) {
	if len(h.existing) >= maxSize {
		return 0, false
	}
	existing := len(h.existing) + sizeUVarint32(uint32(len(h.existing)))
	h.updated = h.updated[:maxSize]
	previous, current, length := uint32(0), uint32(0), len(h.existing)
	for pos := 0; pos < length; {
		delta, currentPos := readUvarint32(h.existing, pos)
		current += delta
		switch {
		case current > docid:
			pos = copy(h.updated, h.existing[:pos])
			_, nextPos := readUvarint32(h.existing, pos)
			pos = putUvarint32(h.updated, pos, docid-previous)
			pos = putUvarint32(h.updated, pos, current-docid)
			pos += copy(h.updated[pos:], h.existing[nextPos:])
			h.updated = h.updated[:pos]
			return len(h.updated) + sizeUVarint32(uint32(len(h.updated))) - existing, true
		case current == docid:
			h.updated = h.updated[:0]
			return 0, false
		}
		previous = current
		pos = currentPos
	}
	pos := copy(h.updated, h.existing)
	pos = putUvarint32(h.updated, pos, docid-previous)
	h.updated = h.updated[:pos]
	return len(h.updated) + sizeUVarint32(uint32(len(h.updated))) - existing, true
}

func (l *HeaderList) get(limit uint32, doctype uint32) *list.Element {
	for i, h := uint32(0), l.Front(); i < limit; h = h.Next() {
		i++
		if h.Value.(*Header).Doctype == doctype {
			return h
		}
	}
	return nil
}

// returns header and true if appended or inserted
func (l *HeaderList) add(limit uint32, doctype uint32) (*Header, bool) {
	h := l.Front()
	header := h.Value.(*Header)
outer:
	for i := uint32(0); i < limit; i++ {
		switch {
		case header.Doctype == doctype:
			return header, false
		case header.Doctype > doctype:
			header = l.Remove(l.Back()).(*Header)
			l.InsertBefore(header, h)
			break outer //goto evil?
		}
		h = h.Next()
		header = h.Value.(*Header)
	}
	header.Doctype = doctype
	header.existing = nil
	return header, true
}

func (p *PostingLine) RemoveDocumentId(id *document.DocumentID) bool {
	if p.count == 0 {
		return false
	}
	h := p.headers.get(p.count, id.Doctype)
	if h == nil {
		return false
	}
	header := h.Value.(*Header)
	diff, count, changed := header.removeDocid(id.Docid)
	p.Length += diff
	if count == 0 {
		p.count--
		p.Length -= sizeUVarint32(id.Doctype) + sizeOfZero
		p.headers.MoveToBack(h)
	}
	return changed
}

func (p *PostingLine) AddDocumentId(id *document.DocumentID) bool {
	if p.count >= maxHeaders {
		return false
	}
	h, added := p.headers.add(p.count, id.Doctype)
	if added {
		p.Length += sizeUVarint32(id.Doctype) + sizeOfZero
		p.count++
	}
	diff, changed := h.insertDocid(id.Docid)
	p.Length += diff
	return changed
}

func NewPostingLine() *PostingLine {
	p := PostingLine{
		Length: 1,
	}
	p.headers.Init()
	for i := 0; i < maxHeaders; i++ {
		p.headers.PushBack(newHeader())
	}
	return &p
}

func (p *PostingLine) Write(b []byte) (int, error) {
	if len(b) == 0 {
		p.Length = 1
		p.count = 0
		return 0, nil
	}
	pos := 0
	p.count, pos = readUvarint32(b, pos)
	if p.count >= maxHeaders {
		panic(fmt.Sprint("Too many headers:", p.count, p.Length))
	}
	for i, h := uint32(0), p.headers.Front(); i < p.count; h = h.Next() {
		i++
		pos += h.Value.(*Header).write(b[pos:])
	}
	if pos < len(b) {
		panic("Not enough written")
	}
	p.Length = pos
	return pos, nil
}

func (p *PostingLine) Read(b []byte) (int, error) {
	if len(b) != p.Length {
		panic(fmt.Sprint("Buffer wrong size: ", len(b), p.Length))
	}
	pos := putUvarint32(b, 0, p.count)
	for i, h := uint32(0), p.headers.Front(); i < p.count; h = h.Next() {
		i++
		pos += h.Value.(*Header).read(b[pos:])
	}
	return pos, io.EOF
}

func (p *PostingLine) FillMap(m *document.SearchMap, pos uint32) {
	if p.count == 0 {
		return
	}
	i := uint32(0)
	for h := p.headers.Front(); i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		doctype := header.Doctype
		for _, docid := range header.Docids() {
			id := document.DocumentID{Doctype: doctype, Docid: docid}
			if tally, ok := (*m)[id]; ok {
				delta := uint64(pos - tally.Last)
				tally.SumDeltas += delta
				tally.SumSquareDeltas += delta * delta
				tally.Last = pos
				tally.Count++
			} else {
				(*m)[id] = &document.Tally{SumDeltas: 0, SumSquareDeltas: 0, Last: pos, Count: 1}
			}
		}
	}
}

func (p *PostingLine) String(debug bool) string {
	buf := new(bytes.Buffer)
	i := uint32(0)
	for h := p.headers.Front(); i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		deltas := header.Deltas()
		buf.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v", header.Doctype, len(deltas), deltas))
		buf.WriteString(fmt.Sprintf("Docids:%v", header.Docids()))
		if debug {
			buf.WriteString(header.String())
		}
		buf.WriteString("\n")
	}
	return buf.String()
}
