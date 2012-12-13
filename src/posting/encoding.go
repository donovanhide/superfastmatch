package posting

import (
	"bytes"
	"container/list"
	"document"
	"encoding/binary"
	"fmt"
	"io"
)

const maxHeaders = 255
const maxDeltas = 255
const maxSize = 255
const sizeOfZero = 1

type Header struct {
	doctype  uint32
	existing []byte
	updated  []byte
	buf      []uint32
}

type PostingLine struct {
	Length    int
	count     uint32
	headers   list.List
	insertion []byte
}

func sizeUVarint32(value uint32) int {
	switch {
	case value < 128:
		return 1
	case value < 16384:
		return 2
	case value < 2097152:
		return 3
	case value < 268435456:
		return 4
	}
	return 5
}

func readUvarint32(buf []byte, pos int) (uint32, int) {
	value, offset := binary.Uvarint(buf[pos:])
	return uint32(value), pos + offset
}

func putUvarint32(buf []byte, pos int, value uint32) int {
	return pos + binary.PutUvarint(buf[pos:], uint64(value))
}

func newHeader() *Header {
	return &Header{
		existing: make([]byte, 0, maxSize),
		updated:  make([]byte, 0, maxSize),
		buf:      make([]uint32, maxDeltas),
	}
}

func (h *Header) Docids() []uint32 {
	current, delta, i := uint32(0), uint32(0), 0
	for pos, length := 0, len(h.existing); pos < length; i++ {
		delta, pos = readUvarint32(h.existing, pos)
		current += delta
		h.buf[i] = current
	}
	return h.buf[:i]
}

func (h *Header) Deltas() []uint32 {
	i := 0
	for pos, length := 0, len(h.existing); pos < length; i++ {
		h.buf[i], pos = readUvarint32(h.existing, pos)
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

func (p *PostingLine) getHeader(doctype uint32) *list.Element {
	h := p.headers.Front()
	header := h.Value.(*Header)
	for i := uint32(0); i < p.count; i++ {
		if header.doctype == doctype {
			return h
		}
		h = h.Next()
		header = h.Value.(*Header)
	}
	return nil
}

func (p *PostingLine) addHeader(doctype uint32) *Header {
	h := p.headers.Front()
	header := h.Value.(*Header)
	for i := uint32(0); i < p.count; i++ {
		switch {
		case header.doctype == doctype:
			// fmt.Println("Exists", doctype, p.count, p.Length, &header)
			return header
		case header.doctype > doctype:
			header = p.headers.Remove(p.headers.Back()).(*Header)
			header.doctype = doctype
			header.existing = nil
			header.updated = header.updated[:0]
			p.headers.InsertBefore(header, h)
			p.count++
			p.Length += sizeUVarint32(doctype) + sizeOfZero
			// fmt.Println("Insert", doctype, p.count, p.Length, &header, p.headers.Len())
			return header
		}
		// fmt.Println("Skip", doctype, header.doctype, header.deltas, &header)
		h = h.Next()
		header = h.Value.(*Header)
	}
	header.doctype = doctype
	header.existing = nil
	header.updated = header.updated[:0]
	p.Length += sizeUVarint32(doctype) + sizeOfZero
	p.count++
	// fmt.Println("Append", doctype, p.count, p.Length, &header)
	return header
}

func (p *PostingLine) RemoveDocumentId(id *document.DocumentID) bool {
	if p.count == 0 {
		return false
	}
	h := p.getHeader(id.Doctype)
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
	h := p.addHeader(id.Doctype)
	diff, changed := h.insertDocid(id.Docid)
	p.Length += diff
	return changed
}

func NewPostingLine() *PostingLine {
	p := PostingLine{
		insertion: make([]byte, 2*binary.MaxVarintLen32),
		Length:    1,
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
	deltasLength := uint32(0)
	header := p.headers.Front()
	for i := uint32(0); i < p.count; i++ {
		h := header.Value.(*Header)
		h.doctype, pos = readUvarint32(b, pos)
		deltasLength, pos = readUvarint32(b, pos)
		h.existing = b[pos : pos+int(deltasLength)]
		h.updated = h.updated[:0]
		// fmt.Println("Write:", &header, h.existing)
		pos += int(deltasLength)
		header = header.Next()
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
	// fmt.Println("During Read:", p.String(true))
	header := p.headers.Front()
	for i := uint32(0); i < p.count; i++ {
		h := header.Value.(*Header)
		deltas := h.existing
		if len(h.updated) > 0 {
			deltas = h.updated
		}
		pos = putUvarint32(b, pos, h.doctype)
		pos = putUvarint32(b, pos, uint32(len(deltas)))
		pos += copy(b[pos:pos+len(deltas)], deltas)
		header = header.Next()
	}
	return pos, io.EOF
}

func (p *PostingLine) FillMap(m *document.SearchMap, pos uint32) {
	if p.count == 0 {
		return
	}
	i := uint32(0)
	for h := p.headers.Front(); h != nil && i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		doctype := header.doctype
		for _, docid := range header.Docids() {
			id := document.DocumentID{Doctype: doctype, Docid: docid}
			if tally, ok := (*m)[id]; ok {
				// MAGIC NUMBER ALERT!!!!!
				if pos-tally.Last < 128 {
					tally.SumDeltas += uint64(pos - tally.Last)
					tally.Last = pos
					tally.Count++
				}
			} else {
				(*m)[id] = &document.Tally{SumDeltas: 0, Last: pos, Count: 1}
			}
		}
	}
}

func (p *PostingLine) String(debug bool) string {
	buf := new(bytes.Buffer)
	i := uint32(0)
	for h := p.headers.Front(); h != nil && i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		docids := header.Docids()
		buf.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v Docids:%v", header.doctype, len(docids), header.Deltas(), docids))
		if debug {
			buf.WriteString(fmt.Sprintf(" E: %v U: %v ", header.existing, header.updated))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}
