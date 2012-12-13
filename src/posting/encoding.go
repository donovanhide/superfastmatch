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
		existing: nil,
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

func removeDocid(docid uint32, in []byte, out []byte) ([]byte, bool) {
	if len(in) == 0 {
		// fmt.Println("Empty:", docid, in, out[:0])
		return out[:0], false
	}
	length := len(in)
	previous, current := uint32(0), uint32(0)
	out = out[:length]
	for pos := 0; pos < length; {
		delta, currentPos := readUvarint32(in, pos)
		current += delta
		if current == docid {
			pos = copy(out, in[:pos])
			v, nextPos := readUvarint32(in, currentPos)
			next := current + v
			if next > current {
				pos = putUvarint32(out, pos, next-previous)
				pos += copy(out[pos:], in[nextPos:])
			}
			// fmt.Println("Found:", docid, in, out[:pos])
			return out[:pos], true
		}
		previous = current
		pos = currentPos
	}
	copy(out, in)
	// fmt.Println("Not Found:", docid, in, out)
	return out, false
}

func insertDocid(docid uint32, in []byte, out []byte) []byte {
	if len(in) >= maxSize {
		return out[:0]
	}
	out = out[:maxSize]
	previous, current := uint32(0), uint32(0)
	length := len(in)
	for pos := 0; pos < length; {
		delta, currentPos := readUvarint32(in, pos)
		current += delta
		switch {
		case current > docid:
			pos = copy(out, in[:pos])
			_, nextPos := readUvarint32(in, pos)
			pos = putUvarint32(out, pos, docid-previous)
			pos = putUvarint32(out, pos, current-docid)
			pos += copy(out[pos:], in[nextPos:])
			// fmt.Println("Insert DocID:", docid, in, out[:pos])
			return out[:pos]
		case current == docid:
			// fmt.Println("Exists DocID:", docid, in, out[:0])
			return out[:0]
		}
		previous = current
		pos = currentPos
	}
	pos := copy(out, in)
	pos = putUvarint32(out, pos, docid-previous)
	// fmt.Println("Append DocID:", docid, in, out[:pos])
	return out[:pos]
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
	changed := true
	header.updated, changed = removeDocid(id.Docid, header.existing, header.updated)
	if !changed {
		return false
	}
	p.Length -= len(header.existing)
	p.Length += len(header.updated)
	if len(header.updated) == 0 {
		p.count--
		p.Length -= sizeUVarint32(id.Doctype) + sizeOfZero
		p.headers.MoveToBack(h)
	}
	return true
}

func (p *PostingLine) AddDocumentId(id *document.DocumentID) bool {
	if p.count >= maxHeaders {
		return false
	}
	// fmt.Println("Headers Before: ", p.String(true))
	h := p.addHeader(id.Doctype)
	h.updated = insertDocid(id.Docid, h.existing, h.updated)
	if len(h.updated) == 0 {
		return false
	}
	// fmt.Println("Headers After: ", p.String(true))
	p.Length -= len(h.existing) + sizeUVarint32(uint32(len(h.existing)))
	p.Length += len(h.updated) + sizeUVarint32(uint32(len(h.updated)))
	return true
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
