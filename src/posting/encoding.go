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

func decodeDeltas(deltas []byte) []uint32 {
	length := len(deltas)
	decoded := make([]uint32, length)
	i := 0
	for pos := 0; pos < length; i++ {
		decoded[i], pos = readUvarint32(deltas, pos)
	}
	return decoded[:i]
}

func decodeDocids(deltas []byte) []uint32 {
	length := len(deltas)
	decoded := make([]uint32, length)
	current, delta, i := uint32(0), uint32(0), 0
	for pos := 0; pos < length; i++ {
		delta, pos = readUvarint32(deltas, pos)
		current += delta
		decoded[i] = current
	}
	return decoded[:i]
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

func (p *PostingLine) AddDocumentId(id *document.DocumentID) bool {
	if p.count >= maxHeaders {
		return false
	}
	// fmt.Println("Headers Before: ", p.DumpHeaders())
	h := p.addHeader(id.Doctype)
	h.updated = insertDocid(id.Docid, h.existing, h.updated)
	if len(h.updated) == 0 {
		return false
	}
	// fmt.Println("Headers After: ", p.DumpHeaders())
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
		header := &Header{
			existing: nil,
			updated:  make([]byte, 0, maxSize),
		}
		p.headers.PushBack(header)
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
	// fmt.Println("During Read:", p.DumpHeaders())
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

func (h *Header) Docids() []uint32 {
	return decodeDocids(h.existing)
}

func (h *Header) Deltas() []uint32 {
	return decodeDeltas(h.existing)
}

func (p *PostingLine) String() string {
	buf := new(bytes.Buffer)
	i := uint32(0)
	for h := p.headers.Front(); h != nil && i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		docids := header.Docids()
		buf.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v Docids:%v\n", header.doctype, len(docids), header.Deltas(), docids))
	}
	return buf.String()
}

func (p *PostingLine) DumpHeaders() string {
	buf := new(bytes.Buffer)
	i := uint32(0)
	for h := p.headers.Front(); h != nil && i < p.count; h = h.Next() {
		i++
		header := h.Value.(*Header)
		buf.WriteString(fmt.Sprintf("Doctype: %v E: %v U: %v ", header.doctype, header.existing, header.updated))
	}
	return buf.String()
}
