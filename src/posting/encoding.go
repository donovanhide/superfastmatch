package posting

import (
	"bytes"
	"document"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type Header struct {
	Doctype uint64
	Length  uint64
}

type Block struct {
	Docids []uint64
}

type PostingLine struct {
	Length  int
	Count   uint64
	Headers []Header
	Blocks  []Block
}

func searchDocids(a []uint64, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

func searchHeaders(a []Header, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Doctype >= x })
}

func (b *Block) addDocid(docid uint64) uint64 {
	i := searchDocids(b.Docids, docid)
	if i == len(b.Docids) || b.Docids[i] > docid {
		b.Docids = append(b.Docids[:i], append([]uint64{docid}, b.Docids[i:]...)...)
		return 1
	}
	return 0
}

func (p *PostingLine) addHeader(doctype uint64) int {
	i := searchHeaders(p.Headers, doctype)
	if i == len(p.Headers) || p.Headers[i].Doctype > doctype {
		p.Headers = append(p.Headers[:i], append([]Header{Header{Doctype: doctype, Length: 0}}, p.Headers[i:]...)...)
		p.Blocks = append(p.Blocks[:i], append([]Block{Block{Docids: []uint64{}}}, p.Blocks[i:]...)...)
		p.Count++
	}
	return i
}

func (p *PostingLine) AddDocumentId(id *document.DocumentID) {
	doctype := uint64(id.Doctype)
	docid := uint64(id.Docid)
	i := p.addHeader(doctype)
	p.Headers[i].Length += p.Blocks[i].addDocid(docid)
}

func ReadPostingLine(r *bytes.Reader) (p *PostingLine, err error) {
	p = new(PostingLine)
	p.Length = r.Len()
	p.Count, err = binary.ReadUvarint(r)
	if err == io.EOF {
		p.Count = 0
		err = nil
		return
	}
	p.Headers = make([]Header, p.Count)
	p.Blocks = make([]Block, p.Count)
	for i := uint64(0); i < p.Count; i++ {
		p.Headers[i].Doctype, err = binary.ReadUvarint(r)
		p.Headers[i].Length, err = binary.ReadUvarint(r)
		p.Blocks[i].Docids = make([]uint64, p.Headers[i].Length)
	}
	for i := uint64(0); i < p.Count; i++ {
		block := &p.Blocks[i]
		previous := uint64(0)
		for j, length := uint64(0), p.Headers[i].Length; j < length; j++ {
			current, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			block.Docids[j] = previous + current
			previous += current
		}
	}
	return p, nil
}

func writeUvarint(w io.Writer, buf []byte, value uint64) int {
	l := binary.PutUvarint(buf, value)
	w.Write(buf[:l])
	return l
}

func (p *PostingLine) Write() []byte {
	length := 0
	buf := new(bytes.Buffer)
	b := make([]byte, binary.MaxVarintLen32)
	length += writeUvarint(buf, b, p.Count)
	for i := uint64(0); i < p.Count; i++ {
		h := &p.Headers[i]
		length += writeUvarint(buf, b, h.Doctype)
		length += writeUvarint(buf, b, h.Length)
	}
	for i := uint64(0); i < p.Count; i++ {
		block := &p.Blocks[i]
		previous := uint64(0)
		for j, k := uint64(0), p.Headers[i].Length; j < k; j++ {
			current := block.Docids[j]
			length += writeUvarint(buf, b, current-previous)
			previous = current
		}
	}
	return buf.Bytes()
}

func (b *Block) Deltas() []uint64 {
	deltas := make([]uint64, len(b.Docids))
	previous := uint64(0)
	for i, d := range b.Docids {
		deltas[i] = d - previous
		previous = d
	}
	return deltas
}

func (p *PostingLine) String() string {
	buf := new(bytes.Buffer)
	for i, h := range p.Headers {
		buf.WriteString(fmt.Sprintf("Doctype: %v Length: %v Deltas: %v Docids:%v\n", h.Doctype, h.Length, p.Blocks[i].Deltas(), p.Blocks[i].Docids))
	}
	return buf.String()
}
