package posting

import (
	"document"
	"expvar"
	"math/rand"
	"sparsetable"
)

type Posting struct {
	offset    uint64
	documents uint64
	calls     uint64
	table     *sparsetable.SparseTable
}

func Init(size uint64, groupSize uint64, offset uint64) *Posting {
	table := sparsetable.Init(size, groupSize)
	posting := Posting{
		table:  table,
		offset: offset,
	}
	expvar.Publish("table", expvar.Func(func() interface{} { return table.Stats() }))
	expvar.Publish("posting", expvar.Func(func() interface{} { return posting.Stats() }))
	return &posting
}

func (p *Posting) Add(doc *document.Document, reply *bool) error {
	p.documents++
	p.calls++
	return nil
}

func (p *Posting) Delete(doc *document.Document, reply *bool) error {
	p.documents++
	p.calls++
	return nil
}

func (p *Posting) Search(doc *document.Document, result *document.Result) error {
	p.calls++
	return nil
}

func (p *Posting) Stress(count int, reply *bool) error {
	p.calls++
	const length int32 = 255
	width := p.table.Size()
	R := make([]byte, length)
	for i := 0; i < count; i++ {
		p.table.Set(uint64(rand.Int63n(int64(width))), R[:rand.Int31n(length)])
	}
	return nil
}

func (p *Posting) Stats() interface{} {
	return map[string]uint64{
		"offset":    p.offset,
		"documents": p.documents,
		"calls":     p.calls,
	}
}
