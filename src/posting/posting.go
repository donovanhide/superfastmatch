package posting

import (
	"bytes"
	"document"
	// "expvar"
	// "fmt"
	"log"
	"os"
	"query"
	"registry"
	"sparsetable"
	"time"
)

type Posting struct {
	logger      *log.Logger
	initialised bool
	hashKey     document.HashKey
	offset      uint64
	size        uint64
	documents   uint64
	registry    *registry.Registry
	table       *sparsetable.SparseTable
}

func newPosting(registry *registry.Registry, prefix string) *Posting {
	return &Posting{
		registry: registry,
		logger:   log.New(os.Stderr, prefix+":", log.LstdFlags),
	}
}

func (p *Posting) add(doc *document.Document) error {
	start := time.Now()
	count := 0
	l := NewPostingLine()
	for _, hash := range doc.Hashes(p.hashKey) {
		pos := hash - p.offset
		if pos < p.size {
			count++
			b, err := p.table.Get(pos)
			if err != nil {
				return err
			}
			if err := l.Read(bytes.NewReader(b)); err != nil {
				return err
			}
			l.AddDocumentId(&doc.Id)
			err = p.table.Set(pos, l.Write())
			if err != nil {
				return err
			}
		}
	}
	p.logger.Printf("Added Document: %v with %v hashes at %.0f hashes/sec", doc.Id.String(), count, float64(count)/time.Now().Sub(start).Seconds())
	p.documents++
	return nil
}

func (p *Posting) Init(conf *registry.PostingConfig, reply *bool) error {
	p.initialised = false
	p.table = sparsetable.Init(conf.Size, conf.GroupSize)
	p.hashKey = document.HashKey{
		HashWidth:  conf.HashWidth,
		WindowSize: conf.WindowSize,
	}
	p.offset = conf.Offset
	p.size = conf.Size
	p.logger.Printf("Initialising Posting Server with Offset: %v Size: %v %v ", p.offset, p.size, p.hashKey)
	docids, err := query.GetDocids(conf.InitialQuery, p.registry)
	if err != nil {
		return err
	}
	p.logger.Printf("Loading %v documents", len(docids))
	for _, docid := range docids {
		doc, err := document.GetDocument(&docid, p.registry)
		if err != nil {
			return err
		}
		err = p.add(doc)
		if err != nil {
			return err
		}
	}
	// expvar.Publish("table", expvar.Func(func() interface{} { return p.table.Stats() }))
	// expvar.Publish("posting", expvar.Func(func() interface{} { return p.stats() }))
	p.initialised = true
	p.logger.Println("Posting Server Initialised")
	return nil
}

func (p *Posting) Add(docid *document.DocumentID, _ *struct{}) error {
	doc, err := document.GetDocument(docid, p.registry)
	if err != nil {
		return err
	}
	return p.add(doc)
}

func (p *Posting) Delete(doc *document.Document, _ *struct{}) error {
	p.documents++
	return nil
}

func (p *Posting) Search(doc *document.Document, result *document.SearchResult) error {
	return nil
}

func (p *Posting) List(in Query, out *Query) error {
	out.Start = in.Start
	out.Limit = in.Limit
	out.Result = in.Result
	if out.Start < p.offset {
		out.Start = p.offset
	}
	end := p.offset + p.size
	l := NewPostingLine()
	for out.Start < end && out.Limit > 0 {
		b, err := p.table.Get(out.Start - p.offset)
		if err != nil {
			return err
		}
		out.Start++
		if len(b) == 0 {
			continue
		}
		if err := l.Read(bytes.NewReader(b)); err != nil {
			return err
		}
		doctypes := make([]Doctype, len(l.Headers))
		for j, _ := range l.Headers {
			doctypes[j] = Doctype{
				Doctype: l.Headers[j].Doctype,
				Docids:  l.Blocks[j].Docids,
				Deltas:  l.Blocks[j].Deltas(),
			}
		}
		out.Result.Rows = append(out.Result.Rows, Row{
			Hash:     out.Start - 1,
			Bytes:    len(b),
			Doctypes: doctypes,
		})
		out.Limit--
	}
	out.Result.TotalRows += p.table.Count()
	return nil
}

func (p *Posting) stats() interface{} {
	return map[string]uint64{
		"offset":    p.offset,
		"size":      p.size,
		"documents": p.documents,
	}
}
