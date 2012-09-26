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
	p.logger.Printf("Adding Document: %v", doc.Id)
	for _, hash := range doc.Hashes(p.hashKey) {
		pos := hash - p.offset
		if pos < p.size {
			b, err := p.table.Get(pos)
			if err != nil {
				return err
			}
			l, err := ReadPostingLine(bytes.NewReader(b))
			if err != nil {
				return err
			}
			l.AddDocumentId(&doc.Id)
			err = p.table.Set(pos, l.Write())
			if err != nil {
				return err
			}
		}
	}
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

func (p *Posting) List(query *QueryParams, result *ListResult) error {
	i := query.Start + uint64(len(result.Rows))
	end := p.offset + p.size
	for i < end && len(result.Rows) < query.Limit {
		b, err := p.table.Get(i)
		if err != nil {
			return err
		}
		i++
		if len(b) == 0 {
			continue
		}
		l, err := ReadPostingLine(bytes.NewReader(b))
		if err != nil {
			return err
		}
		h := Hash{
			Hash:  i,
			Bytes: len(b),
		}
		for j, _ := range l.Headers {
			h.Doctypes = Doctypes{
				Doctype: l.Headers[j].Doctype,
				Docids:  l.Blocks[j].Docids,
				Deltas:  l.Blocks[j].Deltas(),
			}
		}
		result.Rows = append(result.Rows, h)
	}
	result.TotalRows += p.table.Count()
	return nil
}

func (p *Posting) stats() interface{} {
	return map[string]uint64{
		"offset":    p.offset,
		"size":      p.size,
		"documents": p.documents,
	}
}
