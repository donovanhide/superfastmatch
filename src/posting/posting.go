package posting

import (
	"document"
	"fmt"
	"log"
	"os"
	"query"
	"registry"
	"sparsetable"
	"time"
)

type DocumentArg struct {
	Id   *document.DocumentID
	Text string
}

func (a *DocumentArg) GetDocument(registry *registry.Registry) (*document.Document, error) {
	if a.Id != nil {
		return document.GetDocument(a.Id, registry)
	}
	return document.BuildDocument(0, 0, "", a.Text)
}

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

func (p *Posting) remove(doc *document.Document) error {
	start := time.Now()
	hashes := doc.Hashes(p.hashKey)
	count, dupes, set, saturated := 0, 0, 0, 0
	l := NewPostingLine()
	for _, hash := range hashes {
		pos := hash - p.offset
		if pos >= p.size {
			continue
		}
		count++
		if err := p.table.Get(pos, l); err != nil {
			return err
		}
		if !l.RemoveDocumentId(&doc.Id) {
			dupes++
			continue
		}
		if err := p.table.Set(pos, l, l.Length); err != nil {
			if serr, ok := err.(*sparsetable.Error); ok {
				switch {
				case serr.Full:
					saturated++
				case serr.ShortRead:
					p.logger.Printf("Short Read for Document: %v Length: %v\n%v", doc.Id.String(), l.Length, l.String(true))
				default:
					return err
				}
			}
		}
		set++
	}
	if (set + dupes) != count {
		panic(fmt.Sprintln(count, dupes, set))
	}
	p.logger.Printf("Removed Document: %v Hashes: %v/%v Ignored: %.2f%% Saturated: %.2f%% Dupes: %.2f%% Speed: %.0f hashes/sec",
		doc.Id.String(),
		set,
		len(hashes),
		(float64(1)-(float64(count)/float64(len(hashes))))*100,
		(float64(saturated)/float64(set))*100,
		(float64(dupes)/float64(set))*100,
		float64(set)/time.Now().Sub(start).Seconds())
	p.documents--
	return nil
}

func (p *Posting) add(doc *document.Document) error {
	start := time.Now()
	hashes := doc.Hashes(p.hashKey)
	count, dupes, set, saturated := 0, 0, 0, 0
	l := NewPostingLine()
	for _, hash := range hashes {
		pos := hash - p.offset
		if pos >= p.size {
			continue
		}
		count++
		if err := p.table.Get(pos, l); err != nil {
			return err
		}
		if !l.AddDocumentId(&doc.Id) {
			dupes++
			continue
		}
		if err := p.table.Set(pos, l, l.Length); err != nil {
			if serr, ok := err.(*sparsetable.Error); ok {
				switch {
				case serr.Full:
					saturated++
				case serr.ShortRead:
					p.logger.Printf("Short Read for Document: %v Length: %v\n%v", doc.Id.String(), l.Length, l.String(true))
				default:
					return err
				}
			}
		}
		set++
	}
	if (set + dupes) != count {
		panic(fmt.Sprintln(count, dupes, set))
	}
	p.logger.Printf("Added Document: %v Hashes: %v/%v Ignored: %.2f%% Saturated: %.2f%% Dupes: %.2f%% Speed: %.0f hashes/sec",
		doc.Id.String(),
		set,
		len(hashes),
		(float64(1)-(float64(count)/float64(len(hashes))))*100,
		(float64(saturated)/float64(set))*100,
		(float64(dupes)/float64(set))*100,
		float64(set)/time.Now().Sub(start).Seconds())
	p.documents++
	return nil
}

func (p *Posting) search(doc *document.Document, results *document.SearchMap) error {
	start := time.Now()
	*results = make(document.SearchMap)
	hashes := doc.Hashes(p.hashKey)
	count := 0
	l := NewPostingLine()
	for i, hash := range hashes {
		pos := hash - p.offset
		if pos >= p.size {
			continue
		}
		count++
		if err := p.table.Get(pos, l); err != nil {
			return err
		}
		l.FillMap(results, uint32(i))
	}
	p.logger.Printf("Searched Document: %v Hashes: %v/%v Ignored: %.2f%% Speed: %.0f hashes/sec",
		doc.Id.String(),
		count,
		len(hashes),
		(float64(1)-(float64(count)/float64(len(hashes))))*100,
		float64(count)/time.Now().Sub(start).Seconds())
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
	c := document.GetDocuments(docids, p.registry)
	for doc := range c {
		if err = p.add(doc); err != nil {
			return err
		}
	}
	// expvar.Publish("table", expvar.Func(func() interface{} { return p.table.Stats() }))
	// expvar.Publish("posting", expvar.Func(func() interface{} { return p.stats() }))
	p.initialised = true
	p.logger.Println("Posting Server Initialised")
	return nil
}

func (p *Posting) Add(arg *DocumentArg, _ *struct{}) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return err
	}
	return p.add(doc)
}

func (p *Posting) Delete(arg *DocumentArg, _ *struct{}) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return err
	}
	return p.remove(doc)
}

func (p *Posting) Search(arg *DocumentArg, result *document.SearchMap) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return err
	}
	return p.search(doc, result)
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
		err := p.table.Get(out.Start-p.offset, l)
		if err != nil {
			return err
		}
		out.Start++
		if l.Length <= 1 {
			continue
		}
		doctypes := make([]Doctype, l.count)
		for j, h := 0, l.headers.Front(); h != nil && j < int(l.count); h = h.Next() {
			header := h.Value.(*Header)
			doctypes[j] = Doctype{
				Doctype: header.doctype,
				Docids:  header.Docids(),
				Deltas:  header.Deltas(),
			}
			j++
		}
		out.Result.Rows = append(out.Result.Rows, Row{
			Hash:     out.Start - 1,
			Bytes:    l.Length,
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
