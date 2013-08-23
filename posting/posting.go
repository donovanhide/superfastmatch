package posting

import (
	"errors"
	"fmt"
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/query"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/donovanhide/superfastmatch/sparsetable"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Posting struct {
	logger    *log.Logger
	lock      sync.RWMutex
	hashKey   document.HashKey
	offset    uint64
	size      uint64
	documents uint64
	registry  *registry.Registry
	table     *sparsetable.SparseTable
}

func newPostingError(s string, err error) error {
	return errors.New(fmt.Sprint(s, err))
}

func newPosting(registry *registry.Registry, prefix string) *Posting {
	return &Posting{
		registry: registry,
		logger:   log.New(os.Stderr, prefix+":", log.LstdFlags),
	}
}

const (
	Add = iota
	Delete
)

type Stats struct {
	doc       *document.Document
	start     time.Time
	length    uint64
	count     int
	dupes     int
	ops       int
	saturated int
}

func (s *Stats) Valid() bool {
	return (s.ops + s.dupes) == s.count
}

func (s *Stats) String() string {
	return fmt.Sprintf("%v Hashes: %v/%v Ignored: %.2f%% Saturated: %.2f%% Dupes: %.2f%% Speed: %.0f hashes/sec",
		s.doc.Id.String(),
		s.ops,
		s.length,
		(float64(1)-(float64(s.count)/float64(s.length)))*100,
		(float64(s.saturated)/float64(s.count))*100,
		(float64(s.dupes)/float64(s.count))*100,
		float64(s.ops)/time.Now().Sub(s.start).Seconds())
}

func (p *Posting) alter(operation int, doc *document.Document) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	l := NewPostingLine()
	stats := &Stats{
		doc:    doc,
		start:  time.Now(),
		length: doc.HashLength(p.hashKey),
	}
	alterFunc := func(i int, hash uint64) {
		pos := hash - p.offset
		if pos >= p.size {
			return
		}
		stats.count++
		if err := p.table.Get(pos, l); err != nil {
			panic(newPostingError("Add Document: Sparsetable Get:", err))
		}
		switch operation {
		case Add:
			if !l.AddDocumentId(&doc.Id) {
				stats.dupes++
				return
			}
		case Delete:
			if !l.RemoveDocumentId(&doc.Id) {
				stats.dupes++
				return
			}
		}
		if err := p.table.Set(pos, l, l.Length); err != nil {
			if serr, ok := err.(*sparsetable.Error); ok {
				switch {
				case serr.Full:
					stats.saturated++
				case serr.ShortRead:
					p.logger.Printf("Short Read for Document: %v Length: %v\n%v", doc.Id.String(), l.Length, l.String(true))
				default:
					panic(newPostingError("Add Document: Sparsetable Set:", err))
				}
			}
		}
		stats.ops++
	}
	doc.ApplyHasher(p.hashKey, alterFunc)
	switch operation {
	case Add:
		p.logger.Println("Added Document:", stats.String())
		p.documents++
	case Delete:
		p.logger.Println("Deleted Document:", stats.String())
		p.documents--
	}
	return nil
}

func (p *Posting) search(doc *document.Document, results *document.SearchMap) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	stats := &Stats{
		doc:    doc,
		start:  time.Now(),
		length: doc.HashLength(p.hashKey),
	}
	l := NewPostingLine()
	*results = make(document.SearchMap)
	searchFunc := func(i int, hash uint64) {
		pos := hash - p.offset
		if pos >= p.size {
			return
		}
		stats.count++
		if err := p.table.Get(pos, l); err != nil {
			panic(newPostingError("Search Document: Sparsetable Get:", err))
		}
		stats.ops++
		l.FillMap(results, uint32(i))
	}
	doc.ApplyHasher(p.hashKey, searchFunc)
	p.logger.Println("Searched Document: ", stats.String())
	return nil
}

func (p *Posting) init(conf *registry.PostingConfig, c chan *document.Document) error {
	start := time.Now()
	p.table = sparsetable.Init(conf.Size, conf.GroupSize)
	p.hashKey = document.HashKey{
		HashWidth:  conf.HashWidth,
		WindowSize: conf.WindowSize,
	}
	p.offset = conf.Offset
	p.size = conf.Size
	p.logger.Printf("Initialising Posting Server with %v Size: %d Offset: %d", p.hashKey.String(), p.size, p.offset)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
load:
	for doc := range c {
		select {
		case <-sig:
			break load
		default:
			if err := p.alter(Add, doc); err != nil {
				return newPostingError("Init:", err)
			}
		}
	}
	duration, average := time.Now().Sub(start).Seconds(), 0.0
	if p.documents > 0 {
		average = duration / float64(p.documents)
	}
	p.logger.Printf("Posting Server Initialised with %v documents in %.2f secs Average: %.2f secs/doc", p.documents, duration, average)
	return nil
}

func (p *Posting) Init(conf *registry.PostingConfig, reply *bool) error {
	docids, err := query.GetDocids(conf.InitialQuery, p.registry)
	if err != nil {
		return newPostingError("Get Document:", err)
	}
	c := document.GetDocuments(docids, p.registry)
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.init(conf, c)
}

func (p *Posting) Add(arg *document.DocumentArg, _ *struct{}) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return newPostingError("Add Document:", err)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.alter(Add, doc)
}

func (p *Posting) Delete(arg *document.DocumentArg, _ *struct{}) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return newPostingError("Delete Document:", err)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.alter(Delete, doc)
}

func (p *Posting) Search(arg *document.DocumentArg, result *document.SearchMap) error {
	doc, err := arg.GetDocument(p.registry)
	if err != nil {
		return newPostingError("Search Document:", err)
	}
	p.lock.RLock()
	defer p.lock.RUnlock()
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
	p.lock.RLock()
	defer p.lock.Unlock()
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
				Doctype: header.Doctype,
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
