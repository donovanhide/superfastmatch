package document

import (
	"code.google.com/p/gorilla/mux"
	"errors"
	"exp/utf8string"
	"fmt"
	"net/http"
	"net/url"
	"registry"
	"strconv"
	"strings"
	"unicode/utf8"
)

type MetaMap map[string]interface{}

type HashKey struct {
	WindowSize uint64
	HashWidth  uint64
}

type BloomKey struct {
	HashKey
	Size uint64
}

type DocumentID struct {
	Doctype uint32 `json:"doctype"`
	Docid   uint32 `json:"docid"`
}

type Document struct {
	Id             DocumentID    `json:"id" bson:"_id"`
	Title          string        `json:"title"`
	Text           string        `json:",omitempty"`
	Length         uint64        `json:"characters"`
	Valid          bool          `json:"valid"`
	Meta           MetaMap       `json:"metaData,omitempty"`
	Associations   *Associations `json:",omitempty"`
	hashes         map[HashKey][]uint64
	blooms         map[BloomKey]Bloom
	normalisedText *utf8string.String
}

func (k *HashKey) String() string {
	return fmt.Sprintf("Window Size: %v Hash Width: %v", k.WindowSize, k.HashWidth)
}

func parseId(req *http.Request, key string) (uint32, error) {
	value, err := strconv.ParseUint(mux.Vars(req)[key], 10, 32)
	if err != nil || value == 0 {
		return 0, errors.New("Bad " + key)
	}
	return uint32(value), nil
}

func NewDocumentId(req *http.Request) (*DocumentID, error) {
	doctype, err := parseId(req, "doctype")
	if err != nil {
		return nil, err
	}
	docid, err := parseId(req, "docid")
	if err != nil {
		return nil, err
	}
	return &DocumentID{
		Doctype: doctype,
		Docid:   docid,
	}, nil
}

func (d *DocumentID) String() string {
	return fmt.Sprintf("(%v,%v)", d.Doctype, d.Docid)
}

func (d *Document) String() string {
	return fmt.Sprintf("%v %v %v", d.Id, d.Length, d.Title)
}

func (d *Document) Pretty(titleLimit int) string {
	title := utf8string.NewString(d.Title)
	length := min(titleLimit, title.RuneCount())
	format := fmt.Sprintf("Doc:%%%ds (%%d,%%d)", titleLimit)
	return fmt.Sprintf(format, title.Slice(0, length), d.Id.Doctype, d.Id.Docid)
}

func (d *Document) init() *Document {
	d.hashes = make(map[HashKey][]uint64)
	d.blooms = make(map[BloomKey]Bloom)
	return d
}

func BuildDocument(doctype uint32, docid uint32, title string, text string, meta MetaMap) (*Document, error) {
	doc := &Document{
		Id:     DocumentID{Doctype: doctype, Docid: docid},
		Title:  title,
		Text:   text,
		Meta:   meta,
		Valid:  utf8.ValidString(text),
		Length: uint64(utf8.RuneCountInString(text)),
	}
	return doc.init(), nil
}

func NewDocument(id *DocumentID, values *url.Values) (*Document, error) {
	title, text := values.Get("title"), values.Get("text")
	if len(title) == 0 || len(text) == 0 {
		return nil, errors.New("Missing title or text fields")
	}
	meta := make(MetaMap)
	for k, v := range *values {
		if k != "title" && k != "text" {
			meta[k] = v
		}
	}
	return BuildDocument(id.Doctype, id.Docid, title, text, meta)
}

func GetDocument(id *DocumentID, registry *registry.Registry) (*Document, error) {
	doc := &Document{Id: *id}
	if err := registry.C("documents").FindId(doc.Id).One(doc); err != nil {
		return nil, err
	}
	return doc.init(), nil
}

func GetDocuments(ids []DocumentID, registry *registry.Registry) chan *Document {
	c := make(chan *Document, 5)
	go func() {
		for _, id := range ids {
			if doc, err := GetDocument(&id, registry); err == nil {
				c <- doc
			}
		}
		close(c)
	}()
	return c
}

func (document *Document) Save(registry *registry.Registry) error {
	_, err := registry.C("documents").UpsertId(document.Id, document)
	return err
}

func (document *Document) Delete(registry *registry.Registry) error {
	return registry.C("documents").RemoveId(document.Id)
}

func (d *Document) AddAssociation(registry *registry.Registry, other *Document, saveThemes bool) *Association {
	if d.Associations == nil {
		d.Associations = &Associations{}
	}
	association, themes := BuildAssociation(registry.WindowSize, d, other)
	if len(association.Fragments) > 0 {
		d.Associations.Documents = append(d.Associations.Documents, *association)
	}
	if saveThemes {
		themes.Save(registry)
	}
	return association
}

func (d *Document) NormalisedText() *utf8string.String {
	if d.normalisedText == nil {
		d.normalisedText = utf8string.NewString(strings.Map(normaliseRune, d.Text))
	}
	return d.normalisedText
}

func (d *Document) runHasher(length uint64, key HashKey, f StreamFunc) {
	rollingRabinKarp3(d.NormalisedText().String(), length, key, f)
}

func (d *Document) HashLength(key HashKey) uint64 {
	if d.Length > key.WindowSize {
		return d.Length - key.WindowSize + 1
	}
	return 0
}

func (d *Document) ApplyHasher(key HashKey, f StreamFunc) {
	d.runHasher(d.HashLength(key), key, f)
}

func (d *Document) Hashes(key HashKey) []uint64 {
	hashes, ok := d.hashes[key]
	if ok {
		return hashes
	}
	length := d.HashLength(key)
	if length > 0 {
		hashes = make([]uint64, length)
		f := func(i int, h uint64) {
			hashes[i] = h
		}
		d.runHasher(length, key, f)
	}
	d.hashes[key] = hashes
	return hashes
}

func (d *Document) InvertedSlice(key HashKey, bloom Bloom) InvertedSlice {
	length := d.HashLength(key)
	if length > 0 {
		inverted := make(InvertedSlice, 0, bloom.Count())
		f := func(i int, h uint64) {
			if bloom.Test(h) {
				inverted = append(inverted, Inverted{uint32(h), int32(i)})
			}
		}
		d.runHasher(length, key, f)
		return inverted
	}
	return nil
}

func (d *Document) Bloom(key BloomKey) Bloom {
	bloom, ok := d.blooms[key]
	if ok {
		return bloom
	}
	length := d.HashLength(key.HashKey)
	if length > 0 {
		bloom = NewFixedBloom(key.Size, 0.1)
		ws := whiteSpaceHash(key.HashKey)
		f := func(i int, h uint64) {
			if h != ws {
				bloom.Set(h)
			}
		}
		d.runHasher(length, key.HashKey, f)
	}
	d.blooms[key] = bloom
	return bloom
}
