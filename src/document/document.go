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
type HashSet map[uint64]struct{}
type PositionSet map[int]struct{}
type IntersectionMap map[uint64]PositionSet

type HashKey struct {
	WindowSize uint64
	HashWidth  uint64
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
	Meta           MetaMap       `json:"metaData,omitempty"`
	Associations   *Associations `json:",omitempty"`
	hashes         map[HashKey][]uint64
	hashsets       map[HashKey]HashSet
	blooms         map[HashKey]Bloom
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

func BuildDocument(doctype uint32, docid uint32, title string, text string, meta MetaMap) (*Document, error) {
	return &Document{
		Id:       DocumentID{Doctype: doctype, Docid: docid},
		Title:    title,
		Text:     text,
		Meta:     meta,
		Length:   uint64(utf8.RuneCountInString(text)),
		hashsets: make(map[HashKey]HashSet),
		hashes:   make(map[HashKey][]uint64),
		blooms:   make(map[HashKey]Bloom),
	}, nil
}

func NewDocument(id *DocumentID, values *url.Values) (*Document, error) {
	title, text := values.Get("title"), values.Get("text")
	if len(title) == 0 || len(text) == 0 {
		return nil, errors.New("Missing title or text fields")
	}
	if !utf8.ValidString(title) || !utf8.ValidString(text) {
		return nil, errors.New("Invalid UTF8 submitted")
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
	doc.hashsets = make(map[HashKey]HashSet)
	doc.hashes = make(map[HashKey][]uint64)
	doc.blooms = make(map[HashKey]Bloom)
	return doc, nil
}

func GetDocuments(ids []DocumentID, registry *registry.Registry) chan *Document {
	c := make(chan *Document, 20)
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
	d.Associations.Documents = append(d.Associations.Documents, *association)
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

func (d *Document) Hashes(key HashKey) []uint64 {
	hashes, ok := d.hashes[key]
	if ok {
		return hashes
	}
	length := d.Length - key.WindowSize + 1
	if length > 0 {
		hashes = rollingRabinKarp3(d.NormalisedText().String(), length, key)
	}
	d.hashes[key] = hashes
	return hashes
}

func (d *Document) Intersection(other *Document, hashKey HashKey) (IntersectionMap, Bloom) {
	ws := whiteSpaceHash(hashKey)
	hashes := other.HashSet(hashKey)
	bloom := other.Bloom(hashKey)
	inter := make(IntersectionMap)
	b := NewFixedBloom(uint64(len(d.Hashes(hashKey))), 0.2)
	for i, h := range d.Hashes(hashKey) {
		if h != ws && bloom.Test(h) {
			if _, ok := hashes[h]; ok {
				if positions, ok := inter[h]; ok {
					positions[i] = struct{}{}
				} else {
					b.Set(h)
					inter[h] = PositionSet{i: {}}
				}
			}
		}
	}
	return inter, b
}

func (d *Document) Common(other *Document, hashKey HashKey) *Pairs {
	inter, bloom := other.Intersection(d, hashKey)
	pairs := NewPairs(len(inter))
	ws := whiteSpaceHash(hashKey)
	for i, h := range d.Hashes(hashKey) {
		if h != ws && bloom.Test(h) {
			if positions, ok := inter[h]; ok {
				pairs.Append(i, positions)
			}
		}
	}
	return pairs
}

func (d *Document) HashSet(key HashKey) HashSet {
	hashset, ok := d.hashsets[key]
	if ok {
		return hashset
	}
	ws := whiteSpaceHash(key)
	hashset = make(HashSet)
	for _, h := range d.Hashes(key) {
		if h != ws {
			hashset[h] = struct{}{}
		}
	}
	d.hashsets[key] = hashset
	return hashset
}

func (d *Document) Bloom(key HashKey) Bloom {
	bloom, ok := d.blooms[key]
	if ok {
		return bloom
	}
	bloom = NewFixedBloom(d.Length, 0.1)
	for h := range d.HashSet(key) {
		bloom.Set(h)
	}
	d.blooms[key] = bloom
	return bloom
}
