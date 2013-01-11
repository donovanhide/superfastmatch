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
type IntersectionMap map[uint64]PositionSlice

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
	Meta           MetaMap       `json:"metaData,omitempty"`
	Associations   *Associations `json:",omitempty"`
	hashes         map[HashKey][]uint64
	hashsets       map[BloomKey]HashSet
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

func (d *Document) init() *Document {
	d.hashes = make(map[HashKey][]uint64)
	d.hashsets = make(map[BloomKey]HashSet)
	d.blooms = make(map[BloomKey]Bloom)
	return d
}

func BuildDocument(doctype uint32, docid uint32, title string, text string, meta MetaMap) (*Document, error) {
	doc := &Document{
		Id:     DocumentID{Doctype: doctype, Docid: docid},
		Title:  title,
		Text:   text,
		Meta:   meta,
		Length: uint64(utf8.RuneCountInString(text)),
	}
	return doc.init(), nil
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

func (d *Document) Common(other *Document, hashKey HashKey) *Pairs {
	bloomKey := BloomKey{
		HashKey: hashKey,
		Size:    d.Length,
	}
	hashes, bloom := d.HashSetAndBloom(bloomKey)
	inter, interBloom := make(IntersectionMap), NewFixedBloom(d.Length, 0.9)
	for i, h := range other.Hashes(hashKey) {
		if bloom.Test(h) {
			if _, ok := hashes[h]; ok {
				inter[h] = append(inter[h], i)
				interBloom.Set(h)
			}
		}
	}
	pairs := NewPairs(len(inter))
	for i, h := range d.Hashes(hashKey) {
		if interBloom.Test(h) {
			if positions, ok := inter[h]; ok {
				pairs.Append(i, positions)
			}
		}
	}
	return pairs
}

func (d *Document) HashSetAndBloom(key BloomKey) (HashSet, Bloom) {
	hashset, ok := d.hashsets[key]
	if ok {
		return hashset, d.blooms[key]
	}
	bloom := NewFixedBloom(key.Size, 0.1)
	ws := whiteSpaceHash(key.HashKey)
	hashset = make(HashSet)
	for _, h := range d.Hashes(key.HashKey) {
		if h != ws {
			hashset[h] = struct{}{}
			bloom.Set(h)
		}
	}
	d.hashsets[key] = hashset
	d.blooms[key] = bloom
	return hashset, bloom
}
