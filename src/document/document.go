package document

import (
	"code.google.com/p/gorilla/mux"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"registry"
	"strconv"
	"strings"
	"unicode/utf8"
)

type HashKey struct {
	WindowSize uint64
	HashWidth  uint64
}

type DocumentID struct {
	Doctype uint32
	Docid   uint32
}

type Document struct {
	Id             DocumentID `bson:"_id"`
	Title          string
	Text           string `json:",omitempty"`
	Length         uint64
	Meta           map[string]interface{}
	hashes         map[HashKey][]uint64
	normalisedText string
}

type SearchResult struct {
	Id      DocumentID
	results map[DocumentID][]uint32
}

var words []string

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

func NewDocument(id *DocumentID, values *url.Values) (*Document, error) {
	if len(values.Get("title")) == 0 || len(values.Get("text")) == 0 {
		return nil, errors.New("Missing title or text fields")
	}
	if !utf8.ValidString(values.Get("title")) || !utf8.ValidString(values.Get("text")) {
		return nil, errors.New("Invalid UTF8 submitted")
	}
	meta := make(map[string]interface{})
	for k, v := range *values {
		if k != "title" && k != "text" {
			meta[k] = v
		}
	}
	return &Document{
		Id:     *id,
		Title:  values.Get("title"),
		Text:   values.Get("text"),
		Meta:   meta,
		Length: uint64(utf8.RuneCountInString(values.Get("text"))),
	}, nil
}

func GetDocument(id *DocumentID, registry *registry.Registry) (*Document, error) {
	document := Document{Id: *id}
	err := registry.C("documents").FindId(document.Id).One(&document)
	if err != nil {
		return nil, err
	}
	return &document, nil
}

func (document *Document) Save(registry *registry.Registry) error {
	_, err := registry.C("documents").UpsertId(document.Id, document)
	return err
}

func (document *Document) Delete(registry *registry.Registry) error {
	return registry.C("documents").RemoveId(document.Id)
}

func (document *Document) NormalisedText() string {
	if len(document.normalisedText) == 0 {
		document.normalisedText = strings.Map(normaliseRune, document.Text)
	}
	return document.normalisedText
}

func (document *Document) Hashes(key HashKey) []uint64 {
	length := document.Length - key.WindowSize + 1
	hashes := document.hashes[key]
	if len(hashes) == 0 && length > 0 {
		hashes = rollingRabinKarp(document.NormalisedText(), length, key)
	}
	return hashes
}
