package document

import (
	"code.google.com/p/gorilla/mux"
	"errors"
	"labix.org/v2/mgo"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

type DocumentID struct {
	Doctype uint32
	Docid   uint32
}

type Document struct {
	Id             DocumentID `bson:"_id"`
	Title          string
	Text           string `json:",omitempty"`
	Length         int
	Meta           map[string]interface{}
	hashes         map[int][]uint32
	normalisedText string
}

type Result struct {
	Id      DocumentID
	results map[DocumentID][]uint32
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

func NewDocument(req *http.Request) (*Document, error) {
	id, err := NewDocumentId(req)
	if err != nil {
		return nil, err
	}
	title := req.FormValue("title")
	text := req.FormValue("text")
	if len(title) == 0 || len(text) == 0 {
		return nil, errors.New("Missing title or text fields")
	}
	if !utf8.ValidString(title) || !utf8.ValidString(text) {
		return nil, errors.New("Invalid UTF8 submitted")
	}
	return &Document{
		Id:     *id,
		Title:  title,
		Text:   text,
		Length: utf8.RuneCountInString(text),
	}, nil
}

func GetDocument(req *http.Request, coll *mgo.Collection) (*Document, error) {
	id, err := NewDocumentId(req)
	if err != nil {
		return nil, err
	}
	document := Document{Id: *id}
	err = coll.FindId(document.Id).One(&document)
	if err != nil {
		return nil, err
	}
	return &document, nil
}

func (document *Document) Save(coll *mgo.Collection) error {
	_, err := coll.UpsertId(document.Id, document)
	return err
}

func (document *Document) NormalisedText() string {
	if len(document.normalisedText) == 0 {
		document.normalisedText = strings.Map(normaliseRune, document.Text)
	}
	return document.normalisedText
}

func (document *Document) Hashes(windowSize int) []uint32 {
	count := document.Length - windowSize + 1
	hashes := document.hashes[windowSize]
	if len(hashes) == 0 && count > 0 {
		hashes = rollingRabinKarp(document.NormalisedText(), windowSize, count)
	}
	return hashes
}
