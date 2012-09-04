package document

import (
	"code.google.com/p/gorilla/mux"
	"errors"
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
	Id             DocumentID
	Title          string
	Text           string
	Length         int
	hashes         map[int][]uint32
	normalisedText string
}

type Result struct {
	Id      DocumentID
	results map[DocumentID][]uint32
}

func buildDocument(doctype uint32, docid uint32, title string, text string) *Document {
	return &Document{
		Id:     DocumentID{doctype, docid},
		Title:  title,
		Text:   text,
		Length: utf8.RuneCountInString(text),
	}
}

func NewDocument(req *http.Request) (*Document, error) {
	doctype, err := strconv.ParseUint(mux.Vars(req)["doctype"], 10, 32)
	if err != nil || doctype == 0 {
		return nil, errors.New("Bad Doctype")
	}
	docid, err := strconv.ParseUint(mux.Vars(req)["docid"], 10, 32)
	if err != nil || docid == 0 {
		return nil, errors.New("Bad Docid")
	}
	title := req.FormValue("title")
	text := req.FormValue("text")
	if len(title) == 0 || len(text) == 0 {
		return nil, errors.New("Missing title or text fields")
	}
	if !utf8.ValidString(title) || !utf8.ValidString(text) {
		return nil, errors.New("Invalid UTF8 submitted")
	}
	return buildDocument(uint32(doctype), uint32(docid), title, text), nil
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
