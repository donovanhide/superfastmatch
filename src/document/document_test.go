package document

import (
	"testing"
)

func Test_NormalisedText(t *testing.T) {
	doc1 := BuildDocument(1, 1, "This is a test", "This is some text,!&")
	doc2 := BuildDocument(1, 1, "This is a test", "THIS IS SOME TEXT   ")
	if doc1.NormalisedText() != doc2.NormalisedText() {
		t.Error("Bad text normalisation")
	}
}

func Test_Hashes(t *testing.T) {
	doc := BuildDocument(1, 1, "This is a test", "Text gobble TEXT")
	key := HashKey{WindowSize: 4, HashWidth: 32}
	if len(doc.Hashes(key)) != 13 {
		t.Errorf("Wrong number of hashes: %v", len(doc.Hashes(key)))
	}
	firstHash := doc.Hashes(key)[0]
	lastHash := doc.Hashes(key)[12]
	if firstHash != lastHash {
		t.Errorf("Incorrect hashes created: %v %v %v", firstHash, lastHash, doc.Hashes(key))
	}
}

func Test_TestDocument(t *testing.T) {
	id := &DocumentID{
		Doctype: 1,
		Docid:   1,
	}
	doc, err := NewTestDocument(id, 100)
	if err != nil {
		t.Errorf(err.Error())
	}
	if doc.Length == 0 {
		t.Errorf("Bad Test Document")
	}
}
