package document

import (
	"testing"
)

func Test_4598(t *testing.T) {
	doc, _ := BuildDocument(0, 0, "4598", openFile("../../fixtures/4598.txt.gz"), nil)
	key := HashKey{WindowSize: 30, HashWidth: 32}
	length := len(doc.Hashes(key))
	if length != 306153 {
		t.Errorf("Wrong number of hashes: %v", length)
	}
}

func Test_NormalisedText(t *testing.T) {
	doc1, _ := BuildDocument(1, 1, "This is a test", "This is some text,!&", nil)
	doc2, _ := BuildDocument(1, 1, "This is a test", "THIS IS SOME TEXT   ", nil)
	if doc1.NormalisedText().String() != doc2.NormalisedText().String() {
		t.Error("Bad text normalisation")
	}
}

func Test_Hashes(t *testing.T) {
	doc, _ := BuildDocument(1, 1, "This is a test", "Text gobble TEXT", nil)
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
