package document

import (
	"testing"
)

func Test_NormalisedText(t *testing.T) {
	doc1 := buildDocument(1, 1, "This is a test", "This is some text,!&")
	doc2 := buildDocument(1, 1, "This is a test", "THIS IS SOME TEXT   ")
	if doc1.NormalisedText() != doc2.NormalisedText() {
		t.Error("Bad text normalisation")
	}
}

func Test_Hashes(t *testing.T) {
	doc := buildDocument(1, 1, "This is a test", "Text gobble TEXT")
	if len(doc.Hashes(4)) != 13 {
		t.Errorf("Wrong number of hashes: %v", len(doc.Hashes(4)))
	}
	firstHash := doc.Hashes(4)[0]
	lastHash := doc.Hashes(4)[12]
	if firstHash != lastHash {
		t.Errorf("Incorrect hashes created: %v %v %v", firstHash, lastHash, doc.Hashes(4))
	}
}
