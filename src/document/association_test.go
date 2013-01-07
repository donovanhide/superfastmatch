package document

import (
	"testing"
)

func testIsSymmetric(windowSize uint64, left, right string, t *testing.T) {
	doc1, _ := BuildDocument(0, 0, left[:20], left, nil)
	doc2, _ := BuildDocument(0, 0, right[:20], right, nil)
	a1, t1 := BuildAssociation(windowSize, doc1, doc2)
	a2, t2 := BuildAssociation(windowSize, doc2, doc1)
	if len(a1.Fragments) != len(a2.Fragments) {
		t.Errorf("Bad association: %d != %d\n%s", len(a1.Fragments), len(a2.Fragments), a1.Fragments[:100].String(t1))
	}
	if len(t1) != len(t2) {
		t.Errorf("Bad themes: %d != %d\n%s\n%s", len(t1), len(t2), t1, t2)
	}
}

func testWithSelf(windowSize uint64, expectedFragments, expectedThemes int, text string, t *testing.T) {
	doc, _ := BuildDocument(0, 0, text[:20], text, nil)
	a, themes := BuildAssociation(windowSize, doc, doc)
	if len(a.Fragments) != expectedFragments {
		t.Errorf("Bad fragment count with self %d!=%d\n%s", len(a.Fragments), expectedFragments, a.Fragments.String(themes))
	}
	if len(themes) != expectedThemes {
		t.Errorf("Bad theme count with self %d!=%d\n%s", len(themes), expectedThemes, themes)
	}
}

func TestRealAssociation(t *testing.T) {
	bible := openFile("../../fixtures/bible.txt.gz")
	koran := openFile("../../fixtures/koran.txt.gz")
	testIsSymmetric(30, bible, koran, t)
	testWithSelf(30, 108147, 13987, bible, t)
	testWithSelf(30, 25414, 2152, koran, t)
}
