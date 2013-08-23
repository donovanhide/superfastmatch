package document

import (
	"github.com/donovanhide/superfastmatch-go/testutils"
	. "launchpad.net/gocheck"
)

type AssociationSuite struct {
	testutils.DBSuite
}

func testIsSymmetric(windowSize uint64, left, right string, c *C) {
	doc1, _ := BuildDocument(0, 0, left[:20], left, nil)
	doc2, _ := BuildDocument(0, 0, right[:20], right, nil)
	a1, t1 := BuildAssociation(windowSize, doc1, doc2)
	a2, t2 := BuildAssociation(windowSize, doc2, doc1)
	msg := Commentf("Bad association: %d != %d\n%s\n%s", len(a1.Fragments), len(a2.Fragments), a1.Fragments.String(t1), a2.Fragments.String(t2))
	c.Check(len(a1.Fragments) == 0 || len(a1.Fragments) != len(a2.Fragments), Equals, true, msg)
	c.Check(len(t1), Equals, len(t2), Commentf("Bad themes: %d != %d\n%s\n%s", len(t1), len(t2), t1, t2))
	c.Log(len(a1.Fragments), len(a2.Fragments))
}

func testWithSelf(windowSize uint64, expectedFragments, expectedThemes int, text string, c *C) {
	doc, _ := BuildDocument(0, 0, text[:20], text, nil)
	a, themes := BuildAssociation(windowSize, doc, doc)
	msg := Commentf("Bad fragment count with self %d!=%d\n%s", len(a.Fragments), expectedFragments, a.Fragments.String(themes))
	c.Check(len(a.Fragments), Equals, expectedFragments, msg)
	msg = Commentf("Bad theme count with self %d!=%d\n%s", len(themes), expectedThemes, themes)
	c.Check(len(themes), Equals, expectedThemes, msg)
}

func (s *AssociationSuite) TestRealAssociation(c *C) {
	bible := openFile("../../fixtures/gutenberg/bible.txt.gz")
	koran := openFile("../../fixtures/gutenberg/koran.txt.gz")
	testIsSymmetric(30, bible, koran, c)
	testWithSelf(30, 108147, 13987, bible, c)
	testWithSelf(30, 25414, 2152, koran, c)
}
