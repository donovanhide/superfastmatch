package document

import (
	"github.com/donovanhide/superfastmatch-go/testutils"
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type DocumentSuite struct {
	testutils.DBSuite
}

var _ = Suite(&DocumentSuite{})

func (s *DocumentSuite) Test_4598(c *C) {
	doc, err := BuildDocument(0, 0, "4598", openFile("../../fixtures/4598.txt.gz"), nil)
	c.Check(err, IsNil)
	key := HashKey{WindowSize: 30, HashWidth: 32}
	length := len(doc.Hashes(key))
	c.Check(length, Equals, 306153) // Wrong number of hashes
}

func (s *DocumentSuite) Test_NormalisedText(c *C) {
	doc1, _ := BuildDocument(1, 1, "This is a test", "This is some text,!&", nil)
	doc2, _ := BuildDocument(1, 1, "This is a test", "THIS IS SOME TEXT   ", nil)
	c.Check(doc1.NormalisedText().String(), Equals, doc2.NormalisedText().String()) //Bad text normalisation
}

func (s *DocumentSuite) Test_Hashes(c *C) {
	doc, _ := BuildDocument(1, 1, "This is a test", "Text gobble TEXT", nil)
	key := HashKey{WindowSize: 4, HashWidth: 32}
	c.Check(len(doc.Hashes(key)), Equals, 13, Commentf("Wrong number of hashes: %v", len(doc.Hashes(key))))
	firstHash := doc.Hashes(key)[0]
	lastHash := doc.Hashes(key)[12]
	c.Check(firstHash, Equals, lastHash, Commentf("Incorrect hashes created: %v %v %v", firstHash, lastHash, doc.Hashes(key)))
}

func (s *DocumentSuite) Test_TestDocument(c *C) {
	id := &DocumentID{
		Doctype: 1,
		Docid:   1,
	}
	doc, err := NewTestDocument(id, 100)
	c.Check(err, IsNil)
	c.Check(doc.Length, Not(Equals), 0) //Bad Test Document
}
