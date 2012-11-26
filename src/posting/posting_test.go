package posting

import (
	"document"
	. "launchpad.net/gocheck"
	"math/rand"
	"testing"
	"testutils"
)

func Test(t *testing.T) { TestingT(t) }

type PostingSuite struct {
	testutils.DBSuite
}

var _ = Suite(&PostingSuite{})

const docCount = 10

func (s *PostingSuite) TestSimpleAddDocument(c *C) {
	text := document.RandomWords(120)
	p := newPosting(s.Registry, "test")
	p.Init(&s.Registry.PostingConfigs[0], nil)
	doc1 := document.BuildDocument(1, 2, "Document 1", text)
	err := doc1.Save(s.Registry)
	c.Assert(err, IsNil)
	err = p.Add(&doc1.Id, nil)
	c.Assert(err, IsNil)
	doc2 := document.BuildDocument(1, 7, "Document 2", text)
	err = doc2.Save(s.Registry)
	c.Assert(err, IsNil)
	err = p.Add(&doc2.Id, nil)
	c.Assert(err, IsNil)
}

func (s *PostingSuite) TestAddDocumentWithoutClient(c *C) {
	p := newPosting(s.Registry, "test")
	p.Init(&s.Registry.PostingConfigs[0], nil)
	ids := make([]*document.DocumentID, docCount)
	for i := 0; i < docCount; i++ {
		id := &document.DocumentID{
			Doctype: rand.Uint32()%100 + 1,
			Docid:   rand.Uint32()%500 + 1,
		}
		doc, err := document.NewTestDocument(id, 100000)
		c.Assert(err, IsNil)
		err = doc.Save(s.Registry)
		c.Assert(err, IsNil)
		ids[i] = id
	}
	for _, id := range ids {
		err := p.Add(id, nil)
		c.Assert(err, IsNil)
	}
	for _, id := range ids {
		err := p.Delete(id, nil)
		c.Assert(err, IsNil)
	}

}

func (s *PostingSuite) TestAddDocumentViaClient(c *C) {
	go Serve(s.Registry)
	client, err := NewClient(s.Registry)
	client.Initialise()
	c.Check(err, IsNil)
	for i := 0; i < docCount; i++ {
		id := &document.DocumentID{
			Doctype: rand.Uint32()%100 + 1,
			Docid:   rand.Uint32()%500 + 1,
		}
		doc, err := document.NewTestDocument(id, 100000)
		c.Assert(err, IsNil)
		err = doc.Save(s.Registry)
		c.Assert(err, IsNil)
		err = client.CallMultiple("Posting.Add", &doc.Id)
		c.Assert(err, IsNil)
	}
}
