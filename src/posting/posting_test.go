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
	doc1, _ := document.BuildDocument(1, 2, "Document 1", text, nil)
	err := doc1.Save(s.Registry)
	c.Assert(err, IsNil)
	err = p.Add(&document.DocumentArg{Id: &doc1.Id}, nil)
	c.Assert(err, IsNil)
	doc2, _ := document.BuildDocument(1, 7, "Document 2", text, nil)
	err = doc2.Save(s.Registry)
	c.Assert(err, IsNil)
	err = p.Add(&document.DocumentArg{Id: &doc2.Id}, nil)
	c.Assert(err, IsNil)
}

func buildDocuments(s *PostingSuite, c *C) []*document.DocumentID {
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
	return ids
}

func (s *PostingSuite) TestAddDocumentWithoutClient(c *C) {
	p := newPosting(s.Registry, "test")
	p.Init(&s.Registry.PostingConfigs[0], nil)
	ids := buildDocuments(s, c)
	for _, id := range ids {
		err := p.Add(&document.DocumentArg{Id: id}, nil)
		c.Assert(err, IsNil)
	}
	for _, id := range ids {
		err := p.Delete(&document.DocumentArg{Id: id}, nil)
		c.Assert(err, IsNil)
	}
}

func (s *PostingSuite) TestAddDocumentViaClient(c *C) {
	go Serve(s.Registry)
	client, err := NewClient(s.Registry)
	client.Initialise()
	c.Check(err, IsNil)
	ids := buildDocuments(s, c)
	for _, id := range ids {
		err = client.CallMultiple("Posting.Add", &document.DocumentArg{Id: id})
		c.Assert(err, IsNil)
	}
	for _, id := range ids {
		err = client.CallMultiple("Posting.Delete", &document.DocumentArg{Id: id})
		c.Assert(err, IsNil)
	}
	client.Close()
}

func (s *PostingSuite) TestTemporarySearch(c *C) {
	go Serve(s.Registry)
	client, err := NewClient(s.Registry)
	client.Initialise()
	ids := buildDocuments(s, c)
	for _, id := range ids {
		err = client.CallMultiple("Posting.Add", &document.DocumentArg{Id: id})
		c.Assert(err, IsNil)
	}
	tempSearch := &document.DocumentArg{Text: document.RandomWords(10000)}
	_, err = client.Search(tempSearch)
	// c.Check(len(results.), Equals, 0)
	c.Assert(err, IsNil)
}
