package posting

import (
	"document"
	. "launchpad.net/gocheck"
	"testing"
	"testutils"
)

func Test(t *testing.T) { TestingT(t) }

type PostingSuite struct {
	testutils.DBSuite
}

var _ = Suite(&PostingSuite{})

func (s *PostingSuite) TestAddDocument(c *C) {
	go Serve(s.Registry)
	id := &document.DocumentID{
		Doctype: 1,
		Docid:   1,
	}
	doc, err := document.NewTestDocument(id, 5000)
	c.Check(err, IsNil)
	err = doc.Save(s.Registry)
	c.Check(err, IsNil)
	client, err := NewClient(s.Registry)
	client.Initialise()
	c.Check(err, IsNil)
	client.CallMultiple("Posting.Add", &doc.Id)
	// c.Check(p.table.Count(), Not(Equals), 0)
}
