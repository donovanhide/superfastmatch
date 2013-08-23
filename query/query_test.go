package query

import (
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/testutils"
	. "launchpad.net/gocheck"
	"net/http"
	"net/url"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type QuerySuite struct {
	testutils.DBSuite
}

var _ = Suite(&QuerySuite{})

var doctypeRangeTests = []struct {
	doctypes DocTypeRange
	valid    bool
}{
	{"", true},
	{"1", true},
	{"1:3", true},
	{"1-2:3", true},
	{"1-2:3-2", true},
	{"1-2:3-2", true},
	{"-", false},
	{":", false},
	{"1-", false},
	{"-1", false},
	{"1-2:", false},
	{":1-2", false},
	{"asas1-2asas", false},
	{"1:2asas", false},
}

func buildValues(method string, url string, doctypes string) *url.Values {
	req, _ := http.NewRequest(method, url, nil)
	req.ParseForm()
	req.Form.Set("doctypes", doctypes)
	return &req.Form
}

func TestDocTypeRange(t *testing.T) {
	for _, tt := range doctypeRangeTests {
		if tt.doctypes.Valid() != tt.valid {
			t.Errorf("Validity of \"%s\"!=%v", tt.doctypes, tt.valid)
		}
	}
}

func (s *QuerySuite) TestFillDocumentQuery(c *C) {
	values := buildValues("GET", "http://testing.com/?limit=20&order_by=text", "1")
	q := new(DocumentQueryParams)
	decoder.Decode(q, *values)
	q.DefaultSort = []string{"doctype", "docid"}
	db := s.Registry.DB()
	defer db.Session.Close()
	q.getQuery(values, db.C("documents"))
	c.Check(q.Doctypes, Equals, DocTypeRange("1"))
	c.Check(q.Limit, Equals, 20)
	c.Check(q.Sort, DeepEquals, []string{"text", "doctype", "docid"})
}

func (s *QuerySuite) TestGetDocuments(c *C) {
	docs := document.BuildTestCorpus(10, 20, 500)
	for doc := <-docs; doc != nil; doc = <-docs {
		c.Assert(doc.Save(s.Registry), IsNil)
	}
	close(docs)
	values := buildValues("GET", "http://testing.com/?limit=20&order_by=text", "1")
	results, err := GetDocuments(values, s.Registry)
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 20)
	c.Assert(len(results.Rows), Equals, 20)
	values = buildValues("GET", "http://testing.com/?limit=20&order_by=-length", "1-10")
	results, err = GetDocuments(values, s.Registry)
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 200)
	c.Assert(len(results.Rows), Equals, 20)
	values = buildValues("GET", "http://testing.com/?limit=40&order_by=title", "1-2:10")
	results, err = GetDocuments(values, s.Registry)
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 60)
	c.Assert(len(results.Rows), Equals, 40)
}

func (s *QuerySuite) TestGetDocids(c *C) {
	docs := document.BuildTestCorpus(10, 20, 500)
	for doc := <-docs; doc != nil; doc = <-docs {
		c.Assert(doc.Save(s.Registry), IsNil)
	}
	close(docs)
	docids, err := GetDocids("1-10", s.Registry)
	c.Assert(len(docids), Equals, 200)
	c.Assert(err, IsNil)
	docids, err = GetDocids("1:10", s.Registry)
	c.Assert(len(docids), Equals, 40)
	c.Assert(err, IsNil)
	docids, err = GetDocids("1", s.Registry)
	c.Assert(len(docids), Equals, 20)
	c.Assert(err, IsNil)
	docids, err = GetDocids("", s.Registry)
	c.Assert(len(docids), Equals, 200)
	c.Assert(err, IsNil)
	docids, err = GetDocids("999", s.Registry)
	c.Assert(len(docids), Equals, 0)
	c.Assert(err, IsNil)
}
