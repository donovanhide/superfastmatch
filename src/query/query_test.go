package query

import (
	"document"
	. "launchpad.net/gocheck"
	"net/http"
	"net/url"
	"testing"
	"testutils"
)

func Test(t *testing.T) { TestingT(t) }

type QuerySuite struct {
	testutils.DBSuite
}

var _ = Suite(&QuerySuite{})

func buildValues(method string, url string, doctypes string) *url.Values {
	req, _ := http.NewRequest(method, url, nil)
	req.ParseForm()
	req.Form.Set("doctypes", doctypes)
	return &req.Form
}

func (s *QuerySuite) TestFillDocumentQuery(c *C) {
	values := buildValues("GET", "http://testing.com/?limit=20&order_by=text", "1")
	q := new(DocumentQueryParams)
	decoder.Decode(q, *values)
	q.DefaultSort = []string{"doctype", "docid"}
	q.getQuery(values, s.Db().C("documents"))
	c.Check(q.Doctypes, Equals, "1")
	c.Check(q.Limit, Equals, 20)
	c.Check(q.Sort, DeepEquals, []string{"text", "doctype", "docid"})
}

func (s *QuerySuite) TestGetDocuments(c *C) {
	c.Check(document.BuildTestCorpus(s.Db(), 10, 20, 500), IsNil)
	values := buildValues("GET", "http://testing.com/?limit=20&order_by=text", "1")
	results, err := GetDocuments(values, s.Db())
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 20)
	c.Assert(len(results.Rows), Equals, 20)
	values = buildValues("GET", "http://testing.com/?limit=20&order_by=-length", "1-10")
	results, err = GetDocuments(values, s.Db())
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 200)
	c.Assert(len(results.Rows), Equals, 20)
	values = buildValues("GET", "http://testing.com/?limit=40&order_by=title", "1-2:10")
	results, err = GetDocuments(values, s.Db())
	c.Assert(err, IsNil)
	c.Assert(results.TotalRows, Equals, 60)
	c.Assert(len(results.Rows), Equals, 40)
}
