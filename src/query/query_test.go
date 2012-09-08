package query

import (
	// "labix.org/v2/mgo"
	// "fmt"
	. "launchpad.net/gocheck"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"testutils"
)

func Test(t *testing.T) { TestingT(t) }

type QuerySuite struct {
	testutils.DBSuite
}

var _ = Suite(&QuerySuite{})

func buildValues(method string, url string, doctype uint32, docid uint32) *url.Values {
	req, _ := http.NewRequest(method, url, nil)
	req.ParseForm()
	values := req.Form
	values["doctype"] = []string{strconv.FormatUint(uint64(doctype), 10)}
	values["docid"] = []string{strconv.FormatUint(uint64(docid), 10)}
	return &values
}

func (s *QuerySuite) TestFillDocumentQuery(c *C) {
	values := buildValues("GET", "http://testing.com/?limit=20&order_by=text", 1, 2)
	q := new(DocumentQueryParams)
	decoder.Decode(q, *values)
	q.DefaultSort = []string{"doctype", "docid"}
	q.getQuery(values, s.Db.C("documents"))
	c.Check(q.Doctype, Equals, uint32(1))
	c.Check(q.Docid, Equals, uint32(2))
	c.Check(q.Limit, Equals, 20)
	c.Check(q.Sort, DeepEquals, []string{"text", "doctype", "docid"})
}

func (s *QuerySuite) TestGetDocument(c *C) {
	values := buildValues("GET", "http://testing.com/1/2/?limit=20&order_by=text", 1, 2)
	d := s.Db.C("documents")
	results, err := GetDocuments(values, d)
	c.Assert(err, IsNil)
	c.Assert(results, NotNil)
	c.Assert(results.TotalRows, Equals, 0)
	c.Assert(len(results.Rows), Equals, 0)
}
