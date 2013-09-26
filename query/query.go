package query

import (
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/gorilla/schema"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Interval struct {
	start, end uint64
}

type IntervalSlice []Interval

func (s IntervalSlice) Len() int           { return len(s) }
func (s IntervalSlice) Less(i, j int) bool { return s[i].start < s[j].start }
func (s IntervalSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type DocTypeRange string

type QueryParams struct {
	Filter      bson.M
	Select      bson.M
	DefaultSort []string
	Sort        []string `schema:"order_by"`
	Start       int      `schema:"start"`
	Limit       int      `schema:"limit"`
}

type DocumentQueryParams struct {
	QueryParams
	Doctypes DocTypeRange `schema:"doctypes"`
}

type DocumentResult struct {
	Rows      []document.Document `json:"rows"`
	TotalRows int                 `json:"totalRows"`
}

var decoder = schema.NewDecoder()

func init() {
	decoder.RegisterConverter(DocTypeRange(""), decodeDoctypeRange)
}

func (q *QueryParams) getQuery(values *url.Values, coll *mgo.Collection) *mgo.Query {
	q.Limit = 10
	decoder.Decode(q, *values)
	q.Sort = append(q.Sort, q.DefaultSort...)
	return coll.Find(q.Filter).Select(q.Select).Sort(q.Sort...).Skip(q.Start).Limit(q.Limit)
}

func fillResult(r interface{}, query *mgo.Query) error {
	count, err := query.Limit(0).Count()
	if err != nil {
		return err
	}
	reflect.ValueOf(r).Elem().FieldByName("TotalRows").SetInt(int64(count))
	return nil
}

var docTypeRangeRegex = regexp.MustCompile(`^(\d+(-\d+)?(:\d+(-\d+)?)*)*$`)

func (r DocTypeRange) Valid() bool {
	return docTypeRangeRegex.MatchString(string(r))
}

func decodeDoctypeRange(value string) reflect.Value {
	return reflect.ValueOf(DocTypeRange(value))
}

func (r DocTypeRange) Intervals() IntervalSlice {
	if len(r) == 0 {
		return IntervalSlice{}
	}
	sections := strings.Split(string(r), ":")
	intervals := make(IntervalSlice, len(sections))
	for i, f := range sections {
		g := strings.Split(f, "-")
		start, _ := strconv.ParseUint(g[0], 10, 32)
		if len(g) == 2 {
			end, _ := strconv.ParseUint(g[1], 10, 32)
			intervals[i] = Interval{start, end}
		} else {
			intervals[i] = Interval{start, start}
		}
	}
	sort.Sort(intervals)
	return intervals
}

func (r DocTypeRange) Parse() bson.M {
	if len(r) == 0 {
		return bson.M{}
	}
	intervals := r.Intervals()
	filter := make([]bson.M, len(intervals))
	for i, interval := range intervals {
		if interval.start != interval.end {
			filter[i] = bson.M{"_id.doctype": bson.M{"$gte": interval.start, "$lte": interval.end}}
		} else {
			filter[i] = bson.M{"_id.doctype": interval.start}
		}
	}
	return bson.M{"$or": filter}
}

func GetDocuments(values *url.Values, registry *registry.Registry) (*DocumentResult, error) {
	q := new(DocumentQueryParams)
	r := new(DocumentResult)
	decoder.Decode(q, *values)
	if len(q.Doctypes) > 0 {
		q.Filter = q.Doctypes.Parse()
	}
	q.Select = bson.M{"text": 0}
	q.DefaultSort = []string{"_id.doctype", "_id.docid"}
	db := registry.DB()
	defer db.Session.Close()
	docs := q.getQuery(values, db.C("documents"))
	if err := docs.All(&r.Rows); err != nil {
		return nil, err
	}
	if err := fillResult(r, docs); err != nil {
		return nil, err
	}
	return r, nil
}

func GetDocids(docTypeRange string, registry *registry.Registry) ([]document.DocumentID, error) {
	ids := make([]document.DocumentID, 0)
	db := registry.DB()
	defer db.Session.Close()
	query := DocTypeRange(docTypeRange).Parse()
	var doc document.Document
	iter := db.C("documents").Find(query).Select(bson.M{"_id": 1}).Iter()
	for iter.Next(&doc) {
		ids = append(ids, doc.Id)
	}
	err := iter.Close()
	return ids, err
}
