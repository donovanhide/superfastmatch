package query

import (
	"document"
	"github.com/gorilla/schema"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"regexp"
	"registry"
	"strconv"
	"strings"
)

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

func (r DocTypeRange) Parse() bson.M {
	if len(r) == 0 {
		return bson.M{}
	}
	sections := strings.Split(string(r), ":")
	filter := make([]bson.M, len(sections))
	for i, f := range sections {
		g := strings.Split(f, "-")
		start, _ := strconv.ParseUint(g[0], 10, 32)
		if len(g) == 2 {
			end, _ := strconv.ParseUint(g[1], 10, 32)
			filter[i] = bson.M{"_id.doctype": bson.M{"$gte": start, "$lte": end}}
		} else {
			filter[i] = bson.M{"_id.doctype": start}
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
	q.DefaultSort = []string{"doctype", "docid"}
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
	docs := make([]document.DocumentID, 0)
	db := registry.DB()
	defer db.Session.Close()
	query := DocTypeRange(docTypeRange).Parse()
	pipe := []bson.M{{"$project": bson.M{"doctype": "$_id.doctype", "docid": "$_id.docid"}}, {"$match": query}}
	err := db.C("documents").Pipe(pipe).All(&docs)
	return docs, err
}
