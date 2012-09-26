package query

import (
	"code.google.com/p/gorilla/schema"
	"document"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"registry"
	"strconv"
	"strings"
)

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
	Doctypes string `schema:"doctypes"`
}

type DocumentResult struct {
	Rows      []document.Document `json:"rows"`
	TotalRows int                 `json:"totalRows"`
}

var decoder = schema.NewDecoder()

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

func parseDocTypeRange(r string) bson.M {
	if len(r) == 0 {
		return bson.M{}
	}
	sections := strings.Split(r, ":")
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
		q.Filter = parseDocTypeRange(q.Doctypes)
	}
	q.Select = bson.M{"text": 0}
	q.DefaultSort = []string{"doctype", "docid"}
	docs := q.getQuery(values, registry.C("documents"))
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
	query := parseDocTypeRange(docTypeRange)
	pipe := []bson.M{{"$project": bson.M{"doctype": "$_id.doctype", "docid": "$_id.docid"}}, {"$match": query}}
	err := registry.C("documents").Pipe(pipe).All(&docs)
	return docs, err
}
