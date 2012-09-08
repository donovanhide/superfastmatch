package query

import (
	"code.google.com/p/gorilla/schema"
	"document"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
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
	Doctype uint32 `schema:"doctype"`
	Docid   uint32 `schema:"docid"`
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

func GetDocuments(values *url.Values, coll *mgo.Collection) (*DocumentResult, error) {
	q := new(DocumentQueryParams)
	r := new(DocumentResult)
	decoder.Decode(q, *values)
	if q.Doctype != 0 {
		q.Filter = bson.M{"_id.doctype": q.Doctype}
	}
	q.Select = bson.M{"text": 0}
	q.DefaultSort = []string{"doctype", "docid"}
	docs := q.getQuery(values, coll)
	if err := docs.All(&r.Rows); err != nil {
		return nil, err
	}
	if err := fillResult(r, docs); err != nil {
		return nil, err
	}
	return r, nil
}
