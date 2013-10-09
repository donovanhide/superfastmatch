package document

import (
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/gorilla/schema"
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
	Doctypes DocTypeRange `schema:"doctypes"`
}

type DocumentResult struct {
	Rows      []Document `json:"rows"`
	TotalRows int        `json:"totalRows"`
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

func decodeDoctypeRange(value string) reflect.Value {
	return reflect.ValueOf(DocTypeRange(value))
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

func GetDocids(docTypeRange string, registry *registry.Registry) ([]DocumentID, error) {
	ids := make([]DocumentID, 0)
	db := registry.DB()
	defer db.Session.Close()
	query := DocTypeRange(docTypeRange).Parse()
	var doc Document
	iter := db.C("documents").Find(query).Select(bson.M{"_id": 1}).Iter()
	for iter.Next(&doc) {
		ids = append(ids, doc.Id)
	}
	err := iter.Close()
	return ids, err
}
