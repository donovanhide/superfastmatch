package api

import (
	"code.google.com/p/gorilla/mux"
	"document"
	"labix.org/v2/mgo"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"query"
)

var c *rpc.Client
var s *mgo.Session

func getCollection(name string) *mgo.Collection {
	return s.Clone().DB("superfastmatch").C(name)
}

func documentsHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	switch req.Method {
	case "GET":
		documents, err := query.GetDocuments(&req.Form, getCollection("documents"))
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, req, documents)
	}
	return nil
}

func documentHandler(rw http.ResponseWriter, req *http.Request) *appError {
	switch req.Method {
	case "GET":
		document, err := document.GetDocument(req, getCollection("documents"))
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, req, document)
	case "POST":
		document, err := document.NewDocument(req)
		if err != nil {
			return &appError{err, "Badly formed Document", 500}
		}
		document.Save(getCollection("documents"))
		var success bool
		return doRPC("Posting.Add", *document, &success, rw)
	case "DELETE":
		document, err := document.NewDocument(req)
		if err != nil {
			return &appError{err, "Badly formed Document", 500}
		}
		var success bool
		return doRPC("Posting.Delete", *document, &success, rw)
	}
	return nil
}

func Serve(mongoConnection string, rpcConnection string) {
	session, err := mgo.Dial(mongoConnection)
	if err != nil {
		log.Fatal("Cannot create Mongo session:", err)
	}
	s = session
	defer s.Close()
	client, err := rpc.DialHTTP("tcp", rpcConnection)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	c = client
	defer c.Close()
	r := mux.NewRouter().StrictSlash(true)
	r.Handle("/document/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	r.Handle("/document/{doctype:[0-9]+}/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	r.Handle("/document/{doctype:[0-9]+}/{docid:[0-9]+}/", appHandler(documentHandler)).Methods("GET", "POST", "DELETE")
	http.ListenAndServe(":8080", r)
}
