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
	"queue"
)

var c *rpc.Client
var s *mgo.Session

func getCollection(name string) *mgo.Collection {
	return s.Clone().DB("superfastmatch").C(name)
}

func getDb() *mgo.Database {
	return s.Clone().DB("superfastmatch")
}

type QueuedResponse struct {
	Success bool   `json:"success"`
	QueueID string `json:"queueid"`
}

func testHandler(rw http.ResponseWriter, req *http.Request) *appError {
	item, err := queue.NewQueueItem(getDb(), "Test Corpus", nil, nil, nil, nil, req.Body)
	if err != nil {
		return &appError{err, "Test Corpus Problem", 500}
	}
	return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
}

func documentsHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	switch req.Method {
	case "GET":
		documents, err := query.GetDocuments(&req.Form, getDb())
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, req, documents, 200)
	}
	return nil
}

func documentHandler(rw http.ResponseWriter, req *http.Request) *appError {
	switch req.Method {
	case "GET":
		id, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Get document error", 500}
		}
		document, err := document.GetDocument(id, getDb())
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, req, document, 200)
	case "POST":
		target, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Add document error", 500}
		}
		item, err := queue.NewQueueItem(getDb(), "Add Document", nil, target, nil, nil, req.Body)
		if err != nil {
			return &appError{err, "Add document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
	case "DELETE":
		target, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		item, err := queue.NewQueueItem(getDb(), "Delete Document", nil, target, nil, nil, req.Body)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
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
	queue.Start(getDb())
	defer queue.Stop()

	r := mux.NewRouter().StrictSlash(true)
	r.Handle("/document/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	r.Handle("/document/test/", appHandler(testHandler)).Methods("POST")
	r.Handle("/document/{doctypes:(((\\d+-\\d+):?|(\\d+):?))+}/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	r.Handle("/document/{doctype:[0-9]+}/{docid:[0-9]+}/", appHandler(documentHandler)).Methods("GET", "POST", "DELETE")
	http.ListenAndServe(":8080", r)

}
