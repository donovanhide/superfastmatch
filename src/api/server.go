package api

import (
	"code.google.com/p/gorilla/mux"
	"document"
	"log"
	"net/http"
	"posting"
	"query"
	"queue"
	"registry"
)

var r *registry.Registry

var c *posting.Client

type QueuedResponse struct {
	Success bool   `json:"success"`
	QueueID string `json:"queueid"`
}

func testHandler(rw http.ResponseWriter, req *http.Request) *appError {
	item, err := queue.NewQueueItem(r, "Test Corpus", nil, nil, nil, nil, req.Body)
	if err != nil {
		return &appError{err, "Test Corpus Problem", 500}
	}
	return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
}

func documentsHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	switch req.Method {
	case "GET":
		documents, err := query.GetDocuments(&req.Form, r)
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
		document, err := document.GetDocument(id, r)
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, req, document, 200)
	case "POST":
		target, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Add document error", 500}
		}
		item, err := queue.NewQueueItem(r, "Add Document", nil, target, nil, nil, req.Body)
		if err != nil {
			return &appError{err, "Add document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
	case "DELETE":
		target, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		item, err := queue.NewQueueItem(r, "Delete Document", nil, target, nil, nil, req.Body)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueID: item.Id.Hex()}, 202)
	}
	return nil
}

func indexHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	rows, err := c.GetRows(&req.Form)
	if err != nil {
		return &appError{err, "Index problem", 500}
	}
	return writeJson(rw, req, rows, 200)
}

func searchHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	search := &posting.DocumentArg{Text: req.Form.Get("text")}
	rows, err := c.Search(search)
	if err != nil {
		return &appError{err, "Search problem", 500}
	}
	return writeJson(rw, req, rows, 200)
	return nil
}

func Serve(registry *registry.Registry) {
	r = registry
	var err error
	c, err = posting.NewClient(registry)
	if err != nil {
		panic(err)
	}
	defer c.Close()
	router := mux.NewRouter().StrictSlash(true)
	router.Handle("/document/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	router.Handle("/document/test/", appHandler(testHandler)).Methods("POST")
	router.Handle("/document/{doctypes:(((\\d+-\\d+):?|(\\d+):?))+}/", appHandler(documentsHandler)).Methods("GET", "DELETE")
	router.Handle("/document/{doctype:[0-9]+}/{docid:[0-9]+}/", appHandler(documentHandler)).Methods("GET", "POST", "DELETE")
	router.Handle("/index/", appHandler(indexHandler)).Methods("GET")
	router.Handle("/search/", appHandler(searchHandler)).Methods("POST")
	log.Println("Starting API server on:", registry.ApiListener.Addr().String())
	registry.Routines.Add(1)
	http.Serve(registry.ApiListener, router)
	log.Println("Stopping API server")
	registry.Routines.Done()
}
