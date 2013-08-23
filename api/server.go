package api

import (
	"fmt"
	"github.com/donovanhide/mux"
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/posting"
	"github.com/donovanhide/superfastmatch/query"
	"github.com/donovanhide/superfastmatch/queue"
	"github.com/donovanhide/superfastmatch/registry"
	"log"
	"net/http"
)

var r *registry.Registry

var c *posting.Client

const docRegex = `[0-9]+`
const queueRegex = `[0-9a-f]{24}`
const rangeRegex = `\d+(-\d+)?(:\d+(-\d+)?)*`

type is []interface{}
type ss []string

var routes = []struct {
	path    string
	regexes is
	fn      appHandler
	methods ss
}{
	{"/document/", nil, documentsHandler, ss{"GET", "DELETE"}},
	{"/document/test/", nil, testHandler, ss{"POST"}},
	{"/document/{doctypes:%s}/", is{rangeRegex}, documentsHandler, ss{"GET", "DELETE"}},
	{"/document/{doctype:%s}/{docid:%s}/", is{docRegex, docRegex}, documentHandler, ss{"GET", "POST", "DELETE"}},
	{"/association/", nil, associationHandler, ss{"GET", "POST", "DELETE"}},
	{"/association/{source:%s}/", is{rangeRegex}, associationHandler, ss{"GET", "POST", "DELETE"}},
	{"/association/{source:%s}/{target:%s}/", is{rangeRegex, rangeRegex}, associationHandler, ss{"GET", "POST", "DELETE"}},
	{"/queue/", nil, queueHandler, ss{"GET"}},
	{"/queue/{id:%s}/", is{queueRegex}, queueItemHandler, ss{"GET"}},
	{"/index/", nil, indexHandler, ss{"GET"}},
	{"/search/", nil, searchHandler, ss{"POST"}},
}

type QueuedResponse struct {
	*queue.QueueItem
	Success bool `json:"success"`
}

func testHandler(rw http.ResponseWriter, req *http.Request) *appError {
	item, err := queue.NewQueueItem(r, "Test Corpus", nil, nil, "", "", req.Body)
	if err != nil {
		return &appError{err, "Test Corpus Problem", 500}
	}
	return writeJson(rw, req, &QueuedResponse{Success: true, QueueItem: item}, 202)
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
		item, err := queue.NewQueueItem(r, "Add Document", nil, target, "", "", req.Body)
		if err != nil {
			return &appError{err, "Add document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueItem: item}, 202)
	case "DELETE":
		target, err := document.NewDocumentId(req)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		item, err := queue.NewQueueItem(r, "Delete Document", nil, target, "", "", req.Body)
		if err != nil {
			return &appError{err, "Delete document error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueItem: item}, 202)
	}
	return nil
}

func associationHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	switch req.Method {
	case "GET":
		fmt.Println(mux.Vars(req))
		association := &document.Association{}
		return writeJson(rw, req, association, 200)
	case "DELETE":
		return nil
	case "POST":
		source, target := mux.Vars(req)["source"], mux.Vars(req)["target"]
		item, err := queue.NewQueueItem(r, "Associate Document", nil, nil, source, target, req.Body)
		if err != nil {
			return &appError{err, "Association error", 500}
		}
		return writeJson(rw, req, &QueuedResponse{Success: true, QueueItem: item}, 202)
	}
	return nil
}

func queueItemHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	item, err := queue.GetQueueItem(req.Form, r)
	if err != nil {
		return &appError{err, "Queue problem", 500}
	}
	switch item.Status {
	case "Completed":
		if location := item.Location(r); location != "" {
			rw.Header().Set("Location", location)
		}
		return writeJson(rw, req, item, 201)
	case "Failed":
		return writeJson(rw, req, item, 400)
	}
	return writeJson(rw, req, item, 202)
}

func queueHandler(rw http.ResponseWriter, req *http.Request) *appError {
	fillValues(req)
	rows, err := queue.GetQueue(req.Form, r)
	if err != nil {
		return &appError{err, "Queue problem", 500}
	}
	return writeJson(rw, req, rows, 200)
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
	search := document.NewDocumentArg(req.Form)
	group, err := c.Search(search)
	if err != nil {
		return &appError{err, "Search Client", 500}
	}
	result, err := group.GetResult(r, search, false)
	if err != nil {
		return &appError{err, "Search Process results", 500}
	}
	return writeJson(rw, req, result, 200)
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
	for _, r := range routes {
		path := fmt.Sprintf(r.path, r.regexes...)
		router.Handle(path, appHandler(r.fn)).Methods(r.methods...)
	}
	log.Println("Starting API server on:", registry.ApiListener.Addr().String())
	registry.Routines.Add(1)
	http.Serve(registry.ApiListener, router)
	log.Println("Stopping API server")
	registry.Routines.Done()
}
