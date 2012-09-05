package api

import (
	"code.google.com/p/gorilla/mux"
	"document"
	"encoding/json"
	"fmt"
	"labix.org/v2/mgo"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"reflect"
)

type appError struct {
	Error   error
	Message string
	Code    int
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

var c *rpc.Client
var s *mgo.Session

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e := fn(w, r); e != nil {
		log.Printf("%v: %v: %v", e.Code, e.Error, e.Message)
		http.Error(w, fmt.Sprintf("Main Error: %v\nDetails: %v", e.Error, e.Message), e.Code)
	}
}

func doRPC(method string, args interface{}, reply interface{}, rw http.ResponseWriter) *appError {
	call := c.Go(method, args, reply, nil)
	replyCall := <-call.Done
	if replyCall.Error != nil {
		return &appError{replyCall.Error, "RPC Problem", 500}
	}
	return nil
}

func writeJson(rw http.ResponseWriter, object interface{}) *appError {
	value := reflect.Indirect(reflect.ValueOf(object))
	if value.Kind() == reflect.Slice {
		object = map[string]interface{}{"rows": object, "totalRows": value.Len()}
	}
	b, err := json.Marshal(object)
	if err != nil {
		return &appError{err, "Object not serializable to JSON", 500}
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.Write(b)
	return nil
}

func documentsHandler(rw http.ResponseWriter, req *http.Request) *appError {
	switch req.Method {
	case "GET":
		documents, err := document.GetDocuments(req, s.Clone())
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, documents)
	}
	return nil
}

func documentHandler(rw http.ResponseWriter, req *http.Request) *appError {
	switch req.Method {
	case "GET":
		document, err := document.GetDocument(req, s.Clone())
		if err != nil {
			return &appError{err, "Document not found", 404}
		}
		return writeJson(rw, document)
	case "POST":
		document, err := document.NewDocument(req)
		if err != nil {
			return &appError{err, "Badly formed Document", 500}
		}
		document.Save(s.Clone())
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
