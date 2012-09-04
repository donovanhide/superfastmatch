package api

import (
	"code.google.com/p/gorilla/mux"
	"document"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
)

var c *rpc.Client

func doRPC(method string, args interface{}, reply interface{}, rw http.ResponseWriter) {
	call := c.Go(method, args, &reply, nil)
	replyCall := <-call.Done
	if replyCall.Error != nil {
		fmt.Fprintf(rw, "Fail")
	} else {
		fmt.Fprintf(rw, "OK")
	}
}

func documentHandler(rw http.ResponseWriter, req *http.Request) {
	success := false
	document, err := document.NewDocument(req)
	if err == nil {
		switch req.Method {
		case "GET":
			doRPC("Posting.Add", document, success, rw)
		case "POST":
			doRPC("Posting.Add", document, success, rw)
		case "DELETE":
			doRPC("Posting.Delete", document, success, rw)
		}
	}
}

func Serve() {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:8090")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	c = client
	r := mux.NewRouter().StrictSlash(true)
	r.HandleFunc("/document/{doctype}/{docid}/", documentHandler).Methods("GET", "POST", "DELETE")
	http.ListenAndServe(":8080", r)
}
