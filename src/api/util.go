package api

import (
	"code.google.com/p/gorilla/mux"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type appError struct {
	Error   error
	Message string
	Code    int
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

func (fn appHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if e := fn(rw, r); e != nil {
		log.Printf("%v: %v: %v", e.Code, e.Error, e.Message)
		http.Error(rw, fmt.Sprintf("Main Error: %v\nDetails: %v", e.Error, e.Message), e.Code)
	}
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func doRPC(method string, args interface{}, reply interface{}, rw http.ResponseWriter) *appError {
	call := c.Go(method, args, reply, nil)
	replyCall := <-call.Done
	if replyCall.Error != nil {
		return &appError{replyCall.Error, "RPC Problem", 500}
	}
	return nil
}

func writeJson(rw http.ResponseWriter, req *http.Request, object interface{}) *appError {
	var enc *json.Encoder
	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		gz, err := gzip.NewWriterLevel(rw, gzip.BestSpeed)
		if err != nil {
			return &appError{err, "Gzip Error", 500}
		}
		rw.Header().Set("Content-Encoding", "gzip")
		gw := gzipResponseWriter{Writer: gz, ResponseWriter: rw}
		enc = json.NewEncoder(gw)
		defer gz.Close()
	} else {
		enc = json.NewEncoder(rw)
	}
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := enc.Encode(object); err != nil {
		return &appError{err, "Object not serializable to JSON", 500}
	}
	return nil
}

func fillValues(req *http.Request) {
	req.ParseForm()
	for k, v := range mux.Vars(req) {
		req.Form.Add(k, v)
	}
}
