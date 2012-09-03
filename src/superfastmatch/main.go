package main

import (
	// "fmt"
	// "math/rand"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"posting"
	"sparsetable"
)

var table *sparsetable.SparseTable

const width uint64 = 1 << 24
const groupSize uint64 = 48

// func handler(w http.ResponseWriter, r *http.Request) {
// 	const length int32 = 255
// 	R := make([]byte, length)
// 	for i := 0; i < 1000; i++ {
// 		table.Set(uint64(rand.Int63n(int64(width))), R[:rand.Int31n(length)])
// 	}
// 	fmt.Fprintf(w, table.String())
// }

func main() {
	table = sparsetable.Init(width, groupSize)
	posting := new(posting.Posting)
	rpc.Register(posting)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8090")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
	// http.HandleFunc("/", handler)
	// http.ListenAndServe(":8090", nil)
}
