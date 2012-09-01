package main

import (
	"./sparsetable"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
)

var table *sparsetable.SparseTable

const width uint64 = 1 << 24
const groupSize uint64 = 48

func handler(w http.ResponseWriter, r *http.Request) {
	const length int32 = 255
	R := make([]byte, length)
	for i := 0; i < 1000; i++ {
		table.Set(uint64(rand.Int63n(int64(width))), R[:rand.Int31n(length)])
	}
	fmt.Fprintf(w, table.String())
}

func main() {
	table = sparsetable.Init(width, groupSize)
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8090", nil)
}
