package posting

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func Serve(width uint64, groupSize uint64, offset uint64) {
	p := Init(width, groupSize, offset)
	rpc.Register(p)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8090")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
