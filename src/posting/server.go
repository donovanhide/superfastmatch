package posting

import (
	"log"
	"net"
	"net/rpc"
	"registry"
	// "strings"
)

func serve(registry *registry.Registry, l *net.Listener) {
	log.Println("Starting Posting Server on:", (*l).Addr().String())
	p := newPosting(registry, (*l).Addr().String())
	server := rpc.NewServer()
	server.Register(p)
	registry.Routines.Add(1)
	for {
		conn, err := (*l).Accept()
		if err != nil {
			break
		}
		go server.ServeConn(conn)
	}
	log.Println("Stopping Posting Server:", (*l).Addr().String())
	registry.Routines.Done()
}

func Serve(registry *registry.Registry) {
	for i, _ := range registry.PostingListeners {
		go serve(registry, &registry.PostingListeners[i])
	}
}
