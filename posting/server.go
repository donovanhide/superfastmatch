package posting

import (
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/golang/glog"
	"net"
	"net/rpc"
)

func serve(registry *registry.Registry, l *net.Listener) {
	glog.Infoln("Starting Posting Server on:", (*l).Addr().String())
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
	glog.Infoln("Stopping Posting Server:", (*l).Addr().String())
	registry.Routines.Done()
}

func Serve(registry *registry.Registry) {
	for i, _ := range registry.PostingListeners {
		go serve(registry, &registry.PostingListeners[i])
	}
}
