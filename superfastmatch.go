package main

import (
	"github.com/donovanhide/superfastmatch/api"
	"github.com/donovanhide/superfastmatch/client"
	"github.com/donovanhide/superfastmatch/posting"
	"github.com/donovanhide/superfastmatch/queue"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/golang/glog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "client" {
		client.Run(os.Args[2:])
		return
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	registry := registry.NewRegistry()
	registry.Open()
	defer registry.Close()
	glog.Infof("Started in %v mode with Hash Width: %v and Window Size: %v", registry.Mode, registry.HashWidth, registry.WindowSize)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go http.ListenAndServe("localhost:6060", nil)
	switch registry.Mode {
	case "posting":
		posting.Serve(registry)
	case "api":
		go queue.Start(registry)
		go api.MonitorFeeds(registry)
		go api.Serve(registry)
	case "standalone":
		posting.Serve(registry)
		go queue.Start(registry)
		go api.MonitorFeeds(registry)
		go api.Serve(registry)
	}
	<-sig
}
