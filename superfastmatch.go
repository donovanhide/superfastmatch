package main

import (
	"github.com/donovanhide/superfastmatch/api"
	"github.com/donovanhide/superfastmatch/posting"
	"github.com/donovanhide/superfastmatch/queue"
	"github.com/donovanhide/superfastmatch/registry"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	registry := registry.NewRegistry(os.Args)
	registry.Open()
	defer registry.Close()
	switch registry.Mode {
	case "client":
		api.Execute(registry, os.Args)
	default:
		log.Printf("Started in %v mode with Hash Width: %v and Window Size: %v", registry.Mode, registry.HashWidth, registry.WindowSize)
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
}
