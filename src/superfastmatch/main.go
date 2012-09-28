package main

import (
	"api"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"posting"
	"queue"
	"registry"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	registry := registry.NewRegistry(os.Args)
	registry.Open()
	log.Printf("Started in %v mode with Hash Width: %v and Window Size: %v", registry.Mode, registry.HashWidth, registry.WindowSize)
	defer registry.Close()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	if registry.Mode == "posting" || registry.Mode == "standalone" {
		posting.Serve(registry)
	}
	if registry.Mode == "api" || registry.Mode == "standalone" {
		go queue.Start(registry)
		go api.Serve(registry)
	}
	go http.ListenAndServe("localhost:6060", nil)
	<-sig
}
