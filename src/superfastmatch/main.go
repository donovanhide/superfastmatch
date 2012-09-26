package main

import (
	"api"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"posting"
	"queue"
	"registry"
)

func main() {
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
	<-sig
}
