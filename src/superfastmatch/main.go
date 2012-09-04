package main

import (
	"api"
	"flag"
	"log"
	"os"
	"os/exec"
	"posting"
	"time"
)

type Command struct {
	Run       func(cmd *Command, args []string)
	UsageLine string
	Short     string
	Long      string
	Flag      flag.FlagSet
}

func launchPosting() {
	cmd := exec.Command(os.Args[0], "posting")
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
}

func main() {
	switch {
	case len(os.Args) > 1 && os.Args[1] == "api":
		api.Serve()
	case len(os.Args) > 1 && os.Args[1] == "posting":
		posting.Serve(1<<24, 48, 0)
	default:
		launchPosting()
		api.Serve()
	}
}
