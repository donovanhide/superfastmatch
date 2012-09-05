package main

import (
	"api"
	"flag"
	"io"
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

func checkError(err error) {
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func launchPosting() *exec.Cmd {
	cmd := exec.Command(os.Args[0], "posting")
	stdout, err := cmd.StdoutPipe()
	checkError(err)
	stderr, err := cmd.StderrPipe()
	checkError(err)
	err = cmd.Start()
	checkError(err)
	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)
	time.Sleep(500 * time.Millisecond)
	return cmd
}

func main() {
	switch {
	case len(os.Args) > 1 && os.Args[1] == "api":
		api.Serve("localhost", "127.0.0.1:8090")
	case len(os.Args) > 1 && os.Args[1] == "posting":
		posting.Serve(1<<24, 48, 0)
	default:
		cmd := launchPosting()
		defer cmd.Process.Kill()
		api.Serve("localhost", "127.0.0.1:8090")
	}
}
