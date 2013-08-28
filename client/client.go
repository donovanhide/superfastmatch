package client

import (
	"flag"
	"fmt"
	"github.com/donovanhide/superfastmatch/api"
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/queue"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"text/tabwriter"
	"time"
)

var flags struct {
	doctype, docid    uint64
	title, apiAddress string
}

type command func(chan *api.QueuedResponse)

var set = flag.NewFlagSet("client", flag.ExitOnError)

var out = new(tabwriter.Writer)

func Run(args []string) {
	out.Init(os.Stdout, 0, 4, 1, '\t', 0)
	c := make(chan *api.QueuedResponse, 100)
	var cmd command
	done := PollQueue(c)
	usage := "Actions available: add delete search associate"
	set.Usage = func() {
		fmt.Println(usage)
		set.PrintDefaults()
	}
	if len(args) > 1 {
		set.StringVar(&flags.apiAddress, "api_address", "127.0.0.1:8080", "Address for API to listen on.")
		switch args[0] {
		case "add":
			cmd = addCommand
			usage = "Add document\nsuperfastmatch client add [options] file/dir/archive/pipe"
			set.Uint64Var(&flags.doctype, "doctype", 1, "A doctype for the document to be added")
			set.Uint64Var(&flags.docid, "docid", 1, "A docid for the document to be added")
			set.StringVar(&flags.title, "title", "", "A title for the document to be added")
		case "delete":
			cmd = deleteCommand
			usage = "Delete Document\nsuperfastmatch client delete [options]"
			set.Uint64Var(&flags.doctype, "doctype", 1, "A doctype for the document to be deleted")
			set.Uint64Var(&flags.docid, "docid", 1, "A docid for the document to be deleted")
		case "search":
			cmd = searchCommand
			usage = "Search for matching documents\nsuperfastmatch client search file/dir/archive/pipe"
		}
	}
	if len(args) <= 1 || set.Parse(args[1:]) == flag.ErrHelp {
		set.Usage()
		return
	}
	if cmd != nil {
		cmd(c)
		close(c)
		<-done
	}
}

func addCommand(c chan *api.QueuedResponse) {
	f := addFile(c)
	if name := set.Args()[set.NArg()-1]; name == "-" {
		if fi, err := os.Stdin.Stat(); err == nil {
			f("stdin", fi, os.Stdin)
		}
	} else {
		startWalk(name, f)
	}
}

func deleteCommand(c chan *api.QueuedResponse) {
	apiUrl := fmt.Sprintf("http://%s/document/%d/%d/", flags.apiAddress, flags.doctype, flags.docid)
	var queued api.QueuedResponse
	code, err := doRequest("DELETE", apiUrl, &queued)
	if err != nil {
		fmt.Println("Delete Document:", err)
		return
	} else if code == http.StatusAccepted {
		fmt.Fprintf(out, "Deleted:\tDoctype: %3d\tDocid: %3d\tQueueId: %s\f", flags.doctype, flags.docid, queued.Id.Hex())
		c <- &queued
	}
}

func searchCommand(c chan *api.QueuedResponse) {
	f := searchFile(c)
	if name := set.Args()[set.NArg()-1]; name == "-" {
		if fi, err := os.Stdin.Stat(); err == nil {
			f("stdin", fi, os.Stdin)
		}
	} else {
		startWalk(name, f)
	}
}

func searchFile(c chan *api.QueuedResponse) fileFn {
	return func(path string, fi os.FileInfo, r io.Reader) error {
		text, err := ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf("Search File:", err)
		}
		search(string(text))
		return nil
	}
}

func search(text string) {
	apiUrl := fmt.Sprintf("http://%s/search/", flags.apiAddress)
	var result document.SearchResult
	form := make(url.Values)
	form.Add("text", text)
	code, err := doPost(apiUrl, form, &result)
	switch {
	case err != nil:
		fmt.Println("Search:", err)
	case code == http.StatusOK:
		fmt.Fprintln(out, result.Associations.String(text))
	case code != http.StatusOK:
		fmt.Println(code)
	}
	out.Flush()
}

func addFile(c chan *api.QueuedResponse) fileFn {
	var lastPath string
	firstTitle := true
	return func(path string, fi os.FileInfo, r io.Reader) error {
		if path != lastPath && len(lastPath) > 0 {
			flags.doctype++
			flags.docid = 1
		}
		text, err := ioutil.ReadAll(r)
		chosenTitle := fi.Name()
		if firstTitle && flags.title != "Untitled" {
			chosenTitle = flags.title
			firstTitle = false
		}
		if err != nil {
			return fmt.Errorf("Fail: Read Input:\t%s\f", err)
		}
		values := url.Values{
			"title":    []string{chosenTitle},
			"text":     []string{string(text)},
			"path":     []string{path},
			"modified": []string{fi.ModTime().String()},
		}
		lastPath = path
		item, err := addDocument(flags.doctype, flags.docid, values)
		if err == nil {
			c <- item
			flags.docid++
		}
		return err
	}
}

func addDocument(doctype, docid uint64, values url.Values) (*api.QueuedResponse, error) {
	apiUrl := fmt.Sprintf("http://%s/document/%d/%d/", flags.apiAddress, doctype, docid)
	var queued api.QueuedResponse
	code, err := doPost(apiUrl, values, &queued)
	switch {
	case err != nil:
		return nil, fmt.Errorf("Fail: Read Response:\t%s", err)
	case code != http.StatusAccepted:
		return nil, fmt.Errorf("Fail: Did not add to queue:\t%s", err)
	}
	fmt.Println(queued)
	title := values.Get("title")
	fmt.Fprintf(out, "Added:\tDoctype: %3d\tDocid: %3d\tTitle: %s\tQueueId: %s\f", doctype, docid, title, queued.Id.Hex())
	return &queued, nil
}

func PollQueue(c chan *api.QueuedResponse) chan bool {
	done := make(chan bool)
	successes, failures := make([]queue.QueueItem, 0), make([]queue.QueueItem, 0)
	go func() {
		for item := range c {
			apiUrl := fmt.Sprintf("http://%s/queue/%s/", flags.apiAddress, item.Id.Hex())
			var queueItem queue.QueueItem
			code, err := doRequest("GET", apiUrl, &queueItem)
		poll:
			for {
				code, err = doRequest("GET", apiUrl, &queueItem)
				switch {
				case err != nil:
					fmt.Println("GET Queue Item:", err)
				case code == http.StatusCreated:
					successes = append(successes, queueItem)
				case code == http.StatusBadRequest:
					failures = append(failures, queueItem)
				default:
					time.Sleep(time.Second)
					continue poll
				}
				break
			}
		}
		if len(successes) > 0 || len(failures) > 0 {
			fmt.Printf("Successes: %d\t Failures:%d\n", len(successes), len(failures))
		}
		if len(failures) > 0 {
			for _, fail := range failures {
				fmt.Printf("Failed: %s\n", fail.String())
			}
		}
		done <- true
	}()
	return done
}
