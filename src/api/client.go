package api

import (
	"document"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"queue"
	"registry"
	"text/tabwriter"
	"time"
)

var addFlags = flag.NewFlagSet("Add document", flag.ExitOnError)
var addTitle = addFlags.String("title", "Untitled", "A title for the document to be added")
var addDoctype = addFlags.Uint64("doctype", 1, "A doctype for the document to be added")
var addDocid = addFlags.Uint64("docid", 1, "A docid for the document to be added")

var deleteFlags = flag.NewFlagSet("Delete document", flag.ExitOnError)
var deleteDoctype = deleteFlags.Uint64("doctype", 1, "A doctype for the document to be deleted")
var deleteDocid = deleteFlags.Uint64("docid", 1, "A docid for the document to be deleted")

var searchFlags = flag.NewFlagSet("Search", flag.ExitOnError)

var out = new(tabwriter.Writer)

func init() {
	out.Init(os.Stdout, 0, 4, 1, '\t', 0)
	addFlags.Usage = func() {
		fmt.Println("Add document")
		fmt.Println("superfastmatch add [-doctype uint32] [-docid uint32] [-title string] [- (stdin),file(s),directories,archives]")
	}
	deleteFlags.Usage = func() {
		fmt.Println("Delete document")
		fmt.Println("superfastmatch delete -doctype uint32 -docid uint32")
	}
	searchFlags.Usage = func() {
		fmt.Println("Search document")
		fmt.Println("superfastmatch search [quoted text,file(s),directories, archives]")
	}
}

func Execute(registry *registry.Registry, args []string) {
	r = registry
	c := make(chan *QueuedResponse, 100)
	done := PollQueue(c)
	switch args[1] {
	case "add":
		addCommand(args[2:], c)
	case "delete":
		deleteCommand(args[2:], c)
	case "search":
		searchCommand(args[2:], c)
	}
	close(c)
	<-done
}

func addCommand(args []string, c chan *QueuedResponse) {
	f := addFile(c)
	addFlags.Parse(args)
	for _, name := range addFlags.Args() {
		if name == "-" {
			if fi, err := os.Stdin.Stat(); err == nil {
				f("stdin", fi, os.Stdin)
				continue
			}
		}
		startWalk(name, f)
	}
}

func deleteCommand(args []string, c chan *QueuedResponse) {
	deleteFlags.Parse(args)
	apiUrl := fmt.Sprintf("http://%s/document/%d/%d/", r.ApiAddress, *deleteDoctype, *deleteDocid)
	var queued QueuedResponse
	code, err := doRequest("DELETE", apiUrl, &queued)
	if err != nil {
		fmt.Println("Delete Document:", err)
		return
	} else if code == http.StatusAccepted {
		fmt.Fprintf(out, "Deleted:\tDoctype: %3d\tDocid: %3d\tQueueId: %s\f", *deleteDoctype, *deleteDocid, queued.Id.Hex())
		c <- &queued
	}
}

func searchCommand(args []string, c chan *QueuedResponse) {
	searchFlags.Parse(args)
	f := searchFile(c)
	for _, name := range searchFlags.Args() {
		if name == "-" {
			if fi, err := os.Stdin.Stat(); err == nil {
				f("stdin", fi, os.Stdin)
				continue
			}
		}
		startWalk(name, f)
	}
}

func searchFile(c chan *QueuedResponse) fileFn {
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
	apiUrl := fmt.Sprintf("http://%s/search/", r.ApiAddress)
	var result document.SearchResult
	form := make(url.Values)
	form.Add("text", text)
	code, err := doPost(apiUrl, form, &result)
	switch {
	case err != nil:
		fmt.Println("Search:", err)
	case code == http.StatusOK:
		fmt.Fprintln(out, result.Associations.String(text))
		// fmt.Fprintf(out, "%v/f", result)
		// c <- &queued
	case code != http.StatusOK:
		fmt.Println(code)
	}
	out.Flush()
}

func addFile(c chan *QueuedResponse) fileFn {
	var lastPath string
	firstTitle := true
	return func(path string, fi os.FileInfo, r io.Reader) error {
		if path != lastPath && len(lastPath) > 0 {
			*addDoctype++
			*addDocid = 1
		}
		text, err := ioutil.ReadAll(r)
		chosenTitle := fi.Name()
		if firstTitle && *addTitle != "Untitled" {
			chosenTitle = *addTitle
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
		item, err := addDocument(*addDoctype, *addDocid, values)
		if err == nil {
			c <- item
			*addDocid++
		}
		return err
	}
}

func addDocument(doctype, docid uint64, values url.Values) (*QueuedResponse, error) {
	apiUrl := fmt.Sprintf("http://%s/document/%d/%d/", r.ApiAddress, doctype, docid)
	var queued QueuedResponse
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

func PollQueue(c chan *QueuedResponse) chan bool {
	done := make(chan bool)
	successes, failures := make([]queue.QueueItem, 0), make([]queue.QueueItem, 0)
	go func() {
		for item := range c {
			apiUrl := fmt.Sprintf("http://%s/queue/%s/", r.ApiAddress, item.Id.Hex())
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
		fmt.Printf("Successes: %d\t Failures:%d\n", len(successes), len(failures))
		if len(failures) > 0 {
			for _, fail := range failures {
				fmt.Printf("Failed: %s\n", fail.String())
			}
		}
		done <- true
	}()
	return done
}
