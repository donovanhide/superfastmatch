package api

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os" // "path/filepath"
	"path/filepath"
	"registry"
	"strings"
)

var addFlags = flag.NewFlagSet("Add document", flag.ExitOnError)
var title = addFlags.String("title", "Untitled", "A title for the added document")
var doctype = addFlags.Uint64("doctype", 1, "A doctype for the added document")
var docid = addFlags.Uint64("docid", 1, "A docid for the added document")

func Execute(registry *registry.Registry, args []string) {
	r = registry
	switch args[1] {
	case "add":
		addCommand(args[2:])
	}
}

func addCommand(args []string) {
	addFlags.Parse(args)
	if addFlags.NArg() == 0 {
		if in, err := ioutil.ReadAll(os.Stdin); err != nil {
			addDocument(string(in))
		}
	} else {
		for _, name := range addFlags.Args() {
			path, err := filepath.Abs(name)
			if err != nil {
				fmt.Println(err)
			}
			if fi, err := os.Stat(path); err == nil && fi.IsDir() {
				filepath.Walk(path, walkDir)
			} else if err == nil {
				addFile(path, fi)
			}
		}
	}
}

func addDocument(text string) {
	form := make(url.Values)
	form.Add("text", text)
	form.Add("title", *title)
	url := fmt.Sprintf("http://%s/document/%d/%d/", r.ApiAddress, *doctype, *docid)
	resp, err := http.PostForm(url, form)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		fmt.Println(strings.TrimSpace(string(body)))
	} else {
		fmt.Println(err)
	}
}

func addFile(path string, info os.FileInfo) {
	fmt.Printf("Adding:\t\tDoctype:%d Docid:%d\t%s\n", *doctype, *docid, path)
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("Open:", err)
	}
	text, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("Read:", err)
	}
	addDocument(string(text))
}

func walkDir(path string, info os.FileInfo, err error) error {
	if info.IsDir() {
		fmt.Printf("Entering:\t\t\t\t%s\n", path)
		*doctype++
		*docid = 0
	} else {
		*docid++
		addFile(path, info)
	}
	return nil
}
