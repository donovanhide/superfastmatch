package api

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type fileFn func(path string, fi os.FileInfo, r io.Reader) error

func walkTar(pathname string, fi os.FileInfo, r io.Reader, f fileFn) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return fmt.Errorf("Read tar file:%s/f", err)
		case hdr.FileInfo().IsDir():
			continue
		}
		fi := tarHeaderFileInfo{hdr}
		if err := processFile(pathname, fi, ioutil.NopCloser(tr), f); err != nil {
			return err
		}
	}
	return nil
}

func walkZip(pathname string, fi os.FileInfo, r io.Reader, f fileFn) error {
	zr, err := zip.OpenReader(pathname)
	if err != nil {
		return fmt.Errorf("Zip: %s", err)
	}
	defer zr.Close()
	for _, file := range zr.File {
		r2, err := file.Open()
		if err != nil {
			return fmt.Errorf("Read zip file:%s/f", err)
		}
		if err := processFile(pathname, file.FileInfo(), r2, f); err != nil {
			return err
		}
	}
	return nil
}

func getFile(path string) (io.ReadCloser, os.FileInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("Stat: %s", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("Open: %s", err)
	}
	return io.ReadCloser(f), fi, nil
}

func startWalk(path string, f fileFn) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	walk := func(path string, info os.FileInfo, err error) error {
		select {
		case <-sig:
			return fmt.Errorf("Ctrl-C pressed\n")
		default:
			break
		}
		if info.IsDir() {
			fmt.Fprintf(out, "Entering:\t%s\f", path)
			return nil
		}
		r, fi, err := getFile(path)
		if err != nil {
			fmt.Fprintln(out, err)
			return nil
		}
		if err := processFile(path, fi, r, f); err != nil {
			fmt.Fprintf(out, "Open:\t%s\f", err)
		}
		return nil
	}
	if err := filepath.Walk(path, walk); err != nil {
		fmt.Println(err)
	}
}

func processFile(pathname string, fi os.FileInfo, r io.ReadCloser, f fileFn) error {
	defer r.Close()
	var err error
	filename := fi.Name()
	if strings.HasPrefix(filename, ".") {
		return nil
	}
	r2 := io.ReadCloser(r)
	switch path.Ext(filename) {
	case ".gz":
		if r2, err = gzip.NewReader(r); err != nil {
			return fmt.Errorf("Gzip: %s", err)
		}
		defer r2.Close()
		filename = strings.TrimRight(filename, ".gz")
	}
	switch path.Ext(filename) {
	case ".tar":
		fmt.Fprintf(out, "Entering:\t%s\f", fi.Name())
		return walkTar(pathname, fi, r2, f)
	case ".zip":
		fmt.Fprintf(out, "Entering:\t%s\f", fi.Name())
		return walkZip(pathname, fi, r2, f)
	}
	return f(path.Dir(pathname), fi, r2)
}

type tarHeaderFileInfo struct {
	th *tar.Header
}

func (fi tarHeaderFileInfo) Name() string       { return path.Base(fi.th.Name) }
func (fi tarHeaderFileInfo) Size() int64        { return fi.th.Size }
func (fi tarHeaderFileInfo) IsDir() bool        { return fi.th.Mode == tar.TypeDir }
func (fi tarHeaderFileInfo) ModTime() time.Time { return fi.th.ModTime }
func (fi tarHeaderFileInfo) Mode() os.FileMode  { panic("not implemented"); return os.ModePerm }
func (fi tarHeaderFileInfo) Sys() interface{}   { return fi.th }
